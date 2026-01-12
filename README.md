# KVStore — Distributed Replicated Key-Value Store

A fault-tolerant, primary-backup replicated key-value store built in Python.
Designed to demonstrate distributed systems fundamentals: replication, durability,
observability, and graceful degradation under partial node failure.

> Inspired by [Vesper-raft](https://github.com/Oaklight/Vesper-raft).
> Simplified to use primary-backup replication instead of full Raft consensus,
> with added write-ahead logging, structured JSON telemetry, Prometheus metrics,
> and a Grafana dashboard for real-time cluster observability.

---

## Architecture

```
                        ┌─────────────────────────────────────────┐
                        │              CLIENT                     │
                        │   python client.py PUT/GET              │
                        └──────────────┬──────────────────────────┘
                                       │ HTTP PUT/GET /request
                                       ▼
                        ┌────────────────────────────────────────┐
                        │            PRIMARY NODE                │
                        │         http://127.0.0.1:5000          │
                        │                                        │
                        │  ┌─────────┐   ┌──────────────────┐    │
                        │  │ in-mem  │   │  WAL (wal_       │    │
                        │  │   DB    │◄──│  primary.log)    │    │
                        │  └─────────┘   └──────────────────┘    │
                        │                                        │
                        │  Two-phase commit:                     │
                        │    1. Stage + send LOG to replicas     │
                        │    2. Wait for majority ack            │
                        │    3. Write WAL → update DB            │
                        │    4. Send COMMIT to replicas          │
                        └──────────┬──────────────┬──────────────┘
                                   │  /replicate  │  /replicate
                          (POST log│& commit msgs)│
                   ┌───────────────▼──┐     ┌────▼──────────────┐
                   │   REPLICA 1      │     │   REPLICA 2       │
                   │ 127.0.0.1:5001   │     │ 127.0.0.1:5002    │
                   │                  │     │                   │
                   │ ┌────┐ ┌───────┐ │     │ ┌────┐ ┌───────┐  │
                   │ │ DB │ │  WAL  │ │     │ │ DB │ │  WAL  │  │
                   │ └────┘ └───────┘ │     │ └────┘ └───────┘  │
                   └──────────────────┘     └───────────────────┘
                          ▲                          ▲
                          │   POST /heartbeat        │
                          └─────── PRIMARY ──────────┘
                                  (every 50ms)
```

### Write Path (PUT)
1. Client sends `PUT /request` to primary
2. Primary stages the entry in memory
3. Primary sends `POST /replicate` with `action=log` to all replicas in parallel
4. Primary waits until at least 1 replica acks
5. Primary writes to WAL, then updates in-memory DB
6. Primary sends `POST /replicate` with `action=commit` to replicas
7. Primary returns `{"code": "success"}` to client

### Read Path (GET)
Reads are served from the primary's in-memory DB directly.
Replicas can serve reads too (stale reads are possible during replication lag).

### Heartbeat
Primary pings every replica every 50ms via `POST /heartbeat`.
If no response is received, the replica is marked unreachable in the metrics endpoint.
Writes continue as long as at least one replica responds.

---

## Setup

```bash
git clone <your-repo-url>
cd kvstore
pip install -r requirements.txt
```

For metrics visualization, you'll also need [Docker](https://docs.docker.com/get-docker/)
to run the Prometheus + Grafana stack (see [Observability with Prometheus + Grafana](#observability-with-prometheus--grafana) below).

---

## How to Run: 1 Primary + 2 Replicas

Open **3 terminal windows**.

**Terminal 1 — Primary**
```bash
python server.py primary http://127.0.0.1:5000 http://127.0.0.1:5001 http://127.0.0.1:5002
```

**Terminal 2 — Replica 1**
```bash
python server.py replica http://127.0.0.1:5001 http://127.0.0.1:5000
```

**Terminal 3 — Replica 2**
```bash
python server.py replica http://127.0.0.1:5002 http://127.0.0.1:5000
```

**Terminal 4 — Client**
```bash
# Write a value
python client.py http://127.0.0.1:5000 <key> <value>
# {'code': 'success'}

# Read it back
python client.py http://127.0.0.1:5000 name
# {'code': 'success', 'payload': {'key': 'name', 'value': 'tanmayee'}}

# Check node health
curl http://127.0.0.1:5000/metrics

# Check Prometheus-formatted metrics
curl http://127.0.0.1:5000/metrics-prom
```

---

## What Happens When You Kill a Node

### Kill Replica 1 (Ctrl+C in Terminal 2)

Primary log output within ~50ms:
```json
{"timestamp": 1719600123.4821, "node": "http://127.0.0.1:5000", "event": "replica_unreachable", "replica": "http://127.0.0.1:5001"}
```

Writes continue because Replica 2 is still up (majority = 2 out of 3 nodes, we need 1 replica ack):
```json
{"timestamp": 1719600125.1034, "node": "http://127.0.0.1:5000", "event": "put_committed", "key": "city", "commitIdx": 2, "replication_lag_ms": 3.12}
```

Metrics endpoint reflects the degraded state:
```json
{
  "node": "http://127.0.0.1:5000",
  "role": "primary",
  "commitIdx": 2,
  "db_keys": 2,
  "ops_get": 1,
  "ops_put": 2,
  "ops_failed": 0,
  "replicas": {
    "http://127.0.0.1:5001": {"alive": false, "last_seen_s": 4.82},
    "http://127.0.0.1:5002": {"alive": true,  "last_seen_s": 0.04}
  }
}
```

### Kill Both Replicas

Primary cannot reach majority (0 acks < 1 needed). Write times out after 50ms:
```json
{"timestamp": 1719600200.3310, "node": "http://127.0.0.1:5000", "event": "put_timeout", "key": "foo", "waited_ms": 50}
```

Client receives:
```json
{"code": "fail"}
```

Reads still work from primary's in-memory DB.

### Kill the Primary

Replicas remain alive but writes are refused (no failover in this implementation).
Clients attempting a PUT to a replica get:
```json
{"code": "redirect", "primary": "http://127.0.0.1:5000"}
```

### Restart a Node

On restart, the node replays its WAL to recover committed state before accepting traffic:
```json
{"timestamp": 1719600300.0012, "node": "http://127.0.0.1:5001", "event": "wal_replayed", "keys_recovered": ["name", "city"]}
{"timestamp": 1719600300.0031, "node": "http://127.0.0.1:5001", "event": "node_started", "role": "replica", "recovered_keys": ["name", "city"]}
```

---

## Structured Log Events Reference

| event                | emitted by  | meaning                                          |
|----------------------|-------------|--------------------------------------------------|
| `node_started`       | all nodes   | node came online, shows recovered keys from WAL  |
| `wal_init`           | all nodes   | WAL file path initialized                        |
| `wal_replayed`       | all nodes   | WAL replayed on startup, lists recovered keys    |
| `get_hit`            | all nodes   | key found in DB                                  |
| `get_miss`           | all nodes   | key not found                                    |
| `put_committed`      | primary     | write committed, includes replication_lag_ms     |
| `put_timeout`        | primary     | replicas didn't ack in time, write rejected      |
| `committed`          | all nodes   | local commit applied (WAL + DB update)           |
| `replica_staged`     | replica     | received log phase from primary                  |
| `replica_unreachable`| primary     | heartbeat to replica failed                      |
| `replica_recovered`  | primary     | replica came back online                         |
| `replica_behind`     | replica     | replica's commitIdx is behind primary's          |

---

## Observability with Prometheus + Grafana

In addition to the JSON `/metrics` endpoint, every node exposes Prometheus-formatted
metrics at `/metrics-prom`. A `docker-compose.yml` is included to run Prometheus and
Grafana locally — fully free, no signup, no cloud account required.

### Metrics Tracked

| Metric                          | Type      | What it shows                                  |
|----------------------------------|-----------|-------------------------------------------------|
| `kvstore_get_total`              | Counter   | GET ops by node and hit/miss status              |
| `kvstore_put_total`              | Counter   | PUT ops by node and success/timeout status       |
| `kvstore_commit_index`           | Gauge     | Current commit index per node                    |
| `kvstore_db_keys`                | Gauge     | Number of keys stored per node                   |
| `kvstore_replica_alive`          | Gauge     | Replica liveness (1 = up, 0 = down)              |
| `kvstore_replication_lag_ms`     | Histogram | Time between staging a write and majority ack    |
| `kvstore_request_latency_ms`     | Histogram | End-to-end GET/PUT latency                       |

### Running the Stack

1. Start your KV store cluster as described above (1 primary + 2 replicas).
2. From the project root, start Prometheus + Grafana:
   ```bash
   docker compose up -d
   ```
3. Open Grafana at [http://localhost:3000](http://localhost:3000) — login `admin` / `admin`.
   The **KV Store Cluster Dashboard** is auto-provisioned; no manual setup needed.
4. Generate some traffic so the graphs populate:
   ```bash
   python client.py http://127.0.0.1:5000 name tanmayee
   python client.py http://127.0.0.1:5000 name
   ```
5. Optional — query Prometheus directly at [http://localhost:9090](http://localhost:9090).

### Dashboard Panels

- **Replication Lag (ms)** — average lag per node over time
- **PUT / GET Throughput** — operation rate, broken out by status
- **Replica Health** — UP/DOWN timeline per replica
- **Commit Index per Node** — visualizes replication catching up after a failure
- **DB Key Count** — total keys stored per node
- **Failed PUTs (timeouts)** — spikes when majority can't be reached
- **Request Latency p95** — tail latency for GET and PUT

Kill a replica mid-run (`Ctrl+C` in its terminal) and watch the **Replica Health** panel
flip to DOWN within a few seconds, while **PUT Throughput** keeps flowing from the
remaining healthy replica — a live demo of fault tolerance.

### Stopping the Stack

```bash
docker compose down
```

Grafana dashboard state persists in a Docker volume between restarts;
use `docker compose down -v` to wipe it completely.

---

## Metrics Endpoint (JSON)

```
GET /metrics
```

Returns a JSON snapshot of node health:

```json
{
  "node":       "http://127.0.0.1:5000",
  "role":       "primary",
  "commitIdx":  5,
  "db_keys":    3,
  "ops_get":    10,
  "ops_put":    5,
  "ops_failed": 0,
  "replicas": {
    "http://127.0.0.1:5001": {"alive": true,  "last_seen_s": 0.03},
    "http://127.0.0.1:5002": {"alive": false, "last_seen_s": 12.4}
  }
}
```

---

## File Structure

```
kvstore/
├── server.py                          # Flask HTTP layer — client, replication, metrics routes
├── node.py                            # Core logic — replication, commit, heartbeat, metrics
├── wal.py                             # Write-ahead log — append before commit, replay on startup
├── logger.py                          # Structured JSON logger (replaces print statements)
├── metrics_registry.py                # Prometheus metric definitions (counters, gauges, histograms)
├── utils.py                           # HTTP helper for inter-node communication
├── config.py                          # Timing constants
├── client.py                          # CLI client for GET/PUT
├── requirements.txt
├── docker-compose.yml                 # Prometheus + Grafana stack
├── prometheus.yml                     # Prometheus scrape config
└── grafana-provisioning/
    ├── datasources/prometheus.yml     # Auto-registers Prometheus as Grafana's datasource
    └── dashboards/
        ├── dashboards.yml             # Tells Grafana where to find dashboard JSON
        └── kvstore-dashboard.json     # Pre-built cluster dashboard
```

---

## Known Limitations

- **No leader election** — the primary is statically configured. If the primary crashes, writes stop. There is no automatic failover.
- **WAL is append-only** — the log is never compacted. Over time it grows unboundedly. A snapshot + truncation mechanism would be needed for production.
- **Stale reads from replicas** — replicas may serve slightly out-of-date values during the window between a primary commit and the replica receiving the commit message.
- **No authentication or TLS** — all inter-node and client traffic is plaintext HTTP.
- **Single-region only** — no support for geo-distributed nodes or cross-datacenter replication.

---

## Future Improvements

- **Raft consensus** — replace static primary-backup with Raft for automatic leader election and split-brain prevention
- **WAL snapshotting** — periodically checkpoint the DB state and truncate old WAL entries to bound disk usage
- **TLS + mTLS** — encrypt all traffic and enforce mutual authentication between nodes
- **Read-your-writes consistency** — route reads through the primary or add version-based consistency checks for replicas
- **Containerize the KV store nodes themselves** — currently only Prometheus/Grafana run in Docker; the nodes run as bare Python processes
- **Grafana alerting** — trigger alerts when replication lag exceeds a threshold or a replica stays down longer than N seconds