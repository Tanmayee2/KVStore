from prometheus_client import Counter, Gauge, Histogram

# Counters — only go up
OPS_GET     = Counter('kvstore_get_total',    'Total GET operations',    ['node', 'status'])
OPS_PUT     = Counter('kvstore_put_total',    'Total PUT operations',    ['node', 'status'])

# Gauges — can go up and down
COMMIT_IDX  = Gauge('kvstore_commit_index',   'Current commit index',    ['node'])
DB_KEYS     = Gauge('kvstore_db_keys',        'Number of keys in DB',    ['node'])
REPLICA_UP  = Gauge('kvstore_replica_alive',  'Replica liveness (1/0)',  ['node', 'replica'])

# Histogram — tracks distribution of values (latency)
REPL_LAG    = Histogram(
    'kvstore_replication_lag_ms',
    'Replication lag in milliseconds',
    ['node'],
    buckets=[1, 2, 5, 10, 20, 50, 100, 250]
)

REQUEST_LAT = Histogram(
    'kvstore_request_latency_ms',
    'End-to-end request latency in milliseconds',
    ['node', 'operation'],
    buckets=[1, 2, 5, 10, 25, 50, 100]
)