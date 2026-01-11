import threading
import time
import utils
import logger
from config import cfg
from wal import WAL

PRIMARY = "primary"
REPLICA = "replica"


class Node():
    def __init__(self, role, my_ip, replicas, wal_filepath):
        """
        role        : "primary" or "replica"
        my_ip       : this node's own address e.g. http://127.0.0.1:5000
        replicas    : list of replica addresses (only used when role == PRIMARY)
        wal_filepath: path to write-ahead log file for this node
        """
        self.addr   = my_ip
        self.role   = role
        self.replicas = replicas  # empty list for replicas

        # Replication state
        self.lock       = threading.Lock()
        self.commitIdx  = 0
        self.staged     = None

        # Metrics counters
        self.ops_get    = 0
        self.ops_put    = 0
        self.ops_failed = 0

        # WAL + in-memory store
        self.wal = WAL(wal_filepath)
        self.DB  = self.wal.replay()   # recover state from disk on startup

        # Replica health: {addr -> {"alive": bool, "last_seen": float}}
        self.replica_health = {r: {"alive": True, "last_seen": time.time()}
                               for r in replicas}

        logger.log("node_started", role=self.role, recovered_keys=list(self.DB.keys()))

        # Primary starts heartbeat loop to all replicas
        if self.role == PRIMARY:
            for replica in self.replicas:
                t = threading.Thread(target=self._heartbeat_loop, args=(replica,), daemon=True)
                t.start()

    # ------------------------------------------------------------------
    # CLIENT-FACING OPERATIONS
    # ------------------------------------------------------------------

    def handle_get(self, payload):
        key = payload.get("key")
        self.ops_get += 1
        if key in self.DB:
            logger.log("get_hit", key=key)
            return {"key": key, "value": self.DB[key]}
        logger.log("get_miss", key=key)
        return None

    def handle_put(self, payload):
        """
        Two-phase commit (simplified):
          1. Stage the write and send LOG message to all replicas.
          2. Wait until at least one replica acks (majority of 3 = 2 nodes).
          3. Commit locally, write WAL, send COMMIT to replicas.
        """
        key   = payload.get("key")
        value = payload.get("value")

        self.lock.acquire()
        self.staged = payload
        self.ops_put += 1

        log_message = {
            "action":    "log",
            "payload":   payload,
            "commitIdx": self.commitIdx,
        }

        # Track which replicas acked the log phase
        log_acks = [False] * len(self.replicas)
        start    = time.time()

        threading.Thread(
            target=self._spread_update,
            args=(log_message, log_acks),
            daemon=True
        ).start()

        # Wait for majority (at least 1 replica in a 3-node cluster)
        needed  = max(1, len(self.replicas) // 2)
        waited  = 0
        while sum(log_acks) < needed:
            time.sleep(0.0005)
            waited += 0.0005
            if waited > cfg.MAX_LOG_WAIT / 1000:
                logger.log("put_timeout", key=key, waited_ms=round(waited * 1000))
                self.ops_failed += 1
                self.lock.release()
                return False

        # Commit locally
        replication_lag_ms = round((time.time() - start) * 1000, 2)
        self._commit(key, value)

        commit_message = {
            "action":    "commit",
            "payload":   payload,
            "commitIdx": self.commitIdx,
        }
        threading.Thread(
            target=self._spread_update,
            args=(commit_message, None, self.lock),
            daemon=True
        ).start()

        logger.log("put_committed", key=key, commitIdx=self.commitIdx,
                   replication_lag_ms=replication_lag_ms)
        return True

    # ------------------------------------------------------------------
    # REPLICATION HELPERS (PRIMARY SIDE)
    # ------------------------------------------------------------------

    def _spread_update(self, message, acks=None, lock=None):
        """Send a replication message to every replica in parallel."""
        for i, replica in enumerate(self.replicas):
            r = utils.send(replica, "replicate", message)
            if r and acks is not None:
                acks[i] = True
            if not r:
                self._mark_replica_down(replica)
        if lock:
            lock.release()

    def _heartbeat_loop(self, replica):
        """Primary pings each replica every HB_TIME ms to track liveness."""
        while True:
            start = time.time()
            r = utils.send(replica, "heartbeat", {"addr": self.addr, "commitIdx": self.commitIdx})
            if r:
                self._mark_replica_up(replica)
            else:
                self._mark_replica_down(replica)
            elapsed = time.time() - start
            time.sleep(max(0, (cfg.HB_TIME - elapsed * 1000) / 1000))

    def _mark_replica_down(self, replica):
        if self.replica_health[replica]["alive"]:
            logger.log("replica_unreachable", replica=replica)
        self.replica_health[replica]["alive"] = False

    def _mark_replica_up(self, replica):
        if not self.replica_health[replica]["alive"]:
            logger.log("replica_recovered", replica=replica)
        self.replica_health[replica]["alive"]     = True
        self.replica_health[replica]["last_seen"] = time.time()

    # ------------------------------------------------------------------
    # REPLICA-SIDE: RECEIVE REPLICATION MESSAGES FROM PRIMARY
    # ------------------------------------------------------------------

    def receive_replicate(self, msg):
        """Called on a replica when the primary sends log or commit messages."""
        action  = msg.get("action")
        payload = msg.get("payload", {})

        if action == "log":
            self.staged = payload
            logger.log("replica_staged", key=payload.get("key"))

        elif action == "commit":
            if not self.staged:
                self.staged = payload
            key   = self.staged.get("key")
            value = self.staged.get("value")
            self._commit(key, value)

        return self.commitIdx

    def receive_heartbeat(self, msg):
        """Replica receives a heartbeat ping from primary."""
        primary_commit = msg.get("commitIdx", 0)
        if primary_commit > self.commitIdx:
            logger.log("replica_behind", primary_commitIdx=primary_commit,
                       my_commitIdx=self.commitIdx)
        return self.commitIdx

    # ------------------------------------------------------------------
    # COMMIT: write WAL then update in-memory DB
    # ------------------------------------------------------------------

    def _commit(self, key, value):
        self.commitIdx += 1
        # WAL write BEFORE updating memory — durability guarantee
        self.wal.append(self.commitIdx, key, value)
        self.DB[key] = value
        self.staged  = None
        logger.log("committed", key=key, commitIdx=self.commitIdx)

    # ------------------------------------------------------------------
    # METRICS
    # ------------------------------------------------------------------

    def metrics(self):
        replica_status = {}
        for addr, health in self.replica_health.items():
            last = health["last_seen"]
            replica_status[addr] = {
                "alive":        health["alive"],
                "last_seen_s":  round(time.time() - last, 2),
            }

        return {
            "node":         self.addr,
            "role":         self.role,
            "commitIdx":    self.commitIdx,
            "db_keys":      len(self.DB),
            "ops_get":      self.ops_get,
            "ops_put":      self.ops_put,
            "ops_failed":   self.ops_failed,
            "replicas":     replica_status,
        }