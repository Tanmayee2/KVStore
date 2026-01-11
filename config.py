# timing variables
class cfg():
    # in ms
    REQUESTS_TIMEOUT = 50    # timeout for inter-node HTTP calls
    HB_TIME = 50             # heartbeat interval (primary -> replica)
    MAX_LOG_WAIT = 50        # how long primary waits for replica ack before giving up
    WAL_FILE = "wal.log"     # write-ahead log filename (per node, set in server.py)