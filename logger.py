import json
import time
import sys

_node_addr = "unknown"

def init(addr):
    global _node_addr
    _node_addr = addr

def log(event, **kwargs):
    """Emit a structured JSON log line to stdout."""
    entry = {
        "timestamp": round(time.time(), 4),
        "node": _node_addr,
        "event": event,
    }
    entry.update(kwargs)
    print(json.dumps(entry), flush=True)