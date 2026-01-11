import json
import os
import logger

class WAL:
    """
    Write-Ahead Log: every committed key/value is appended to a file
    before being applied to the in-memory store.

    On restart, call replay() to rebuild the in-memory DB from disk.
    """

    def __init__(self, filepath):
        self.filepath = filepath
        logger.log("wal_init", filepath=filepath)

    def append(self, commit_idx, key, value):
        """Append one committed entry to the WAL. Called before updating DB."""
        entry = {"commit_idx": commit_idx, "key": key, "value": value}
        with open(self.filepath, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def replay(self):
        """
        Read the WAL on startup and return a dict representing
        the last known committed state.
        """
        db = {}
        if not os.path.exists(self.filepath):
            return db
        with open(self.filepath, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    db[entry["key"]] = entry["value"]
                except json.JSONDecodeError:
                    logger.log("wal_corrupt_line", line=line)
        logger.log("wal_replayed", keys_recovered=list(db.keys()))
        return db