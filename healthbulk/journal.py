import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile


class Journal:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.blobs = self.root / 'blobs'
        self.blobs.mkdir(exist_ok=True)
        self.database = sqlite3.connect(self.root / 'captures.sqlite3')
        self.database.execute('CREATE TABLE IF NOT EXISTS captures (digest TEXT PRIMARY KEY, source TEXT NOT NULL, size INTEGER NOT NULL)')

    def save(self, payload, source):
        digest = hashlib.sha256(payload).hexdigest()
        destination = self.blobs / digest
        if not destination.exists():
            descriptor, name = tempfile.mkstemp(dir=self.blobs)
            try:
                with os.fdopen(descriptor, 'wb') as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(name, destination)
            finally:
                if os.path.exists(name):
                    os.unlink(name)
        with self.database:
            self.database.execute('INSERT OR IGNORE INTO captures VALUES (?, ?, ?)', (digest, source, len(payload)))
        return {'digest': digest, 'size': len(payload), 'path': str(destination)}

    def close(self):
        self.database.close()
