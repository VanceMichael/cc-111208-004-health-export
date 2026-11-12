import json
import os
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from datetime import datetime, timezone


def utc_now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


class EpochChanged(Exception):
    """Raised when a download's term was superseded (cancel or newer run)."""


class Store:
    """Durable state for export jobs and immutable received batches.

    A new connection is opened for every operation so that status inspection
    and cancellation can run safely from another thread or process while a
    downloader is active.
    """

    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / 'export.sqlite3'
        self._initialize()

    def connect(self):
        database = sqlite3.connect(self.path, timeout=10)
        database.row_factory = sqlite3.Row
        database.execute('PRAGMA busy_timeout = 10000')
        database.execute('PRAGMA foreign_keys = ON')
        return database

    @contextmanager
    def transaction(self):
        database = self.connect()
        try:
            database.execute('BEGIN IMMEDIATE')
            yield database
            database.commit()
        except BaseException:
            database.rollback()
            raise
        finally:
            database.close()

    def _initialize(self):
        with self.connect() as database:
            database.execute('PRAGMA journal_mode = WAL')
            database.executescript('''
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                require_url TEXT NOT NULL,
                status_url TEXT UNIQUE,
                state TEXT NOT NULL,
                reason TEXT,
                transaction_time TEXT,
                requires_access_token INTEGER,
                download_epoch INTEGER NOT NULL DEFAULT 0,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                delete_sent INTEGER NOT NULL DEFAULT 0,
                created TEXT NOT NULL,
                updated TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS job_files (
                job_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                kind TEXT NOT NULL,
                type TEXT NOT NULL,
                url TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending',
                reason TEXT,
                bytes INTEGER NOT NULL DEFAULT 0,
                lines INTEGER NOT NULL DEFAULT 0,
                accepted INTEGER NOT NULL DEFAULT 0,
                duplicates INTEGER NOT NULL DEFAULT 0,
                conflicts INTEGER NOT NULL DEFAULT 0,
                diagnostics INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (job_id, ordinal)
            );
            CREATE TABLE IF NOT EXISTS staging_resources (
                job_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                resource_type TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                version_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (job_id, resource_type, resource_id, version_id)
            );
            CREATE TABLE IF NOT EXISTS staging_conflicts (
                job_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                resource_type TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                earlier_version TEXT NOT NULL,
                later_version TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS staging_references (
                job_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                from_type TEXT NOT NULL,
                from_id TEXT NOT NULL,
                from_version TEXT NOT NULL,
                target_type TEXT NOT NULL,
                target_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS staging_diagnostics (
                job_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS batches (
                batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL UNIQUE,
                transaction_time TEXT NOT NULL,
                requires_access_token INTEGER NOT NULL,
                output_files INTEGER NOT NULL,
                error_files INTEGER NOT NULL,
                resources INTEGER NOT NULL,
                duplicates INTEGER NOT NULL,
                conflicts INTEGER NOT NULL,
                diagnostics INTEGER NOT NULL,
                resolved_references INTEGER NOT NULL,
                dangling_references INTEGER NOT NULL,
                created TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS batch_resources (
                batch_id INTEGER NOT NULL,
                resource_type TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                version_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (batch_id, resource_type, resource_id, version_id)
            );
            CREATE TABLE IF NOT EXISTS batch_references (
                batch_id INTEGER NOT NULL,
                from_type TEXT NOT NULL,
                from_id TEXT NOT NULL,
                from_version TEXT NOT NULL,
                target_type TEXT NOT NULL,
                target_id TEXT NOT NULL,
                resolved INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS file_receipts (
                job_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                type TEXT NOT NULL,
                bytes INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                lines INTEGER NOT NULL,
                received TEXT NOT NULL,
                PRIMARY KEY (job_id, ordinal)
            );
            CREATE TABLE IF NOT EXISTS events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                at TEXT NOT NULL,
                event TEXT NOT NULL,
                detail TEXT NOT NULL
            );
            ''')

    # -- event audit -----------------------------------------------------

    def event(self, job_id, event, detail=None):
        with self.transaction() as database:
            database.execute('INSERT INTO events (job_id, at, event, detail) VALUES (?, ?, ?, ?)',
                             (job_id, utc_now(), event, json.dumps(detail or {}, sort_keys=True)))

    def events(self, job_id=None):
        with self.connect() as database:
            if job_id is None:
                rows = database.execute('SELECT * FROM events ORDER BY event_id').fetchall()
            else:
                rows = database.execute('SELECT * FROM events WHERE job_id = ? ORDER BY event_id',
                                        (job_id,)).fetchall()
            return [dict(row) | {'detail': json.loads(row['detail'])} for row in rows]

    # -- jobs ------------------------------------------------------------

    def create_job(self, job_id, require_url):
        with self.transaction() as database:
            database.execute('INSERT INTO jobs (id, require_url, state, created, updated) '
                             'VALUES (?, ?, ?, ?, ?)',
                             (job_id, require_url, 'starting', utc_now(), utc_now()))

    def job(self, job_id):
        with self.connect() as database:
            row = database.execute('SELECT * FROM jobs WHERE id = ?', (job_id,)).fetchone()
            return dict(row) if row else None

    def latest_job_for(self, require_url):
        with self.connect() as database:
            row = database.execute('SELECT * FROM jobs WHERE require_url = ? '
                                   'ORDER BY created DESC, rowid DESC LIMIT 1',
                                   (require_url,)).fetchone()
            return dict(row) if row else None

    def jobs(self):
        with self.connect() as database:
            return [dict(row) for row in database.execute(
                'SELECT * FROM jobs ORDER BY created, rowid').fetchall()]

    def set_job(self, job_id, **fields):
        if not fields:
            return
        allowed = {'state', 'reason', 'status_url', 'transaction_time',
                   'requires_access_token', 'download_epoch', 'cancel_requested',
                   'delete_sent'}
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError('Unknown job fields: ' + ', '.join(sorted(unknown)))
        assignments = ', '.join('%s = ?' % name for name in fields)
        values = list(fields.values()) + [utc_now(), job_id]
        with self.transaction() as database:
            database.execute('UPDATE jobs SET %s, updated = ? WHERE id = ?' % assignments,
                             values)

    def increment_epoch(self, job_id):
        with self.transaction() as database:
            database.execute('UPDATE jobs SET download_epoch = download_epoch + 1, updated = ? '
                             'WHERE id = ?', (utc_now(), job_id))
            return database.execute('SELECT download_epoch FROM jobs WHERE id = ?',
                                    (job_id,)).fetchone()[0]

    def mark_cancelled(self, job_id):
        """Requested cancellation: state changes immediately and the download
        term advances so in-flight files can never complete."""
        with self.transaction() as database:
            database.execute('UPDATE jobs SET cancel_requested = 1, state = ?, '
                             'download_epoch = download_epoch + 1, updated = ? '
                             'WHERE id = ? AND state != ?',
                             ('cancelled', utc_now(), job_id, 'ready'))
            database.execute('UPDATE job_files SET state = ?, reason = ? '
                             'WHERE job_id = ? AND state = ?',
                             ('cancelled', 'cancelled before completion', job_id, 'pending'))
            database.execute('UPDATE job_files SET state = ?, reason = ? '
                             'WHERE job_id = ? AND state = ?',
                             ('cancelled', 'cancelled while downloading', job_id, 'downloading'))

    # -- files -----------------------------------------------------------

    def add_file(self, job_id, ordinal, kind, file_type, url):
        with self.transaction() as database:
            database.execute('INSERT OR IGNORE INTO job_files '
                             '(job_id, ordinal, kind, type, url, state) VALUES (?, ?, ?, ?, ?, ?)',
                             (job_id, ordinal, kind, file_type, url, 'pending'))

    def file(self, job_id, ordinal):
        with self.connect() as database:
            row = database.execute('SELECT * FROM job_files WHERE job_id = ? AND ordinal = ?',
                                   (job_id, ordinal)).fetchone()
            return dict(row) if row else None

    def files(self, job_id):
        with self.connect() as database:
            return [dict(row) for row in database.execute(
                'SELECT * FROM job_files WHERE job_id = ? ORDER BY ordinal', (job_id,)).fetchall()]

    def set_file(self, job_id, ordinal, **fields):
        allowed = {'state', 'reason', 'bytes', 'lines', 'accepted', 'duplicates',
                   'conflicts', 'diagnostics'}
        receipt = fields.pop('receipt', None)
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError('Unknown file fields: ' + ', '.join(sorted(unknown)))
        assignments = ', '.join('%s = ?' % name for name in fields)
        with self.transaction() as database:
            database.execute('UPDATE job_files SET %s WHERE job_id = ? AND ordinal = ?'
                             % assignments, list(fields.values()) + [job_id, ordinal])
            if receipt is not None:
                database.execute(
                    'INSERT OR REPLACE INTO file_receipts (job_id, ordinal, type, bytes, '
                    'sha256, lines, received) VALUES (?, ?, ?, ?, ?, ?, ?)',
                    (job_id, ordinal, receipt['type'], receipt['bytes'], receipt['sha256'],
                     receipt['lines'], utc_now()))

    def receipt(self, job_id, ordinal):
        with self.connect() as database:
            row = database.execute('SELECT * FROM file_receipts WHERE job_id = ? AND ordinal = ?',
                                   (job_id, ordinal)).fetchone()
            return dict(row) if row else None

    def receipts(self, job_id):
        with self.connect() as database:
            return [dict(row) for row in database.execute(
                'SELECT * FROM file_receipts WHERE job_id = ? ORDER BY ordinal', (job_id,)).fetchall()]

    def clear_file_staging(self, job_id, ordinal):
        with self.transaction() as database:
            database.execute('DELETE FROM staging_resources WHERE job_id = ? AND ordinal = ?',
                             (job_id, ordinal))
            database.execute('DELETE FROM staging_conflicts WHERE job_id = ? AND ordinal = ?',
                             (job_id, ordinal))
            database.execute('DELETE FROM staging_references WHERE job_id = ? AND ordinal = ?',
                             (job_id, ordinal))
            database.execute('DELETE FROM staging_diagnostics WHERE job_id = ? AND ordinal = ?',
                             (job_id, ordinal))

    # -- staging ---------------------------------------------------------

    def insert_resource(self, job_id, ordinal, resource_type, resource_id,
                        version_id, payload, references):
        """Insert one streamed resource. Returns 'accepted' | 'duplicate' | 'conflict'.

        Identity is the exact string tuple (resourceType, id, meta.versionId).
        Versions are never compared for ordering. The file counters are
        updated in the same transaction, so a restart sees exactly the
        progress that was durably recorded.
        """
        counters = {'accepted': 'accepted', 'duplicate': 'duplicates', 'conflict': 'conflicts'}
        status = 'accepted'
        with self.transaction() as database:
            exact = database.execute(
                'SELECT 1 FROM staging_resources WHERE job_id = ? AND resource_type = ? '
                'AND resource_id = ? AND version_id = ?',
                (job_id, resource_type, resource_id, version_id)).fetchone()
            if exact:
                status = 'duplicate'
            else:
                earlier = database.execute(
                    'SELECT version_id FROM staging_resources WHERE job_id = ? '
                    'AND resource_type = ? AND resource_id = ? LIMIT 1',
                    (job_id, resource_type, resource_id)).fetchone()
                if earlier is not None:
                    status = 'conflict'
                    database.execute(
                        'INSERT INTO staging_conflicts (job_id, ordinal, resource_type, '
                        'resource_id, earlier_version, later_version) VALUES (?, ?, ?, ?, ?, ?)',
                        (job_id, ordinal, resource_type, resource_id,
                         earlier['version_id'], version_id))
                database.execute(
                    'INSERT INTO staging_resources (job_id, ordinal, resource_type, '
                    'resource_id, version_id, payload) VALUES (?, ?, ?, ?, ?, ?)',
                    (job_id, ordinal, resource_type, resource_id, version_id, payload))
                for target_type, target_id in references:
                    database.execute(
                        'INSERT INTO staging_references (job_id, ordinal, from_type, '
                        'from_id, from_version, target_type, target_id) '
                        'VALUES (?, ?, ?, ?, ?, ?, ?)',
                        (job_id, ordinal, resource_type, resource_id, version_id,
                         target_type, target_id))
            database.execute('UPDATE job_files SET %s = %s + 1 WHERE job_id = ? AND ordinal = ?'
                             % (counters[status], counters[status]), (job_id, ordinal))
        return status

    def insert_diagnostic(self, job_id, ordinal, payload):
        with self.transaction() as database:
            database.execute('INSERT INTO staging_diagnostics (job_id, ordinal, payload) '
                             'VALUES (?, ?, ?)', (job_id, ordinal, payload))
            database.execute('UPDATE job_files SET diagnostics = diagnostics + 1 '
                             'WHERE job_id = ? AND ordinal = ?', (job_id, ordinal))

    def staging_counts(self, job_id):
        with self.connect() as database:
            resources = database.execute(
                'SELECT COUNT(*) FROM staging_resources WHERE job_id = ?', (job_id,)).fetchone()[0]
            conflicts = database.execute(
                'SELECT COUNT(*) FROM staging_conflicts WHERE job_id = ?', (job_id,)).fetchone()[0]
            references = database.execute(
                'SELECT COUNT(*) FROM staging_references WHERE job_id = ?', (job_id,)).fetchone()[0]
            diagnostics = database.execute(
                'SELECT COUNT(*) FROM staging_diagnostics WHERE job_id = ?', (job_id,)).fetchone()[0]
        return {'resources': resources, 'conflicts': conflicts,
                'references': references, 'diagnostics': diagnostics}

    # -- batches ---------------------------------------------------------

    def promote(self, job_id, transaction_time, requires_access_token,
                output_files, error_files, duplicate_count, epoch=None):
        """Copy fully received staging data into an immutable visible batch.

        The whole copy happens in one transaction, so a partially received
        export is never visible downstream. If epoch is given it must still
        be current; a concurrent cancellation therefore cannot promote.
        """
        with self.transaction() as database:
            if epoch is not None:
                row = database.execute('SELECT download_epoch, cancel_requested, state '
                                       'FROM jobs WHERE id = ?', (job_id,)).fetchone()
                if (row is None or row['download_epoch'] != epoch
                        or row['cancel_requested'] or row['state'] == 'cancelled'):
                    raise EpochChanged(job_id)
            resources = database.execute(
                'SELECT resource_type, resource_id, version_id, payload FROM staging_resources '
                'WHERE job_id = ?', (job_id,)).fetchall()
            references = database.execute(
                'SELECT from_type, from_id, from_version, target_type, target_id '
                'FROM staging_references WHERE job_id = ?', (job_id,)).fetchall()
            conflicts = database.execute(
                'SELECT COUNT(*) FROM staging_conflicts WHERE job_id = ?', (job_id,)).fetchone()[0]
            diagnostics = database.execute(
                'SELECT COUNT(*) FROM staging_diagnostics WHERE job_id = ?', (job_id,)).fetchone()[0]
            present = {(row['resource_type'], row['resource_id']) for row in resources}
            resolved = sum(1 for row in references
                           if (row['target_type'], row['target_id']) in present)
            dangling = len(references) - resolved
            cursor = database.execute(
                'INSERT INTO batches (job_id, transaction_time, requires_access_token, '
                'output_files, error_files, resources, duplicates, conflicts, diagnostics, '
                'resolved_references, dangling_references, created) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (job_id, transaction_time, int(requires_access_token), output_files, error_files,
                 len(resources), duplicate_count, conflicts, diagnostics,
                 resolved, dangling, utc_now()))
            batch_id = cursor.lastrowid
            database.executemany(
                'INSERT INTO batch_resources (batch_id, resource_type, resource_id, '
                'version_id, payload) VALUES (?, ?, ?, ?, ?)',
                [(batch_id, row['resource_type'], row['resource_id'], row['version_id'],
                  row['payload']) for row in resources])
            database.executemany(
                'INSERT INTO batch_references (batch_id, from_type, from_id, from_version, '
                'target_type, target_id, resolved) VALUES (?, ?, ?, ?, ?, ?, ?)',
                [(batch_id, row['from_type'], row['from_id'], row['from_version'],
                  row['target_type'], row['target_id'],
                  int((row['target_type'], row['target_id']) in present))
                 for row in references])
            database.execute('UPDATE jobs SET state = ?, reason = NULL, cancel_requested = 0, '
                             'updated = ? WHERE id = ?', ('ready', utc_now(), job_id))
        return batch_id

    def batches(self):
        with self.connect() as database:
            return [dict(row) for row in database.execute(
                'SELECT * FROM batches ORDER BY batch_id').fetchall()]

    def latest_batch(self):
        with self.connect() as database:
            row = database.execute('SELECT * FROM batches ORDER BY batch_id DESC LIMIT 1').fetchone()
            return dict(row) if row else None

    def batch_resources(self, batch_id):
        with self.connect() as database:
            return [dict(row) for row in database.execute(
                'SELECT resource_type, resource_id, version_id FROM batch_resources '
                'WHERE batch_id = ? ORDER BY resource_type, resource_id, version_id',
                (batch_id,)).fetchall()]

    def batch_references(self, batch_id):
        with self.connect() as database:
            return [dict(row) for row in database.execute(
                'SELECT * FROM batch_references WHERE batch_id = ?', (batch_id,)).fetchall()]


def reset_job_staging(store, job_id, ordinal=None):
    clause, value = ('', (job_id,)) if ordinal is None else (' AND ordinal = ?', (job_id, ordinal))
    with store.transaction() as database:
        for table in ('staging_resources', 'staging_conflicts',
                      'staging_references', 'staging_diagnostics'):
            database.execute('DELETE FROM %s WHERE job_id = ?%s' % (table, clause), value)


def part_path(root, job_id, ordinal):
    directory = Path(root) / 'streams' / job_id
    directory.mkdir(parents=True, exist_ok=True)
    return directory / ('%03d.ndjson.part' % ordinal)


def final_path(root, job_id, ordinal):
    directory = Path(root) / 'streams' / job_id
    directory.mkdir(parents=True, exist_ok=True)
    return directory / ('%03d.ndjson' % ordinal)


def fsync_replace(source, destination):
    with open(source, 'rb') as stream:
        os.fsync(stream.fileno())
    os.replace(source, destination)
