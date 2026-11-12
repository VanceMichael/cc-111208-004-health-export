import hashlib
import json
import time
import uuid
from pathlib import Path

from .manifest import ManifestError, parse_manifest
from .records import BadLine, read_ndjson, references_of
from .store import (Store, final_path, fsync_replace, part_path, reset_job_staging,
                    utc_now, EpochChanged)
from .transport import fetch, open_stream, require_trusted, retry_after_seconds, normalize_trusted


class ExportError(Exception):
    pass


class UncertainJob(ExportError):
    pass


class Cancelled(Exception):
    pass


class StateError(ExportError):
    pass


TERMINAL = frozenset(('ready', 'diagnostic', 'uncertain', 'cancelled'))
RESUMABLE = frozenset(('starting', 'waiting', 'downloading', 'blocked', 'cancelled'))


def describe_job(job, files=None, batch=None):
    """Translate internal rows into the externally visible state taxonomy.

    waiting  : job address is known; the source still reports in-progress
    downloading : manifest accepted; files are being received/verified
    diagnostic : an error file (OperationOutcome) blocks the whole switch
    ready      : this export is the visible batch
    blocked    : bad line / failed download; previous visible batch is kept
    cancelled  : cancellation took effect; no further downloads are scheduled
    uncertain  : the create response was lost; manual reconciliation required
    """
    if job is None:
        raise StateError('Unknown job')
    if job['state'] == 'blocked':
        visible = 'blocked'
    else:
        visible = job['state']
    detail = {'job': job['id'], 'state': visible, 'reason': job.get('reason'),
              'status_url': job.get('status_url'),
              'transactionTime': job.get('transaction_time'),
              'requiresAccessToken': (None if job.get('requires_access_token') is None
                                      else bool(job['requires_access_token'])),
              'visible_batch': batch['batch_id'] if batch else None,
              'files': []}
    for item in files or []:
        detail['files'].append({
            'kind': item['kind'], 'type': item['type'], 'ordinal': item['ordinal'],
            'state': item['state'], 'reason': item.get('reason'),
            'bytes': item['bytes'], 'lines': item['lines'], 'accepted': item['accepted'],
            'duplicates': item['duplicates'], 'conflicts': item['conflicts'],
            'diagnostics': item['diagnostics'],
        })
    return detail


def _digest(path):
    hasher = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(65536), b''):
            hasher.update(block)
    return hasher.hexdigest()


def _length(headers):
    """Declared Content-Length when the stream is a complete body; None when
    the source used chunked transfer coding."""
    try:
        value = int(headers.get('Content-Length', ''))
    except (TypeError, ValueError):
        return None
    return value if value >= 0 else None


class Exporter:
    def __init__(self, root, trusted=None, token=None, clock=None, timeout=5,
                 poll_cap=30.0, observer=None):
        self.store = Store(root)
        self.root = Path(root)
        self.trusted = normalize_trusted(trusted) if trusted else None
        self.token = token
        self.clock = clock or time.sleep
        self.timeout = timeout
        self.poll_cap = poll_cap
        self.observer = observer
        self.last_retry_after = None

    # -- headers ---------------------------------------------------------

    def _auth(self, job=None):
        """Credentials travel only when this export declared requiresAccessToken.

        A job that has not yet received a manifest sends the token only to its
        own export/status endpoints, which are not manifest outputs.
        """
        if job is not None and job.get('requires_access_token') == 0:
            return {}
        return {'Authorization': 'Bearer ' + self.token} if self.token else {}

    # -- job creation ----------------------------------------------------

    def create(self, require_url, force_new=False):
        """Persist a job identity, then request the export exactly once.

        A local job row is written before any network I/O. The status address
        is persisted before the first poll. If the start response is lost the
        job becomes 'uncertain' and is never silently re-created.
        """
        require_trusted(require_url, self.trusted)
        previous = self.store.latest_job_for(require_url)
        if previous and not force_new and previous['state'] not in ('cancelled', 'ready'):
            self.store.event(previous['id'], 'create_skipped_existing',
                             {'state': previous['state']})
            return previous['id'], 'existing'
        job_id = uuid.uuid4().hex
        self.store.create_job(job_id, require_url)
        self.store.event(job_id, 'job_created', {'require_url': require_url})
        try:
            result = fetch(require_url, 'GET', headers={'Prefer': 'respond-async', **self._auth()},
                           trusted=self.trusted, timeout=self.timeout, observer=self.observer)
        except (OSError, ValueError) as error:
            self.store.set_job(job_id, state='uncertain',
                               reason='create response unavailable; manual reconciliation required')
            self.store.event(job_id, 'create_uncertain', {'error': type(error).__name__})
            raise UncertainJob(job_id) from error
        self.store.event(job_id, 'create_response', {'status': result.status})
        if result.status == 202:
            location = result.headers.get('Content-Location')
            if not location:
                self.store.set_job(job_id, state='uncertain',
                                   reason='202 without Content-Location; manual reconciliation required')
                self.store.event(job_id, 'create_uncertain', {'reason': 'missing Content-Location'})
                raise UncertainJob(job_id)
            try:
                require_trusted(location, self.trusted)
            except ValueError as error:
                self.store.set_job(job_id, state='uncertain', reason=str(error))
                raise UncertainJob(job_id) from error
            self.last_retry_after = retry_after_seconds(result.headers.get('Retry-After'))
            self.store.set_job(job_id, status_url=location, state='waiting',
                               reason='source reports export in progress')
            self.store.event(job_id, 'job_address_persisted', {'status_url': location})
            return job_id, 'created'
        if result.status == 200:
            self._accept_manifest(job_id, result.body)
            return job_id, 'created'
        self.store.set_job(job_id, state='uncertain',
                           reason='create returned HTTP %d; manual reconciliation required'
                                  % result.status)
        self.store.event(job_id, 'create_uncertain', {'status': result.status})
        raise UncertainJob(job_id)

    # -- polling ---------------------------------------------------------

    def poll(self, job_id):
        job = self.store.job(job_id)
        if job is None:
            raise StateError('Unknown job')
        if self._cancelled(job):
            raise Cancelled(job_id)
        if job['state'] in ('ready', 'diagnostic', 'cancelled'):
            return job['state']
        if not job['status_url']:
            self.store.set_job(job_id, state='uncertain',
                               reason='job address was never persisted; manual reconciliation required')
            return 'uncertain'
        try:
            result = fetch(job['status_url'], 'GET', headers=self._auth(job),
                           trusted=self.trusted, timeout=self.timeout, observer=self.observer)
        except (OSError, ValueError) as error:
            self.store.set_job(job_id, state='waiting', reason='poll connection failed: ' + type(error).__name__)
            self.store.event(job_id, 'poll_failed', {'error': type(error).__name__})
            return 'waiting'
        self.store.event(job_id, 'poll_response', {'status': result.status})
        if result.status == 202:
            self.last_retry_after = retry_after_seconds(result.headers.get('Retry-After'))
            self.store.set_job(job_id, state='waiting', reason='source reports export in progress')
            return 'waiting'
        if result.status == 200:
            self._accept_manifest(job_id, result.body)
            return self.store.job(job_id)['state']
        if result.status == 404:
            self.store.set_job(job_id, state='blocked',
                               reason='job address no longer exists; not re-creating')
        else:
            self.store.set_job(job_id, state='waiting',
                               reason='poll returned HTTP %d' % result.status)
        self.store.event(job_id, 'poll_failed', {'status': result.status})
        return self.store.job(job_id)['state']

    def _accept_manifest(self, job_id, payload):
        try:
            manifest = parse_manifest(payload)
        except ManifestError as error:
            self.store.set_job(job_id, state='blocked', reason='invalid manifest: ' + str(error))
            self.store.event(job_id, 'manifest_rejected', {'error': str(error)})
            return
        requires = manifest['requiresAccessToken']
        self.store.set_job(job_id, state='downloading', transaction_time=manifest['transactionTime'],
                           requires_access_token=int(requires),
                           reason='manifest received')
        self.store.event(job_id, 'manifest_received', {
            'transactionTime': manifest['transactionTime'],
            'requiresAccessToken': requires,
            'output': len(manifest['output']), 'error': len(manifest['error'])})
        ordinal = 0
        for item in manifest['output']:
            self.store.add_file(job_id, ordinal, 'output', item['type'], item['url'])
            ordinal += 1
        diagnostic_files = []
        for item in manifest['error']:
            self.store.add_file(job_id, ordinal, 'error', item['type'], item['url'])
            diagnostic_files.append(ordinal)
            ordinal += 1
        if manifest['error']:
            self.store.set_job(job_id, state='diagnostic',
                               reason='manifest contains %d error file(s); batch not switched'
                                      % len(manifest['error']))
            self.store.event(job_id, 'diagnostic_block', {'error_files': len(manifest['error'])})
            for file_ordinal in diagnostic_files:
                self._download_diagnostic(job_id, file_ordinal)
            return manifest
        return manifest

    # -- download --------------------------------------------------------

    def _cancelled(self, job):
        fresh = self.store.job(job['id'])
        return fresh is not None and (fresh['cancel_requested'] or fresh['state'] == 'cancelled')

    def _checkpoint(self, job_id, epoch):
        job = self.store.job(job_id)
        if job is None:
            raise Cancelled(job_id)
        if job['cancel_requested'] or job['state'] == 'cancelled' or job['download_epoch'] != epoch:
            raise Cancelled(job_id)

    def progress_path(self, job_id, ordinal):
        directory = self.root / 'streams' / job_id
        directory.mkdir(parents=True, exist_ok=True)
        return directory / ('%03d.progress' % ordinal)

    def _download_diagnostic(self, job_id, ordinal):
        job = self.store.job(job_id)
        item = self.store.file(job_id, ordinal)
        headers = self._auth(job)
        epoch = job['download_epoch']
        try:
            with open_stream(item['url'], headers=headers, trusted=self.trusted,
                             timeout=self.timeout, observer=self.observer) as (result, response):
                if result.status != 200:
                    self.store.set_file(job_id, ordinal, state='failed',
                                        reason='HTTP %d' % result.status)
                    return
                counts = read_ndjson(response, part_path(self.root, job_id, ordinal),
                                     lambda document, line: self._diagnostic_line(job_id, ordinal, document, line),
                                     checkpoint=lambda: self._checkpoint(job_id, epoch),
                                     expected_bytes=_length(result.headers))
                digest = _digest(part_path(self.root, job_id, ordinal))
                fsync_replace(part_path(self.root, job_id, ordinal),
                              final_path(self.root, job_id, ordinal))
            self.store.set_file(job_id, ordinal, state='ok', bytes=counts['bytes'],
                                lines=counts['lines'], diagnostics=counts.get('diagnostics', counts['lines']),
                                receipt={'type': item['type'], 'bytes': counts['bytes'],
                                         'sha256': digest, 'lines': counts['lines']})
            self.store.event(job_id, 'diagnostic_file_received', {'ordinal': ordinal})
        except Cancelled:
            self.store.set_file(job_id, ordinal, state='cancelled',
                                reason='cancelled; partial bytes retained')
            self.store.event(job_id, 'download_cancelled', {'ordinal': ordinal})
            raise
        except (BadLine, OSError, ValueError) as error:
            self.store.set_file(job_id, ordinal, state='failed', reason=str(error))
            self.store.event(job_id, 'diagnostic_file_failed', {'ordinal': ordinal, 'error': str(error)})

    def _diagnostic_line(self, job_id, ordinal, document, line):
        kind = document.get('resourceType')
        if kind != 'OperationOutcome':
            raise BadLine(line, 'error file line is not an OperationOutcome')
        self.store.insert_diagnostic(job_id, ordinal, json.dumps(document, sort_keys=True))

    def _business_line(self, job_id, ordinal, file_type, document, line):
        kind = document.get('resourceType')
        if not isinstance(kind, str) or not kind:
            raise BadLine(line, 'resourceType must be a non-empty string')
        if kind == 'OperationOutcome':
            self.store.insert_diagnostic(job_id, ordinal, json.dumps(document, sort_keys=True))
            return 'diagnostic'
        if kind != file_type:
            raise BadLine(line, 'resourceType %s does not match declared type %s' % (kind, file_type))
        identity = document.get('id')
        if not isinstance(identity, str) or not identity:
            raise BadLine(line, 'id must be a non-empty string')
        meta = document.get('meta')
        if not isinstance(meta, dict):
            raise BadLine(line, 'meta object is required')
        version = meta.get('versionId')
        if not isinstance(version, str) or not version:
            raise BadLine(line, 'meta.versionId must be a non-empty string')
        payload = json.dumps(document, sort_keys=True)
        return self.store.insert_resource(job_id, ordinal, kind, identity, version,
                                          payload, references_of(document))

    def download(self, job_id):
        """Receive all pending output files, then promote if complete.

        The loop re-reads durable state before every request, so a job
        cancelled concurrently schedules no further downloads.
        """
        job = self.store.job(job_id)
        if job is None:
            raise StateError('Unknown job')
        if self._cancelled(job):
            raise Cancelled(job_id)
        if job['state'] in ('ready', 'diagnostic'):
            return job['state']
        files = self.store.files(job_id)
        if not files:
            raise StateError('No manifest has been received for this job')
        if any(item['kind'] == 'error' for item in files):
            self.store.set_job(job_id, state='diagnostic',
                               reason='manifest contains error files; batch not switched')
            return 'diagnostic'
        requires = bool(job['requires_access_token'])
        epoch = job['download_epoch']
        for item in files:
            current = self.store.job(job_id)
            if current is None or self._cancelled(current):
                raise Cancelled(job_id)
            if item['state'] == 'ok' and final_path(self.root, job_id, item['ordinal']).exists():
                continue
            self._download_file(job_id, item['ordinal'], item['type'], requires, epoch, current)
            current = self.store.job(job_id)
            if current is None or self._cancelled(current):
                raise Cancelled(job_id)
            if current['state'] not in ('downloading',):
                return current['state']
        files = self.store.files(job_id)
        failures = [item for item in files if item['kind'] == 'output' and item['state'] != 'ok']
        if failures:
            self.store.set_job(job_id, state='blocked',
                               reason='incomplete output files; previous visible batch kept')
            return 'blocked'
        return self._promote(job_id, epoch, len(files))

    def _download_file(self, job_id, ordinal, file_type, requires, epoch, job=None):
        self._checkpoint(job_id, epoch)
        item = self.store.file(job_id, ordinal)
        headers = self._auth(job or self.store.job(job_id))
        self.store.set_file(job_id, ordinal, state='downloading', reason=None,
                            bytes=0, lines=0, accepted=0, duplicates=0,
                            conflicts=0, diagnostics=0)
        reset_job_staging(self.store, job_id, ordinal)
        accepted = duplicates = conflicts = diagnostics = 0
        try:
            with open_stream(item['url'], headers=headers, trusted=self.trusted,
                             timeout=self.timeout, observer=self.observer) as (result, response):
                if result.status != 200:
                    raise ExportError('HTTP %d while downloading %s' % (result.status, item['type']))
                checkpoint = lambda: self._checkpoint(job_id, epoch)
                counts = read_ndjson(response, part_path(self.root, job_id, ordinal),
                                     lambda document, line: self._business_line(
                                         job_id, ordinal, file_type, document, line),
                                     checkpoint=checkpoint,
                                     expected_bytes=_length(result.headers),
                                     progress_path=self.progress_path(job_id, ordinal))
            digest = _digest(part_path(self.root, job_id, ordinal))
            fsync_replace(part_path(self.root, job_id, ordinal),
                          final_path(self.root, job_id, ordinal))
        except Cancelled:
            self.store.set_file(job_id, ordinal, state='cancelled',
                                reason='cancelled; partial bytes retained')
            self.store.event(job_id, 'download_cancelled', {'ordinal': ordinal})
            raise
        except (BadLine, ExportError, OSError, ValueError) as error:
            self.store.set_file(job_id, ordinal, state='failed', reason=str(error))
            self.store.set_job(job_id, state='blocked',
                               reason='download failed: ' + str(error))
            self.store.event(job_id, 'download_failed', {'ordinal': ordinal, 'error': str(error)})
            return
        stats = self.store.file(job_id, ordinal)
        self.store.set_file(job_id, ordinal, state='ok', reason=None,
                            bytes=counts['bytes'], lines=counts['lines'],
                            duplicates=stats['duplicates'] if stats else 0,
                            conflicts=stats['conflicts'] if stats else 0,
                            diagnostics=stats['diagnostics'] if stats else 0,
                            receipt={'type': file_type, 'bytes': counts['bytes'],
                                     'sha256': digest, 'lines': counts['lines']})
        self.store.event(job_id, 'file_received', {
            'ordinal': ordinal, 'type': file_type,
            'bytes': counts['bytes'], 'lines': counts['lines']})

    def _promote(self, job_id, epoch, output_files):
        fresh = self.store.job(job_id)
        if fresh is None:
            raise StateError('Unknown job')
        if fresh['cancel_requested'] or fresh['state'] == 'cancelled' or fresh['download_epoch'] != epoch:
            raise Cancelled(job_id)
        counts = self.store.staging_counts(job_id)
        error_files = sum(1 for item in self.store.files(job_id) if item['kind'] == 'error')
        duplicate_count = sum(item['duplicates'] for item in self.store.files(job_id))
        try:
            batch_id = self.store.promote(
                job_id, fresh['transaction_time'], bool(fresh['requires_access_token']),
                output_files, error_files, duplicate_count, epoch=epoch)
        except EpochChanged as error:
            raise Cancelled(job_id) from error
        self.store.event(job_id, 'batch_visible', {'batch': batch_id,
                         'resources': counts['resources'], 'conflicts': counts['conflicts'],
                         'diagnostics': counts['diagnostics']})
        return self.store.job(job_id)['state']

    # -- orchestration ---------------------------------------------------

    def run(self, job_id, max_polls=None, wait_budget=None):
        """Resume one persisted job through polling and downloading.

        Never issues a new export create request. wait_budget is wall-clock
        seconds measured from entry, so unattended runs cannot poll forever.
        """
        deadline = None if wait_budget is None else time.monotonic() + wait_budget
        polls = 0
        while True:
            job = self.store.job(job_id)
            if job is None:
                raise StateError('Unknown job')
            if self._cancelled(job):
                return 'cancelled'
            if deadline is not None and time.monotonic() >= deadline:
                return job['state']
            if job['state'] in ('ready', 'diagnostic'):
                return job['state']
            if job['state'] in ('starting', 'waiting', 'uncertain'):
                if not job['status_url']:
                    self.store.set_job(job_id, state='uncertain',
                                       reason='job address was never persisted; manual reconciliation required')
                    return 'uncertain'
                self.poll(job_id)
                job = self.store.job(job_id)
                if job['state'] == 'waiting':
                    polls += 1
                    if max_polls is not None and polls >= max_polls:
                        return 'waiting'
                    delay = self.last_retry_after or 0
                    self.clock(min(delay, self.poll_cap))
            elif job['state'] in ('downloading', 'blocked'):
                return self.download(job_id)
            elif job['state'] == 'cancelled':
                return 'cancelled'
            else:
                raise StateError('Unhandled job state: ' + job['state'])
            if self.store.job(job_id)['state'] == 'uncertain':
                return 'uncertain'
    # -- direct manifest -------------------------------------------------

    def receive(self, manifest_url):
        """Accept a manifest url directly (used for public/error demonstrations)."""
        require_trusted(manifest_url, self.trusted)
        job_id = uuid.uuid4().hex
        self.store.create_job(job_id, manifest_url)
        self.store.event(job_id, 'job_created', {'require_url': manifest_url, 'mode': 'manifest'})
        try:
            result = fetch(manifest_url, 'GET', headers=self._auth(), trusted=self.trusted,
                           timeout=self.timeout, observer=self.observer)
        except (OSError, ValueError) as error:
            self.store.set_job(job_id, state='uncertain', reason='manifest unavailable')
            raise UncertainJob(job_id) from error
        if result.status != 200:
            self.store.set_job(job_id, state='uncertain',
                               reason='manifest returned HTTP %d' % result.status)
            raise UncertainJob(job_id)
        self._accept_manifest(job_id, result.body)
        state = self.store.job(job_id)['state']
        if state == 'downloading':
            return job_id, self.download(job_id)
        return job_id, state

    # -- cancellation ----------------------------------------------------

    def cancel(self, job_id, send_delete=True):
        job = self.store.job(job_id)
        if job is None:
            raise StateError('Unknown job')
        if job['state'] == 'ready':
            return {'job': job_id, 'state': 'ready', 'delete_sent': False,
                    'note': 'visible batch is immutable'}
        self.store.mark_cancelled(job_id)
        self.store.event(job_id, 'cancel_requested', {'delete': bool(send_delete and job['status_url'])})
        delete_sent = False
        if send_delete and job['status_url']:
            try:
                result = fetch(job['status_url'], 'DELETE', headers=self._auth(job),
                               trusted=self.trusted, timeout=self.timeout, observer=self.observer)
                delete_sent = True
                self.store.event(job_id, 'cancel_response', {'status': result.status})
            except (OSError, ValueError) as error:
                self.store.event(job_id, 'cancel_request_failed', {'error': type(error).__name__})
        return {'job': job_id, 'state': 'cancelled', 'delete_sent': delete_sent}

    # -- reporting -------------------------------------------------------

    def status(self, job_id=None):
        latest = self.store.latest_batch()
        if job_id is not None:
            job = self.store.job(job_id)
        else:
            jobs = self.store.jobs()
            job = jobs[-1] if jobs else None
        return describe_job(job, self.store.files(job['id']) if job else [], latest)

    def summary(self, batch_id=None):
        if batch_id is None:
            batch = self.store.latest_batch()
        else:
            batch = next((item for item in self.store.batches()
                          if item['batch_id'] == batch_id), None)
        if batch is None:
            return {'visible_batch': None}
        resources = self.store.batch_resources(batch['batch_id'])
        types = {}
        identities = []
        for item in resources:
            types[item['resource_type']] = types.get(item['resource_type'], 0) + 1
            identities.append('%s/%s@%s' % (item['resource_type'], item['resource_id'],
                                            item['version_id']))
        digest = hashlib.sha256('\n'.join(sorted(identities)).encode()).hexdigest()
        return {
            'visible_batch': batch['batch_id'],
            'job': batch['job_id'],
            'transactionTime': batch['transaction_time'],
            'requiresAccessToken': bool(batch['requires_access_token']),
            'output_files': batch['output_files'],
            'error_files': batch['error_files'],
            'resource_counts': dict(sorted(types.items())),
            'resources': len(resources),
            'duplicates_collapsed': batch['duplicates'],
            'conflicts': batch['conflicts'],
            'diagnostics': batch['diagnostics'],
            'resolved_references': batch['resolved_references'],
            'dangling_references': batch['dangling_references'],
            'receipt_digest': digest,
            'file_receipts': [
                {'type': item['type'], 'bytes': item['bytes'], 'sha256': item['sha256'],
                 'lines': item['lines']}
                for item in self.store.receipts(batch['job_id'])
            ],
        }
