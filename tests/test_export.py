import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from healthbulk.export import Cancelled, Exporter, StateError, UncertainJob
from healthbulk.manifest import ManifestError, parse_manifest
from healthbulk.records import BadLine, read_ndjson, references_of
from healthbulk.store import Store
from healthbulk.transport import (normalize_trusted, origin_of, require_trusted,
                                  retry_after_seconds, TargetNotTrusted)
from scripts.materialize import main as materialize
from scripts.source_server import ReplayServer, running


def exporter(state, server, token='local-demo', clock=None):
    return Exporter(state, trusted=normalize_trusted([server.origin]), token=token,
                    timeout=3, clock=clock or (lambda seconds: None))


def snapshot(server):
    with server.guard:
        return list(server.requests)


class ContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        materialize()

    def test_manifest_validation_rejects_wrong_shapes(self):
        for payload in (b'nope', b'[]',
                        b'{"transactionTime":"t","requiresAccessToken":"true","output":[]}',
                        b'{"transactionTime":"t","requiresAccessToken":true,"output":{}}',
                        b'{"transactionTime":"t","requiresAccessToken":true,"output":[{"type":"P"}]}'):
            with self.assertRaises(ManifestError):
                parse_manifest(payload)
        manifest = parse_manifest(b'{"transactionTime":"2026-11-12T00:00:00Z",'
                                  b'"requiresAccessToken":false,"output":'
                                  b'[{"type":"Patient","url":"http://127.0.0.1/f"}],"error":[]}')
        self.assertEqual(manifest['output'][0]['type'], 'Patient')

    def test_retry_after_seconds_and_http_date(self):
        self.assertEqual(retry_after_seconds('0'), 0)
        self.assertEqual(retry_after_seconds('7'), 7)
        later = retry_after_seconds('Wed, 21 Oct 2099 07:28:00 GMT')
        self.assertGreater(later, 1000)
        past = retry_after_seconds('Thu, 01 Jan 1970 00:00:00 GMT')
        self.assertEqual(past, 0)
        with self.assertRaises(ValueError):
            retry_after_seconds('soon')

    def test_untrusted_target_rejected_before_io(self):
        with self.assertRaises(TargetNotTrusted):
            require_trusted('https://example.invalid/files', normalize_trusted(['http://127.0.0.1:1']))
        self.assertEqual(origin_of('http://127.0.0.1:8765/a'), 'http://127.0.0.1:8765')

    def test_reference_extraction_ignores_contained_and_absolute(self):
        document = {'resourceType': 'Observation', 'subject': {'reference': 'Patient/P1'},
                    'contained': [{'reference': '#inner'}],
                    'hasMember': [{'reference': 'http://elsewhere/fhir/Observation/O9'},
                                  {'reference': 'urn:uuid:1234'}]}
        self.assertEqual(references_of(document), [('Patient', 'P1')])

    def test_ndjson_rejects_bad_lines_without_partial_acceptance(self):
        class TruncatingResponse:
            """Declares 40 bytes but closes the connection after 20."""

            def __init__(self, payload, declared):
                self.payload = payload
                self.declared = declared

            def read1(self, size):
                out, self.payload = self.payload[:size], self.payload[size:]
                return out

        seen = []
        with tempfile.TemporaryDirectory() as directory:
            part = Path(directory) / 'part'
            with self.assertRaises(BadLine) as caught:
                read_ndjson(TruncatingResponse(b'{"a":1}\nnot json\n', 40), part,
                            lambda document, line: seen.append(line))
            self.assertEqual(caught.exception.line, 2)
            self.assertEqual(seen, [1])

        seen = []
        with tempfile.TemporaryDirectory() as directory:
            part = Path(directory) / 'part'
            truncated = TruncatingResponse(b'{"a":1}\n{"b":2', 25)
            with self.assertRaises(BadLine) as caught:
                read_ndjson(truncated, part, lambda document, line: seen.append(line),
                            expected_bytes=25)
            self.assertIn('truncated', str(caught.exception))
            self.assertEqual(seen, [1])


class AsyncLifecycleTests(unittest.TestCase):
    def test_polling_recovery_never_recreates_the_job(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            first = exporter(state, server)
            job, outcome = first.create(server.origin + '/Group/G1/$export')
            self.assertEqual(outcome, 'created')
            self.assertEqual(first.store.job(job)['state'], 'waiting')
            # A second process resumes from durable state only.
            second = exporter(state, server)
            self.assertEqual(second.create(server.origin + '/Group/G1/$export'),
                             (job, 'existing'))
            self.assertEqual(second.run(job), 'ready')
            creates = [item for item in snapshot(server) if '/$export' in item['path']]
            self.assertEqual(len(creates), 1)
            self.assertEqual(len(second.store.jobs()), 1)

    def test_connection_loss_before_address_marks_uncertain(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            address = server.origin + '/absent/$export'
            with self.assertRaises(UncertainJob):
                client.create(address)
            job = client.store.jobs()[-1]
            self.assertEqual(job['state'], 'uncertain')
            client.run(job['id'])
            self.assertEqual(client.store.job(job['id'])['state'], 'uncertain')
            unavailable = [item for item in snapshot(server) if item['path'] == '/absent/$export']
            self.assertEqual(len(unavailable), 1)

    def test_http_date_retry_after_is_injectable(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            waited = []
            client = exporter(state, server, clock=waited.append)
            job, _ = client.create(server.origin + '/Group/G3/$export')
            self.assertEqual(client.poll(job), 'waiting')
            self.assertGreater(client.last_retry_after, 1000)
            state_name = client.run(job)
            self.assertEqual(state_name, 'ready')
            polls = [item for item in snapshot(server) if item['path'] == '/jobs/J3']
            self.assertEqual(len(polls), 2)

    def test_poll_recovery_after_restart_uses_saved_address(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            job, _ = client.create(server.origin + '/slow-creating/$export')
            for _ in range(2):
                self.assertEqual(client.poll(job), 'waiting')
            # Restart: a fresh Exporter on the same state resumes the job.
            restarted = exporter(state, server)
            self.assertEqual(restarted.run(job), 'ready')
            polls = [item for item in snapshot(server) if item['path'] == '/jobs/JSLOW']
            self.assertGreaterEqual(len(polls), 3)
            self.assertEqual(len([item for item in snapshot(server) if '/$export' in item['path']]), 1)

    def test_cancel_prevents_new_downloads_and_deletes_job(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            job, _ = client.create(server.origin + '/Group/G1/$export')
            self.assertEqual(client.poll(job), 'waiting')
            result = client.cancel(job)
            self.assertTrue(result['delete_sent'])
            self.assertEqual(client.store.job(job)['state'], 'cancelled')
            self.assertEqual(client.run(job), 'cancelled')
            paths = [item['path'] for item in snapshot(server)]
            self.assertIn('/jobs/J1', [p for p in paths if p.endswith('J1')])
            self.assertFalse(any(p.startswith('/files/') for p in paths))
            self.assertEqual(len([item for item in snapshot(server)
                                  if item['method'] == 'DELETE']), 1)

    def test_cancel_race_leaves_no_visible_batch(self):
        """A cancellation that lands while bytes are streaming must win.

        A real second process cancels the job after the file has demonstrably
        started arriving; the downloader is then unblocked and must not
        promote anything.
        """
        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            job, _ = client.create(server.origin + '/Group/G2/$export')
            self.assertEqual(client.poll(job), 'waiting')
            self.assertEqual(client.poll(job), 'downloading')
            # The source streams slowly, giving the cancel a real window.
            thread = threading.Thread(target=self._cancel_later,
                                      args=(state, server, client.root, job))
            thread.start()
            with self.assertRaises(Cancelled):
                client.download(job)
            thread.join()
            self.assertEqual(client.store.job(job)['state'], 'cancelled')
            self.assertIsNone(client.store.latest_batch())

    def _cancel_later(self, state, server, root, job):
        cancel = Exporter(root, trusted=normalize_trusted([server.origin]),
                          token='local-demo', timeout=3, clock=lambda seconds: None)
        deadline = time.time() + 5
        while time.time() < deadline:
            marker = Path(root) / 'streams' / job / '000.progress'
            if marker.exists() and int(marker.read_text() or 0) > 0:
                break
            time.sleep(0.02)
        self.assertTrue(marker.exists() and int(marker.read_text() or 0) > 0)
        cancel.cancel(job)


class ReceiptTests(unittest.TestCase):
    def test_full_receipt_and_duplicate_collapse(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            job, state_name = client.receive(server.origin + '/public/manifest')
            self.assertEqual(state_name, 'ready')
            summary = client.summary()
            self.assertEqual(summary['resources'], 1)
            self.assertFalse(summary['requiresAccessToken'])
            self.assertEqual(client.status(job)['state'], 'ready')
            public = [item for item in snapshot(server) if item['path'] == '/public/patients']
            self.assertFalse(public[0]['authorization_sent'])
            self.assertTrue(all(not item['authorization_sent'] for item in public))
            self.assertEqual(
                client.store.receipt(job, 0)['sha256'],
                __import__('hashlib').sha256(
                    b'{"resourceType": "Patient", "id": "P1", "meta": {"versionId": "opaque-vA"}}\n'
                ).hexdigest())

    def test_requires_token_true_sends_bearer_and_false_does_not(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            job, _ = client.create(server.origin + '/Group/G1/$export')
            self.assertEqual(client.run(job), 'ready')
            file_requests = [item for item in snapshot(server) if item['path'].startswith('/files/')]
            self.assertEqual(len(file_requests), 2)
            self.assertTrue(all(item['authorization_sent'] for item in file_requests))
            self.assertEqual(file_requests[0]['headers']['authorization'], 'Bearer local-demo')
            events = {item['event'] for item in client.store.events(job)}
            self.assertIn('batch_visible', events)
            self.assertIn('file_received', events)

        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            job, _ = client.receive(server.origin + '/public/manifest')
            file_requests = [item for item in snapshot(server) if item['path'] == '/public/patients']
            self.assertEqual(len(file_requests), 1)
            self.assertFalse(file_requests[0]['authorization_sent'])

    def test_error_file_blocks_batch_and_previous_batch_stays_visible(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            good, _ = client.receive(server.origin + '/public/manifest')
            self.assertEqual(client.summary()['resources'], 1)
            previous = client.summary()['visible_batch']
            bad, state_name = client.receive(server.origin + '/error/manifest')
            self.assertEqual(state_name, 'diagnostic')
            self.assertEqual(client.summary()['visible_batch'], previous)
            self.assertEqual(client.summary()['resources'], 1)
            detail = client.status(bad)
            self.assertEqual(detail['state'], 'diagnostic')
            error_files = [item for item in detail['files'] if item['kind'] == 'error']
            self.assertEqual(len(error_files), 1)
            self.assertEqual(error_files[0]['state'], 'ok')
            self.assertEqual(error_files[0]['diagnostics'], 1)
            self.assertTrue(client.store.receipt(bad, error_files[0]['ordinal']))
            downloaded = [item['path'] for item in snapshot(server)]
            self.assertNotIn('/files/patients', downloaded)
            self.assertNotIn('/files/observations', downloaded)

    def test_bad_line_and_wrong_type_block_whole_batch(self):
        for path, expected in (('/badline/manifest', 'Line 2'),
                               ('/wrongtype/manifest', 'declared type')):
            with running() as server, tempfile.TemporaryDirectory() as state:
                client = exporter(state, server)
                job, state_name = client.receive(server.origin + path)
                self.assertEqual(state_name, 'blocked')
                self.assertIsNone(client.summary()['visible_batch'])
                self.assertIn(expected, client.status(job)['reason'])
                self.assertEqual(client.status(job)['files'][0]['state'], 'failed')

    def test_truncated_response_is_not_accepted(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            job, state_name = client.receive(server.origin + '/truncated/manifest')
            self.assertEqual(state_name, 'blocked')
            self.assertIn('truncated', client.status(job)['reason'])
            self.assertIsNone(client.summary()['visible_batch'])

    def test_late_patient_file_resolves_earlier_observation_reference(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            job, state_name = client.receive(server.origin + '/cross/manifest')
            self.assertEqual(state_name, 'ready')
            summary = client.summary()
            self.assertEqual(summary['resources'], 3)
            self.assertEqual(summary['resolved_references'], 1)
            self.assertEqual(summary['dangling_references'], 1)
            references = client.store.batch_references(summary['visible_batch'])
            resolved = [item for item in references if item['resolved']]
            self.assertEqual(len(resolved), 1)
            self.assertIn('Patient/P1', ['%s/%s' % (item['target_type'], item['target_id'])
                                         for item in resolved])
            dangling = [item for item in references if not item['resolved']]
            self.assertEqual(len(dangling), 1)
            self.assertIn('P9', dangling[0]['target_id'])

    def test_version_identity_is_opaque_and_conflicts_are_flagged(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            job, state_name = client.receive(server.origin + '/conflict/manifest')
            self.assertEqual(state_name, 'ready')
            summary = client.summary()
            self.assertEqual(summary['conflicts'], 1)
            self.assertEqual(summary['resources'], 2)
            identities = {(item['resource_id'], item['version_id'])
                          for item in client.store.batch_resources(summary['visible_batch'])}
            self.assertEqual(identities, {('P1', 'v10'), ('P1', 'v9')})
            self.assertNotEqual(sorted(identities)[0], ('P1', 'v9'))

    def test_diagnostic_operation_outcome_is_never_business_data(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            # A business file that embeds an OperationOutcome blocks a mismatched line? No:
            # it is staged as diagnostic only, not returned as a resource.
            server.auxiliary['GET /inline/manifest'] = [{
                'body': '{"transactionTime": "t", "requiresAccessToken": false, "output": '
                        '[{"type": "Patient", "url": "{{ORIGIN}}/inline/patients"}], "error": []}'}]
            server.auxiliary['GET /inline/patients'] = [{
                'body': '{"resourceType": "Patient", "id": "P1", "meta": {"versionId": "v1"}}\n'
                        '{"resourceType": "OperationOutcome", "issue": [{"severity": "warning"}]}\n'}]
            job, state_name = client.receive(server.origin + '/inline/manifest')
            self.assertEqual(state_name, 'ready')
            summary = client.summary()
            self.assertEqual(summary['resources'], 1)
            self.assertEqual(summary['diagnostics'], 1)
            kinds = {item['resource_type'] for item in
                     client.store.batch_resources(summary['visible_batch'])}
            self.assertEqual(kinds, {'Patient'})


class RestartAndCancelCrashTests(unittest.TestCase):
    def test_completed_files_are_not_recounted_after_restart(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            job, _ = client.create(server.origin + '/slow-creating/$export')
            while client.poll(job) == 'waiting':
                pass
            self.assertEqual(client.store.job(job)['state'], 'downloading')
            first = client.download(job)
            self.assertEqual(first, 'ready')
            summary = client.summary()
            before = summary['resources']
            restarted = exporter(state, server)
            self.assertEqual(restarted.run(job), 'ready')
            file_requests = [item for item in snapshot(server) if item['path'] == '/files/patients']
            self.assertEqual(len(file_requests), 1)
            self.assertEqual(restarted.summary()['resources'], before)

    def test_killed_downloader_recovers_reservable_progress(self):
        """Terminate a real subprocess mid-download, then resume from disk.

        The source streams slowly, so the child is killed while a file is in
        progress and the first file is already fully on disk.
        """
        with running() as server, tempfile.TemporaryDirectory() as state:
            scenario = Path(state) / 'demo' / 'scenario.json'
            scenario.parent.mkdir()
            with open(scenario, 'w') as stream:
                json.dump({'routes': {
                    'GET /Group/G2/$export': [{
                        'status': 202,
                        'headers': {'Content-Location': '{{ORIGIN}}/jobs/J2'}}],
                    'GET /jobs/J2': [{
                        'body': '{"transactionTime": "2026-11-12T00:00:00Z", '
                                '"requiresAccessToken": true, "output": ['
                                '{"type": "Patient", "url": "{{ORIGIN}}/slow/patients"}], '
                                '"error": []}'}],
                    'GET /slow/patients': [{
                        'chunk': 16, 'delay': 0.05,
                        'body': ''.join(
                            '{"resourceType": "Patient", "id": "P%d", '
                            '"meta": {"versionId": "v%d"}}\n' % (index, index)
                            for index in range(1, 13))}],
                }}, stream)
            slow = ReplayServer(scenario.parent, scenario.name)
            slow_thread = threading.Thread(target=slow.serve_forever, daemon=True)
            slow_thread.start()
            try:
                child = subprocess.Popen(
                    [sys.executable, '-m', 'healthbulk', 'start',
                     '--start', slow.origin + '/Group/G2/$export',
                     '--origin', '{{ORIGIN}}', '--token', 'local-demo',
                     '--state', state, '--wait', '--wait-budget', '20'],
                    cwd=Path(__file__).resolve().parent.parent,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                # Wait for a complete first line to be durably written, which
                # proves the download is genuinely mid-stream.
                part_prefix = Path(state) / 'streams'
                deadline = time.time() + 15
                while time.time() < deadline:
                    finished = list(part_prefix.rglob('000.ndjson'))
                    if finished and any(item.stat().st_size > 0 for item in finished):
                        break
                    time.sleep(0.01)
                child.terminate()
                child.wait(timeout=10)
                client = exporter(state, slow)
                jobs = client.store.jobs()
                self.assertEqual(len(jobs), 1)
                self.assertIn(jobs[0]['state'], ('downloading', 'blocked', 'waiting', 'ready'))
                created = [item for item in snapshot(slow) if '/$export' in item['path']]
                self.assertEqual(len(created), 1)
                self.assertEqual(client.run(jobs[0]['id']), 'ready')
                self.assertEqual(len([item for item in snapshot(slow)
                                      if '/$export' in item['path']]), 1)
                # The file that completed before the kill is reused, not re-counted.
                files = client.store.files(jobs[0]['id'])
                self.assertEqual(len(files), 1)
                self.assertEqual(len([item for item in snapshot(slow)
                                      if item['path'] == '/slow/patients']), 1)
                self.assertEqual(client.summary()['resources'], 12)
            finally:
                slow.shutdown()
                slow.server_close()
                slow_thread.join()

    def test_restart_after_cancel_does_not_download(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            job, _ = client.create(server.origin + '/Group/G1/$export')
            client.poll(job)
            client.cancel(job, send_delete=True)
            restarted = exporter(state, server)
            self.assertEqual(restarted.run(job), 'cancelled')
            self.assertFalse(any(item['path'].startswith('/files/')
                                 for item in snapshot(server)))


class RedirectTests(unittest.TestCase):
    def test_same_origin_redirect_keeps_authorization(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            server.auxiliary['GET /redir/manifest'] = [{
                'redirect_to': '{{ORIGIN}}/redir/manifest-target'}]
            server.auxiliary['GET /redir/manifest-target'] = [{
                'body': '{"transactionTime": "t", "requiresAccessToken": false, "output": '
                        '[{"type": "Patient", "url": "{{ORIGIN}}/public/patients"}], "error": []}'}]
            client = exporter(state, server)
            job, state_name = client.receive(server.origin + '/redir/manifest')
            self.assertEqual(state_name, 'ready')
            hops = [item for item in snapshot(server)
                    if item['path'] in ('/redir/manifest', '/redir/manifest-target')]
            self.assertEqual(len(hops), 2)
            self.assertTrue(hops[0]['authorization_sent'])
            self.assertTrue(hops[1]['authorization_sent'])

    def test_cross_origin_redirect_to_untrusted_target_is_refused(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            client = exporter(state, server)
            with self.assertRaises(UncertainJob):
                client.receive(server.origin + '/redirect/files/patients')
            self.assertEqual(client.store.jobs()[0]['state'], 'uncertain')
            self.assertEqual(len(client.store.jobs()), 1)

    def test_cross_origin_redirect_strips_authorization(self):
        with running() as server, tempfile.TemporaryDirectory() as state:
            other = ReplayServer('fixtures', 'source.json')
            other_thread = threading.Thread(target=other.serve_forever, daemon=True)
            other_thread.start()
            try:
                other.auxiliary['GET /hop/manifest'] = [{
                    'body': '{"transactionTime": "t", "requiresAccessToken": false, "output": '
                            '[{"type": "Patient", "url": "{{ORIGIN}}/public/patients"}], '
                            '"error": []}'}]
                server.auxiliary['GET /hop/manifest'] = [{
                    'redirect_to': other.origin + '/hop/manifest'}]
                client = Exporter(state,
                                  trusted=normalize_trusted([server.origin, other.origin]),
                                  token='local-demo', timeout=3, clock=lambda s: None)
                job, state_name = client.receive(server.origin + '/hop/manifest')
                self.assertEqual(state_name, 'ready')
                first_hop = [item for item in snapshot(server)
                             if item['path'] == '/hop/manifest'][0]
                cross_hop = [item for item in snapshot(other)
                             if item['path'] == '/hop/manifest'][0]
                other_file = [item for item in snapshot(other)
                              if item['path'] == '/public/patients'][0]
                self.assertTrue(first_hop['authorization_sent'])
                self.assertFalse(cross_hop['authorization_sent'])
                self.assertFalse(other_file['authorization_sent'])
            finally:
                other.shutdown()
                other.server_close()
                other_thread.join()


if __name__ == '__main__':
    unittest.main()
