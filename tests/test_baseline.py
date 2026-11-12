import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from healthbulk.parser import inspect
from healthbulk.journal import Journal
from healthbulk.transport import capture
from scripts.materialize import main as materialize
from scripts.source_server import running


class BaselineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        materialize()

    def test_capture_journal_is_content_addressed(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = Journal(directory)
            try:
                first = journal.save(b'synthetic', 'source-1')
                second = journal.save(b'synthetic', 'source-1')
                self.assertEqual(first, second)
                self.assertEqual(Path(first['path']).read_bytes(), b'synthetic')
                self.assertEqual(journal.database.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 1)
            finally:
                journal.close()

    def test_remote_transport_is_rejected_before_io(self):
        with self.assertRaises(ValueError):
            capture('https://example.invalid/private')

    def test_cli_help(self):
        result = subprocess.run([sys.executable, '-m', 'healthbulk', '--help'], capture_output=True)
        self.assertEqual(result.returncode, 0)
        self.assertIn(b'manifest', result.stdout)

    def test_local_source_and_request_log(self):
        with running() as server:
            status, headers, payload = capture(server.origin + '/public/manifest', 'GET', headers={})
            self.assertEqual(status, 200)
            self.assertGreater(len(payload), 0)
            self.assertEqual(server.requests[0]['method'], 'GET')

    def test_unknown_source_route_is_not_success(self):
        with running() as server:
            self.assertEqual(capture(server.origin + '/not-present')[0], 404)

    def test_manifest_file_types(self):
        result = inspect(Path('fixtures/manifest.json').read_bytes())
        self.assertEqual([item['type'] for item in result['files']], ['Patient', 'Observation'])

    def test_requires_access_token_is_not_string(self):
        with self.assertRaises(ValueError):
            inspect(b'{"transactionTime":"t", "requiresAccessToken":"false"}')

    def test_async_job_and_auth_contract(self):
        with running() as server:
            status, headers, payload = capture(server.origin + '/Group/G1/$export')
            self.assertEqual(status, 202)
            self.assertIn('/jobs/J1', headers['Content-Location'])
            self.assertEqual(capture(server.origin + '/jobs/J1')[0], 202)
            self.assertEqual(capture(server.origin + '/jobs/J1')[0], 200)
            self.assertEqual(capture(server.origin + '/files/patients')[0], 412)
            self.assertEqual(capture(server.origin + '/files/patients', headers={'Authorization': 'Bearer local-demo'})[0], 200)


if __name__ == '__main__':
    unittest.main()
