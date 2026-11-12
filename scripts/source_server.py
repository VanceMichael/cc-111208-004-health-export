import argparse
import base64
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import time
from urllib.parse import parse_qs, urlsplit

OBSERVABLE_HEADERS = ('authorization', 'prefer', 'accept', 'content-type', 'retry-after')


class ReplayServer(ThreadingHTTPServer):
    def __init__(self, root, scenario, host='127.0.0.1', port=0):
        self.root = Path(root).resolve()
        self.routes = json.loads((self.root / scenario).read_text())['routes']
        self.counts = {}
        self.requests = []
        self.guard = threading.Lock()
        self.trusted = True
        self.deleted_jobs = set()
        self.substitutions = {}
        self.auxiliary = {}
        super().__init__((host, port), ReplayHandler)

    def handle_error(self, request, client_address):
        # Clients that cancel or die mid-stream close the connection; that is
        # an expected event in the cancellation demonstration, not a server
        # failure worth printing.
        import sys
        if sys.exc_info()[0] in (BrokenPipeError, ConnectionResetError):
            return
        super().handle_error(request, client_address)

    @property
    def origin(self):
        return 'http://127.0.0.1:' + str(self.server_port)

    def expand(self, text):
        places = dict(self.substitutions, ORIGIN=self.origin)
        for name, value in places.items():
            text = text.replace('{{%s}}' % name, value)
        return text

    def log(self, record):
        with self.guard:
            self.requests.append(record)


class ReplayHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    # -- control surface -------------------------------------------------

    def record(self, body):
        headers = {}
        for name, value in self.headers.items():
            if name.lower() in OBSERVABLE_HEADERS:
                headers[name.lower()] = value
        record = {'method': self.command, 'path': self.path,
                  'headers': headers,
                  'authorization_sent': 'authorization' in headers,
                  'body': body.decode(errors='replace')[:4096],
                  'origin': urlsplit(self.server.origin).netloc}
        self.server.log(record)
        return record

    def dispatch(self):
        length = int(self.headers.get('Content-Length', '0'))
        if length > 1048576:
            self.send_error(413)
            return
        incoming = self.rfile.read(length)

        if self.path == '/__requests' and self.command == 'GET':
            with self.server.guard:
                snapshot = list(self.server.requests)
            self.respond(200, {'Content-Type': 'application/json'},
                         json.dumps(snapshot).encode())
            return

        if self.path == '/__control' and self.command in ('POST', 'GET'):
            if self.command == 'POST':
                patch = json.loads(incoming or b'{}')
                if 'trusted' in patch:
                    self.server.trusted = bool(patch['trusted'])
                if 'substitutions' in patch:
                    self.server.substitutions.update(
                        {str(key): str(value) for key, value in patch['substitutions'].items()})
                if patch.get('reset_counts'):
                    self.server.counts.clear()
            self.respond(200, {'Content-Type': 'application/json'},
                         json.dumps({'trusted': self.server.trusted}).encode())
            return

        self.record(incoming)
        if not self.server.trusted:
            self.respond(403, {'Content-Type': 'text/plain'}, b'origin is not trusted by scenario')
            return

        path = self.path.split('?', 1)[0]
        key = self.command + ' ' + path
        with self.server.guard:
            if self.command == 'GET' and path.startswith('/jobs/') and path in self.server.deleted_jobs:
                self.respond(404, {'Content-Type': 'text/plain'}, b'job deleted')
                return
            if self.command == 'DELETE' and path.startswith('/jobs/'):
                self.server.deleted_jobs.add(path)
            choices = self.server.routes.get(key) or self.server.auxiliary.get(key)
            if not choices:
                self.respond(404, {'Content-Type': 'text/plain'}, b'unknown local source route')
                return
            offset = self.server.counts.get(key, 0)
            choice = choices[min(offset, len(choices) - 1)]
            self.server.counts[key] = offset + 1

        expected = choice.get('expect', {})
        for name, value in expected.get('headers', {}).items():
            if self.headers.get(name) != value:
                self.respond(412, {'Content-Type': 'text/plain'}, b'header precondition failed')
                return
        if 'form' in expected and parse_qs(incoming.decode(), keep_blank_values=True) != expected['form']:
            self.respond(409, {'Content-Type': 'text/plain'}, b'stale or invalid form state')
            return
        if 'contains' in expected and expected['contains'].encode() not in incoming:
            self.respond(400, {'Content-Type': 'text/plain'}, b'request body mismatch')
            return

        if 'redirect_to' in choice:
            target = self.server.expand(choice['redirect_to'])
            self.respond(choice.get('redirect_status', 302),
                         {'Location': target}, b'redirect')
            return

        if 'file' in choice:
            payload = (self.server.root / choice['file']).read_bytes()
        elif 'base64' in choice:
            payload = base64.b64decode(choice['base64'])
        else:
            payload = choice.get('body', '').encode()
        payload = self.server.expand(payload.decode(errors='replace')).encode()
        headers = {name: self.server.expand(value)
                   for name, value in choice.get('headers', {}).items()}
        status = choice.get('status', 200)

        if choice.get('drop_after') is not None:
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload[:int(choice['drop_after'])])
            self.wfile.flush()
            self.close_connection = True
            return

        if choice.get('chunk'):
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            size = int(choice['chunk'])
            delay = float(choice.get('delay', 0))
            for start in range(0, len(payload), size):
                self.wfile.write(payload[start:start + size])
                self.wfile.flush()
                if delay:
                    time.sleep(delay)
            return

        self.respond(status, headers, payload)

    def respond(self, status, headers, payload):
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = dispatch
    do_POST = dispatch
    do_PUT = dispatch
    do_PATCH = dispatch
    do_DELETE = dispatch


@contextmanager
def running(root='fixtures', scenario='source.json'):
    with ReplayServer(root, scenario) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield server
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', default='fixtures')
    parser.add_argument('--scenario', default='source.json')
    parser.add_argument('--port', type=int, default=8765)
    args = parser.parse_args()
    server = ReplayServer(args.root, args.scenario, port=args.port)
    print(server.origin, flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
