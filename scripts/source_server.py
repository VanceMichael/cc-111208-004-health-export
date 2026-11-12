import argparse
import base64
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
from urllib.parse import parse_qs


class ReplayServer(ThreadingHTTPServer):
    def __init__(self, root, scenario, host='127.0.0.1', port=0):
        self.root = Path(root).resolve()
        self.routes = json.loads((self.root / scenario).read_text())['routes']
        self.counts = {}
        self.requests = []
        self.guard = threading.Lock()
        super().__init__((host, port), ReplayHandler)

    @property
    def origin(self):
        return 'http://127.0.0.1:' + str(self.server_port)


class ReplayHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def dispatch(self):
        length = int(self.headers.get('Content-Length', '0'))
        if length > 1048576:
            self.send_error(413)
            return
        incoming = self.rfile.read(length)
        with self.server.guard:
            self.server.requests.append({'method': self.command, 'path': self.path,
                                         'headers': dict(self.headers), 'body': incoming.decode(errors='replace')})
            if self.path == '/__requests' and self.command == 'GET':
                self.respond(200, {}, json.dumps(self.server.requests[:-1]).encode())
                return
            key = self.command + ' ' + self.path
            choices = self.server.routes.get(key)
            if not choices:
                self.respond(404, {}, b'unknown local source route')
                return
            offset = self.server.counts.get(key, 0)
            choice = choices[min(offset, len(choices) - 1)]
            expected = choice.get('expect', {})
            for name, value in expected.get('headers', {}).items():
                if self.headers.get(name) != value:
                    self.respond(412, {}, b'header precondition failed')
                    return
            if 'form' in expected and parse_qs(incoming.decode(), keep_blank_values=True) != expected['form']:
                self.respond(409, {}, b'stale or invalid form state')
                return
            if 'contains' in expected and expected['contains'].encode() not in incoming:
                self.respond(400, {}, b'request body mismatch')
                return
            self.server.counts[key] = offset + 1
            if 'file' in choice:
                payload = (self.server.root / choice['file']).read_bytes()
            elif 'base64' in choice:
                payload = base64.b64decode(choice['base64'])
            else:
                payload = choice.get('body', '').encode()
            payload = payload.replace(b'{{ORIGIN}}', self.server.origin.encode())
            headers = {name: value.replace('{{ORIGIN}}', self.server.origin)
                       for name, value in choice.get('headers', {}).items()}
            self.respond(choice.get('status', 200), headers, payload)

    def respond(self, status, headers, payload):
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = dispatch
    do_POST = dispatch
    do_PROPFIND = dispatch
    do_REPORT = dispatch
    do_DELETE = dispatch


@contextmanager
def running(root='fixtures', scenario='source.json'):
    server = ReplayServer(root, scenario)
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
    parser.add_argument('--scenario', default='source.json')
    parser.add_argument('--port', type=int, default=8765)
    args = parser.parse_args()
    with ReplayServer('fixtures', args.scenario, port=args.port) as server:
        print(server.origin, flush=True)
        server.serve_forever()


if __name__ == '__main__':
    main()
