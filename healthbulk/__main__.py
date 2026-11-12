import argparse
import json
from pathlib import Path
import sys
from .parser import inspect
from .journal import Journal
from .transport import capture
from .cli import main as export_main


def main():
    if len(sys.argv) > 1 and sys.argv[1] in ('start', 'resume', 'status', 'cancel',
                                             'receive', 'summary'):
        return export_main(sys.argv[1:])
    parser = argparse.ArgumentParser(description='体检服务 FHIR 导出清单的离线检查与响应留存。')
    sub = parser.add_subparsers(dest='command', required=True)
    view = sub.add_parser('manifest')
    view.add_argument('source')
    raw = sub.add_parser('capture')
    raw.add_argument('url')
    raw.add_argument('--state', default='var')
    raw.add_argument('--method', default='GET')
    raw.add_argument('--body')
    raw.add_argument('--header', action='append', default=[])
    args = parser.parse_args()
    try:
        if args.command == 'capture':
            headers = dict(value.split(':', 1) for value in args.header)
            headers = {key.strip(): value.strip() for key, value in headers.items()}
            status, response_headers, payload = capture(args.url, args.method,
                Path(args.body).read_bytes() if args.body else None, headers)
            journal = Journal(args.state)
            try:
                result = journal.save(payload, args.url)
            finally:
                journal.close()
            result.update({'http_status': status, 'headers': response_headers})
        else:
            result = inspect(Path(args.source).read_bytes())
        print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())