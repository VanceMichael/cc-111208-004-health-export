import argparse
import json
import sys
from urllib.parse import urlsplit

from .export import ExportError, Exporter, StateError
from .transport import normalize_trusted


def _client(args):
    origins = list(getattr(args, 'origin', None) or [])
    if any('{{ORIGIN}}' in item for item in origins):
        reference = getattr(args, 'start', None) or getattr(args, 'manifest', None)
        if not reference:
            raise ValueError('--origin {{ORIGIN}} requires --start or --manifest')
        parts = urlsplit(reference)
        prefix = parts.scheme + '://' + parts.netloc
        origins = [item.replace('{{ORIGIN}}', prefix) for item in origins]
    trusted = normalize_trusted(origins) if origins else None
    return Exporter(args.state, trusted=trusted, token=getattr(args, 'token', None),
                    timeout=args.timeout, poll_cap=args.poll_cap)


def _print(value):
    print(json.dumps(value, ensure_ascii=True, sort_keys=True))


def command_start(args):
    exporter = _client(args)
    job_id, outcome = exporter.create(args.start, force_new=args.force_new)
    if outcome == 'created':
        if args.wait:
            exporter.run(job_id, wait_budget=args.wait_budget)
        else:
            exporter.download(job_id) if exporter.store.files(job_id) else None
    _print(exporter.status(job_id))


def command_resume(args):
    exporter = _client(args)
    job_id = args.job or (exporter.store.jobs()[-1]['id'] if exporter.store.jobs() else None)
    if job_id is None:
        raise StateError('No persisted job to resume')
    state = exporter.run(job_id, max_polls=args.max_polls, wait_budget=args.wait_budget)
    _print(exporter.status(job_id))
    return 0 if state not in ('blocked',) else 3


def command_status(args):
    exporter = _client(args)
    job_id = args.job or (exporter.store.jobs()[-1]['id'] if exporter.store.jobs() else None)
    _print(exporter.status(job_id))


def command_cancel(args):
    exporter = _client(args)
    job_id = args.job or (exporter.store.jobs()[-1]['id'] if exporter.store.jobs() else None)
    if job_id is None:
        raise StateError('No persisted job to cancel')
    _print(exporter.cancel(job_id, send_delete=not args.no_delete))


def command_receive(args):
    exporter = _client(args)
    job_id, state = exporter.receive(args.manifest)
    _print(exporter.status(job_id))
    return 0 if state == 'ready' else 3


def command_summary(args):
    exporter = _client(args)
    _print(exporter.summary(args.batch))


def main(argv=None):
    parser = argparse.ArgumentParser(description='体检 FHIR Bulk Data 导出的可审计接收。')
    sub = parser.add_subparsers(dest='command', required=True)

    def common(subparser):
        subparser.add_argument('--state', required=True)
        subparser.add_argument('--origin', action='append', default=[],
                               help='trusted origin; {{ORIGIN}} expands to the --start origin')
        subparser.add_argument('--token', help='bearer token used only when requiresAccessToken=true')
        subparser.add_argument('--timeout', type=float, default=5)
        subparser.add_argument('--poll-cap', type=float, default=30)

    start = sub.add_parser('start')
    common(start)
    start.add_argument('--start', required=True, help='Group/$export url')
    start.add_argument('--wait', action='store_true')
    start.add_argument('--wait-budget', type=float, default=8)
    start.add_argument('--force-new', action='store_true')

    resume = sub.add_parser('resume')
    common(resume)
    resume.add_argument('--job')
    resume.add_argument('--max-polls', type=int)
    resume.add_argument('--wait-budget', type=float)

    status = sub.add_parser('status')
    common(status)
    status.add_argument('--job')

    cancel = sub.add_parser('cancel')
    common(cancel)
    cancel.add_argument('--job')
    cancel.add_argument('--no-delete', action='store_true')

    receive = sub.add_parser('receive')
    common(receive)
    receive.add_argument('--manifest', required=True)

    summary = sub.add_parser('summary')
    common(summary)
    summary.add_argument('--batch', type=int)

    args = parser.parse_args(argv)
    handlers = {'start': command_start, 'resume': command_resume, 'status': command_status,
                'cancel': command_cancel, 'receive': command_receive, 'summary': command_summary}
    try:
        return handlers[args.command](args) or 0
    except (ValueError, OSError, ExportError) as error:
        print(str(error), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
