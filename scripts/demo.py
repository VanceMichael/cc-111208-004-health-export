import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time

from healthbulk.export import Exporter, Cancelled, UncertainJob
from healthbulk.transport import normalize_trusted
from .materialize import main as materialize
from .source_server import ReplayServer, running

TOKEN = 'local-demo'


def new_state():
    return tempfile.mkdtemp(prefix='healthbulk-demo-')


def client(state, origin, token=TOKEN):
    return Exporter(state, trusted=normalize_trusted([origin]), token=token,
                    timeout=4, clock=lambda seconds: None)


def request_lines(records):
    return ['%s %s auth=%s' % (item['method'], item['path'],
                               'Bearer' if item['authorization_sent'] else 'none')
            for item in records]


def run_cli(command, state, origin):
    return subprocess.run(
        [sys.executable, '-m', 'healthbulk', command, '--state', state,
         '--origin', origin, '--token', TOKEN],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True, text=True, timeout=30)


def cancel_from_other_process(state, origin, job, marker):
    """A real child process cancels the job once bytes demonstrably landed."""
    script = (
        'import sys, time\n'
        'from pathlib import Path\n'
        'from healthbulk.export import Exporter\n'
        'from healthbulk.transport import normalize_trusted\n'
        'state, origin, job, marker = sys.argv[1:5]\n'
        'client = Exporter(state, trusted=normalize_trusted([origin]), token="%s",\n'
        '                  timeout=4, clock=lambda s: None)\n'
        'for _ in range(400):\n'
        '    p = Path(marker)\n'
        '    if p.exists() and int(p.read_text() or 0) > 0:\n'
        '        break\n'
        '    time.sleep(0.01)\n'
        'print(client.cancel(job))\n' % TOKEN)
    return subprocess.Popen([sys.executable, '-c', script, state, origin, job, marker],
                            cwd=Path(__file__).resolve().parent.parent)


def demo():
    materialize()
    print('== 本地合成机构 FHIR export：创建 -> 持久化 -> 轮询 -> 流式接收 ==')
    print('（全部数据为本地合成，接收摘要不含任何业务字段）')
    with running() as server, tempfile.TemporaryDirectory() as state:
        origin = server.origin
        worker = client(state, origin)
        job, outcome = worker.create(origin + '/Group/G1/$export')
        print('job=%s outcome=%s persisted_state=%s' % (job, outcome,
              worker.store.job(job)['state']))
        print('status_url persisted=%s' % worker.store.job(job)['status_url'])

        # A second, independent worker resumes strictly from durable state.
        resumed = client(state, origin)
        print('restart create -> %s' % (resumed.create(origin + '/Group/G1/$export'),))
        final = resumed.run(job)
        print('resumed state=%s' % final)
        summary = resumed.summary()
        print('receipt=' + json.dumps(summary, ensure_ascii=True, sort_keys=True))

        requests = [item for item in server.requests if item['path'].startswith('/')]
        print('real requests:')
        for line in request_lines(requests):
            print('  ' + line)
        creates = [item for item in requests if '/$export' in item['path']]
        print('export create count=%d (must be 1)' % len(creates))

        # A diagnostic (error file) export must never replace the visible batch.
        diagnostic = client(state, origin)
        bad_job, state_name = diagnostic.receive(origin + '/error/manifest')
        print('diagnostic job state=%s visible_batch=%s'
              % (state_name, diagnostic.summary()['visible_batch']))
        print('previous visible batch still readable: resources=%d'
              % diagnostic.summary()['resources'])

        # requiresAccessToken=false: the same receipt path, no Authorization header.
        public_state = new_state()
        try:
            public = client(public_state, origin)
            public_job, public_name = public.receive(origin + '/public/manifest')
            print('public manifest state=%s requiresAccessToken=%s'
                  % (public_name, public.summary()['requiresAccessToken']))
            for line in request_lines([item for item in server.requests
                                       if item['path'] == '/public/patients']):
                print('  ' + line)
        finally:
            shutil.rmtree(public_state, ignore_errors=True)

    print('== 取消竞态：另一进程在下载途中取消，禁止切换到可用批次 ==')
    with running() as server, tempfile.TemporaryDirectory() as state:
        origin = server.origin
        worker = client(state, origin)
        job, _ = worker.create(origin + '/Group/G2/$export')
        worker.poll(job)
        worker.poll(job)
        marker = str(Path(worker.root) / 'streams' / job / '000.progress')
        child = cancel_from_other_process(state, origin, job, marker)
        try:
            worker.download(job)
            outcome = 'ready'
        except Cancelled:
            outcome = 'cancelled'
        child.wait(timeout=20)
        print('download outcome=%s job_state=%s visible_batch=%s progress_on_disk=%s'
              % (outcome, worker.store.job(job)['state'],
                 worker.store.latest_batch(),
                 sorted(item.name for item in (Path(worker.root) / 'streams' / job).iterdir())))
        print('cancel delete request=%s'
              % [line for line in request_lines(server.requests) if 'DELETE' in line])

    print('== 真实子进程被终止后重启：已落盘文件不重复计数 ==')
    with running() as server, tempfile.TemporaryDirectory() as state:
        origin = server.origin
        child = subprocess.Popen(
            [sys.executable, '-m', 'healthbulk', 'start', '--start', origin + '/Group/G1/$export',
             '--origin', '{{ORIGIN}}', '--token', TOKEN, '--state', state, '--wait'],
            cwd=Path(__file__).resolve().parent.parent,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 15
        while time.time() < deadline:
            with server.guard:
                if any(item['path'].startswith('/files/') for item in server.requests):
                    break
            time.sleep(0.02)
        child.terminate()
        child.wait(timeout=10)
        restarted = client(state, origin)
        job = restarted.store.jobs()[0]['id']
        print('after kill job_state=%s files=%s progress=%s'
              % (restarted.store.job(job)['state'],
                 [(f['ordinal'], f['state'], f['accepted']) for f in restarted.store.files(job)],
                 [(f['ordinal'], f['bytes']) for f in restarted.store.files(job)]))
        print('resume -> %s' % restarted.run(job))
        print('receipt=' + json.dumps(restarted.summary(), ensure_ascii=True, sort_keys=True))
        print('create requests after restart=%d'
              % len([item for item in server.requests if '/$export' in item['path']]))

    print('== 不允许：丢失去创建地址时不偷偷重建 ==')
    with running() as server, tempfile.TemporaryDirectory() as state:
        origin = server.origin
        worker = client(state, origin)
        try:
            worker.create(origin + '/lost/$export')
        except UncertainJob:
            pass
        job = worker.store.jobs()[-1]
        print('state=%s reason=%s' % (worker.store.job(job['id'])['state'],
                                     worker.store.job(job['id'])['reason']))
        worker.run(job['id'])
        print('after resume state=%s create calls=%d'
              % (worker.store.job(job['id'])['state'],
                 len([item for item in server.requests if item['path'] == '/lost/$export'])))


if __name__ == '__main__':
    demo()
