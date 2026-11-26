"""
Faults around MageML's warm Python worker processes (warm_workers.py), which outlive the
block they ran: what a block leaves behind must not reach the next one, a process that dies
between blocks is replaced, and a run that is killed (a block timeout, a crashed scheduler)
leaves no process behind.

One worker, plain (the standard library only), prepared once for the module.
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import psutil
import pytest

from integration_tests.workers.test_worker_pipelines import (
    by_name,
    completed,
    error_of,
    output,
    pipeline_of,
    run,
    write_worker,
)

pytestmark = pytest.mark.skipif(shutil.which('uv') is None, reason='uv is not installed')


@pytest.fixture(scope='module')
def workers(mage_project, tmp_path_factory):
    from mage_ai.data_preparation import environments, warm_workers

    previous = os.environ.get(environments.ENVIRONMENTS_DIR_ENV)
    os.environ[environments.ENVIRONMENTS_DIR_ENV] = str(tmp_path_factory.mktemp('prepared'))
    folder = Path(mage_project, environments.WORKERS_DIR, 'plain')
    write_worker(folder, 'plain', '>=3.12', [])
    environments.prepare(environments.named_workers(mage_project)['plain'], log=lambda _: None)
    yield mage_project
    warm_workers.stop_all()
    shutil.rmtree(folder, ignore_errors=True)
    if previous is None:
        os.environ.pop(environments.ENVIRONMENTS_DIR_ENV, None)
    else:
        os.environ[environments.ENVIRONMENTS_DIR_ENV] = previous


def logs_of(block_run) -> str:
    return json.dumps(block_run.logs)


def seconds(block_run) -> float:
    return (block_run.completed_at - block_run.started_at).total_seconds()


def run_fused(pipeline):
    # A fused chain runs in one job, so its blocks in one worker share a warm process; each
    # block of an unfused run is a job of its own.
    from mage_ai.orchestration import fusion

    return completed(run(pipeline, fusion.BLOCK_FUSION_CHAINS))


def pids(pipeline, pipeline_run, names):
    return [output(pipeline, pipeline_run, name)['pid'] for name in names]


def gone(pid: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
                return True
        except psutil.NoSuchProcess:
            return True
        time.sleep(0.2)
    return False


def test_a_block_that_replaces_its_output_streams_leaves_the_next_its_log(workers):
    from mage_ai.data_preparation import warm_workers

    pipeline = pipeline_of(workers, [
        ('quiet', 'data_loader', 'plain', '''
            import io
            import os
            import sys

            @data_loader
            def load(*args, **kwargs):
                sys.stdout = io.StringIO()
                sys.stderr = io.StringIO()
                return {'pid': os.getpid()}
        '''),
        ('speaks', 'transformer', 'plain', '''
            import os
            import sys

            @transformer
            def speak(data, *args, **kwargs):
                print('speaks on stdout')
                print('speaks on stderr', file=sys.stderr)
                return {'pid': os.getpid()}
        '''),
    ])
    pipeline_run = run_fused(pipeline)
    assert len(set(pids(pipeline, pipeline_run, ['quiet', 'speaks']))) == 1
    speaks = by_name(pipeline_run)['speaks']
    assert 'speaks on stdout' in logs_of(speaks)
    assert 'speaks on stderr' in logs_of(speaks)
    # The end markers of both arrived: neither waited for them.
    for block_run in by_name(pipeline_run).values():
        assert seconds(block_run) < warm_workers.OUTPUT_SECONDS, seconds(block_run)


def test_a_block_that_closes_its_output_streams_completes_and_so_does_the_next(workers):
    pipeline = pipeline_of(workers, [
        ('closes', 'data_loader', 'plain', '''
            import os
            import sys

            @data_loader
            def load(*args, **kwargs):
                print('before closing')
                sys.stdout.close()
                sys.stderr.close()
                return {'pid': os.getpid()}
        '''),
        ('speaks', 'transformer', 'plain', '''
            import os

            @transformer
            def speak(data, *args, **kwargs):
                print('speaks after')
                return {'pid': os.getpid()}
        '''),
    ])
    pipeline_run = run_fused(pipeline)
    assert len(set(pids(pipeline, pipeline_run, ['closes', 'speaks']))) == 1
    assert 'before closing' in logs_of(by_name(pipeline_run)['closes'])
    assert 'speaks after' in logs_of(by_name(pipeline_run)['speaks'])


def test_a_block_that_adds_a_log_handler_logs_each_line_once(workers):
    # A block that adds a handler at each run, as code moved from a script does. In a new
    # process each block's line appears once.
    code = '''
        import logging
        import os
        import sys

        @{kind}
        def step(*args, **kwargs):
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter('handler: %(message)s'))
            logger = logging.getLogger('faults')
            logger.addHandler(handler)
            logger.warning('line from {name}')
            return {{'pid': os.getpid()}}
    '''
    names = ['first', 'second', 'third']
    pipeline = pipeline_of(workers, [
        (name, 'data_loader' if index == 0 else 'transformer', 'plain',
         code.format(kind='data_loader' if index == 0 else 'transformer', name=name))
        for index, name in enumerate(names)
    ])
    pipeline_run = run_fused(pipeline)
    assert len(set(pids(pipeline, pipeline_run, names))) == 1
    for name, block_run in by_name(pipeline_run).items():
        assert logs_of(block_run).count(f'handler: line from {name}') == 1, (name, block_run.logs)


def test_a_block_that_changes_directory_leaves_the_next_in_the_project(workers):
    pipeline = pipeline_of(workers, [
        ('moves', 'data_loader', 'plain', '''
            import os

            @data_loader
            def load(*args, **kwargs):
                os.chdir('/')
                return {'pid': os.getpid()}
        '''),
        ('where', 'transformer', 'plain', '''
            import os

            @transformer
            def where(data, *args, **kwargs):
                return {'pid': os.getpid(), 'cwd': os.getcwd()}
        '''),
    ])
    pipeline_run = run_fused(pipeline)
    assert len(set(pids(pipeline, pipeline_run, ['moves', 'where']))) == 1
    cwd = output(pipeline, pipeline_run, 'where')['cwd']
    assert os.path.realpath(cwd) == os.path.realpath(workers)


KILLER = '''
import os
import signal
import time

import psutil


@transformer
def kill(data, *args, **kwargs):
    # In MageML's Python, between two blocks of the worker: its warm process is idle.
    process = psutil.Process(data['pid'])
    target = process.parent() if {supervisor} else process
    os.kill(target.pid, signal.SIGKILL)
    deadline = time.monotonic() + 10
    while psutil.pid_exists(data['pid']) and time.monotonic() < deadline:
        if psutil.Process(data['pid']).status() == psutil.STATUS_ZOMBIE:
            break
        time.sleep(0.1)
    return data
'''


@pytest.mark.parametrize('supervisor', [False, True], ids=['worker', 'supervisor'])
def test_a_warm_process_killed_while_idle_is_replaced_for_the_next_block(workers, supervisor):
    # As the system's out-of-memory killer ends an idle process, or its supervisor.
    pipeline = pipeline_of(workers, [
        ('before', 'data_loader', 'plain', '''
            import os

            @data_loader
            def load(*args, **kwargs):
                return {'pid': os.getpid()}
        '''),
        ('kill', 'transformer', None, KILLER.format(supervisor=supervisor)),
        ('after', 'transformer', 'plain', '''
            import os

            @transformer
            def after(data, *args, **kwargs):
                return {'pid': os.getpid(), 'before': data['pid']}
        '''),
    ])
    pipeline_run = run_fused(pipeline)
    result = output(pipeline, pipeline_run, 'after')
    assert result['pid'] != result['before']
    assert gone(result['before'], 10), 'the killed worker is still running'


CHILD = '''
import sys

from mage_ai.data_preparation.models.pipeline import Pipeline

repo, pipeline_uuid, block_uuid = sys.argv[1:]
Pipeline.get(pipeline_uuid, repo_path=repo).get_block(block_uuid).execute_sync()
'''


@pytest.mark.parametrize('how', [signal.SIGKILL, signal.SIGTERM])
def test_a_killed_run_leaves_no_worker_process_behind(workers, tmp_path, how):
    # A block timeout kills the block run's job, and a crashed scheduler is gone without a
    # word: the worker running the block must stop with it.
    pid_file = tmp_path / 'pid'
    pipeline = pipeline_of(workers, [('hangs', 'data_loader', 'plain', f'''
        import os
        import time

        @data_loader
        def load(*args, **kwargs):
            with open({str(pid_file)!r}, 'w') as file:
                file.write(str(os.getpid()))
            time.sleep(600)
    ''')])
    [block_uuid] = list(pipeline.blocks_by_uuid)
    script = tmp_path / 'child.py'
    script.write_text(textwrap.dedent(CHILD))
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(
        p for p in (str(Path(__file__).resolve().parents[2]), os.environ.get('PYTHONPATH')) if p))
    child = subprocess.Popen([sys.executable, str(script), workers, pipeline.uuid, block_uuid],
                             cwd=workers, env=env, stdout=subprocess.DEVNULL,
                             stderr=subprocess.PIPE)
    try:
        deadline = time.monotonic() + 120
        while not pid_file.exists() or not pid_file.read_text():
            assert child.poll() is None, child.stderr.read().decode()[-3000:]
            assert time.monotonic() < deadline, 'the block did not start'
            time.sleep(0.2)
        pid = int(pid_file.read_text())
        assert psutil.pid_exists(pid)
        child.send_signal(how)
        child.wait(timeout=30)
        assert gone(pid, 15), f'the worker process {pid} outlived its run'
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()


def test_a_block_that_ends_its_process_fails_and_the_next_run_gets_a_new_one(workers):
    pipeline = pipeline_of(workers, [('ends', 'data_loader', 'plain', '''
        import sys

        @data_loader
        def load(*args, **kwargs):
            sys.exit(0)
    ''')])
    pipeline_run = run(pipeline)
    block_run = by_name(pipeline_run)['ends']
    assert getattr(block_run.status, 'value', block_run.status) == 'failed'
    assert 'SystemExit' in error_of(block_run), error_of(block_run)
    after = pipeline_of(workers, [('load', 'data_loader', 'plain', '''
        import os

        @data_loader
        def load(*args, **kwargs):
            return {'pid': os.getpid()}
    ''')])
    assert output(after, completed(run(after)), 'load')['pid']
