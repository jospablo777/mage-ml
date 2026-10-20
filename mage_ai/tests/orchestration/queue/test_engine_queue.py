"""
The scheduler's jobs on the Rust run engine (docs/development/run-engine.md): warm workers
run them, the queue tracks them as the process queue does, and a job cannot leave state
behind for the next one.
"""
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from mage_ai.orchestration.fusion import ProcessState
from mage_ai.orchestration.queue import engine_worker
from mage_ai.orchestration.queue.config import QueueConfig
from mage_ai.orchestration.queue.engine_queue import EngineQueue, engine_supports_jobs
from mage_ai.orchestration.queue.queue_factory import (
    QueueFactory,
    job_engine_mode,
    job_engine_requested,
)
from mage_ai.pipeline_services.engine import EngineNotFound, engine_path
from mage_ai.tests.base_test import TestCase


def touch(path):
    Path(path).write_text(str(os.getpid()))


def sleep_then_touch(seconds, path):
    time.sleep(seconds)
    touch(path)


def set_variable_and_cwd(directory):
    import warnings

    import numpy as np

    os.environ['MAGE_TEST_LEAKED'] = 'yes'
    os.chdir(directory)
    np.seterr(divide='raise')
    warnings.simplefilter('error')


def report_state(path):
    import warnings

    import numpy as np

    errors = any(f[0] == 'error' and f[2] is Warning for f in warnings.filters)
    Path(path).write_text(
        f'{os.environ.get("MAGE_TEST_LEAKED")}|{os.getcwd()}|{np.geterr()["divide"]}|{errors}',
    )


def register_alias_and_wait(alias, seconds):
    from mage_ai.orchestration.queue.process_queue import register_job_alias

    register_job_alias(alias)
    time.sleep(seconds)


def fail():
    raise ValueError('the block failed')


def _engine():
    try:
        path = engine_path()
    except EngineNotFound:
        return None
    return path if engine_supports_jobs(path) else None


ENGINE = _engine()


def wait_until(condition, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return False


class EngineWorkerTests(unittest.TestCase):
    def test_a_job_cannot_leave_variables_cwd_or_path_behind(self):
        state = ProcessState()
        before = os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            os.environ['MAGE_TEST_LEAKED'] = 'yes'
            os.chdir(directory)
            sys.path.append(directory)
            state.restore()
        self.assertNotIn('MAGE_TEST_LEAKED', os.environ)
        self.assertEqual(os.getcwd(), before)
        self.assertNotIn(directory, sys.path)

    def test_the_project_modules_are_dropped_and_mage_stays(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, 'helpers_for_engine_test.py').write_text('VALUE = 1\n')
            sys.path.insert(0, directory)
            try:
                import helpers_for_engine_test  # noqa: F401

                dropped = engine_worker.drop_project_modules([os.path.realpath(directory) + os.sep])
            finally:
                sys.path.remove(directory)
        self.assertEqual(dropped, ['helpers_for_engine_test'])
        self.assertNotIn('helpers_for_engine_test', sys.modules)
        self.assertIn('mage_ai.orchestration.queue.engine_worker', sys.modules)

    def test_an_alias_becomes_an_event_for_the_engine(self):
        sent = []

        class Replies:
            def send(self, message):
                sent.append(message)

        sink = engine_worker._AliasSink(Replies())
        sink['block_run_7'] = 1234
        sink['other'] = 'completed'
        self.assertEqual(sent, [dict(event='alias', alias='block_run_7')])

    def test_a_thread_left_running_is_found(self):
        import threading

        state = ProcessState()
        stop = threading.Event()
        thread = threading.Thread(target=stop.wait, name='left-running')
        thread.start()
        try:
            self.assertEqual([t.name for t in state.threads_left_running()], ['left-running'])
        finally:
            stop.set()
            thread.join()


class QueueFactoryTests(unittest.TestCase):
    def tearDown(self):
        QueueFactory.queue = None

    def test_the_engine_is_asked_for_with_mage_job_engine(self):
        with patch.dict(os.environ, MAGE_JOB_ENGINE='on'):
            self.assertTrue(job_engine_requested())
        with patch.dict(os.environ, MAGE_JOB_ENGINE='off'):
            self.assertFalse(job_engine_requested())

    def test_without_an_engine_jobs_run_in_processes(self):
        from mage_ai.orchestration.queue.process_queue import ProcessQueue

        QueueFactory.queue = None
        with patch.dict(os.environ, MAGE_JOB_ENGINE='on',
                        MAGE_ENGINE_PATH='/nonexistent/mage-service'):
            queue = QueueFactory.get_queue()
        self.assertIsInstance(queue, ProcessQueue)
        queue.mp_manager.shutdown()

    def fake_engine(self, directory, has_jobs=True):
        path = Path(directory, 'mage-service')
        path.write_text('#!/bin/sh\nexit 0\n' if has_jobs else '#!/bin/sh\nexit 2\n')
        path.chmod(0o755)
        return path

    def test_unset_is_automatic(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('MAGE_JOB_ENGINE', None)
            self.assertEqual(job_engine_mode(), 'auto')
        with patch.dict(os.environ, MAGE_JOB_ENGINE='Off'):
            self.assertEqual(job_engine_mode(), 'off')

    def test_an_installed_engine_runs_jobs_by_default(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = self.fake_engine(directory)
            env = dict(MAGE_ENGINE_PATH=str(engine))
            with patch.dict(os.environ, env):
                os.environ.pop('MAGE_JOB_ENGINE', None)
                queue = QueueFactory.get_queue()
            self.assertIsInstance(queue, EngineQueue)

    def test_off_runs_jobs_in_processes_with_an_engine_installed(self):
        from mage_ai.orchestration.queue.process_queue import ProcessQueue

        with tempfile.TemporaryDirectory() as directory:
            engine = self.fake_engine(directory)
            with patch.dict(os.environ, MAGE_ENGINE_PATH=str(engine), MAGE_JOB_ENGINE='off'):
                queue = QueueFactory.get_queue()
        self.assertIsInstance(queue, ProcessQueue)
        queue.mp_manager.shutdown()

    def test_a_source_checkouts_build_is_not_used_by_default(self):
        from mage_ai.orchestration.queue.process_queue import ProcessQueue

        with tempfile.TemporaryDirectory() as directory:
            built = self.fake_engine(directory)
            with patch.dict(os.environ, {}), \
                    patch('mage_ai.pipeline_services.engine.installed_engine_path',
                          return_value=None), \
                    patch('mage_ai.pipeline_services.engine.engine_path', return_value=built):
                os.environ.pop('MAGE_JOB_ENGINE', None)
                os.environ.pop('MAGE_ENGINE_PATH', None)
                queue = QueueFactory.get_queue()
        self.assertIsInstance(queue, ProcessQueue)
        queue.mp_manager.shutdown()

    def test_an_engine_without_jobs_is_not_used(self):
        with tempfile.TemporaryDirectory() as directory:
            old = Path(directory, 'mage-service')
            old.write_text('#!/bin/sh\necho "unrecognized subcommand jobs" >&2\nexit 2\n')
            old.chmod(0o755)
            self.assertFalse(engine_supports_jobs(old))


@unittest.skipIf(ENGINE is None, 'the engine with the jobs command is not built')
class EngineQueueTests(TestCase):
    def setUp(self):
        super().setUp()
        self.directory = tempfile.mkdtemp()
        self.queue = EngineQueue(QueueConfig.load(config=dict(concurrency=3)), ENGINE)
        self.queue.start()

    def tearDown(self):
        self.queue.stop()
        super().tearDown()

    def path(self, name):
        return os.path.join(self.directory, name)

    def test_a_job_runs_in_a_warm_worker_and_finishes(self):
        self.queue.enqueue('block_run_1', touch, self.path('one'))
        self.assertTrue(wait_until(lambda: os.path.exists(self.path('one'))))
        self.assertTrue(wait_until(lambda: not self.queue.has_job('block_run_1')))
        self.assertTrue(self.queue.jobs_finished())
        self.assertFalse(self.queue.jobs_finished(), 'reported once')
        # The next jobs run in warm workers, not in a process each. Which warm worker takes
        # a job depends on when each finished starting, so only the count is fixed: at most
        # the queue's concurrency.
        pids = {Path(self.path('one')).read_text()}
        for index in range(2, 6):
            name = f'block_run_{index}'
            self.queue.enqueue(name, touch, self.path(name))
            self.assertTrue(wait_until(lambda name=name: os.path.exists(self.path(name))))
            self.assertTrue(wait_until(lambda name=name: not self.queue.has_job(name)))
            pids.add(Path(self.path(name)).read_text())
        self.assertLessEqual(len(pids), 3, pids)

    def test_a_running_job_is_present_and_a_killed_one_is_not(self):
        self.queue.enqueue('block_run_3', sleep_then_touch, 30, self.path('never'))
        self.assertTrue(wait_until(lambda: self.queue.running_jobs() == ['block_run_3']))
        self.assertTrue(self.queue.has_job('block_run_3'))
        self.queue.kill_job('block_run_3')
        self.assertFalse(self.queue.has_job('block_run_3'))
        self.assertTrue(wait_until(self.queue.jobs_finished))
        self.assertFalse(os.path.exists(self.path('never')))
        # The engine replaces the killed worker; the next job runs.
        self.queue.enqueue('block_run_4', touch, self.path('after'))
        self.assertTrue(wait_until(lambda: os.path.exists(self.path('after'))))

    def test_a_block_run_a_stage_runs_is_present_while_the_stage_runs(self):
        self.queue.enqueue('block_run_5', register_alias_and_wait, 'block_run_6', 2)
        self.assertTrue(wait_until(lambda: self.queue.has_job('block_run_6')))
        self.assertTrue(wait_until(lambda: not self.queue.has_job('block_run_6')))
        self.assertFalse(self.queue.has_job('block_run_5'))

    def test_killing_a_block_run_of_a_stage_kills_the_stage(self):
        # A timeout or a cancel names the block run the stage runs now, not the stage.
        self.queue.enqueue('block_run_13', register_alias_and_wait, 'block_run_14', 30)
        self.assertTrue(wait_until(lambda: self.queue.has_job('block_run_14')))
        self.queue.kill_job('block_run_14')
        self.assertFalse(self.queue.has_job('block_run_14'))
        self.assertFalse(self.queue.has_job('block_run_13'))
        # The engine reports the stage's job ended: its process was killed.
        self.assertTrue(wait_until(self.queue.jobs_finished, seconds=10))

    def test_a_job_does_not_see_what_the_one_before_changed(self):
        self.queue.enqueue('block_run_7', set_variable_and_cwd, self.directory)
        self.assertTrue(wait_until(lambda: not self.queue.has_job('block_run_7')))
        self.queue.enqueue('block_run_8', report_state, self.path('state'))
        self.assertTrue(wait_until(lambda: os.path.exists(self.path('state'))))
        leaked, cwd, divide, warnings_raise = Path(self.path('state')).read_text().split('|')
        self.assertEqual(leaked, 'None')
        self.assertNotEqual(os.path.realpath(cwd), os.path.realpath(self.directory))
        self.assertNotEqual(divide, 'raise')
        self.assertEqual(warnings_raise, 'False')

    def test_a_failed_job_ends_and_the_worker_runs_the_next(self):
        self.queue.enqueue('block_run_9', fail)
        self.assertTrue(wait_until(lambda: not self.queue.has_job('block_run_9')))
        self.queue.enqueue('block_run_10', touch, self.path('next'))
        self.assertTrue(wait_until(lambda: os.path.exists(self.path('next'))))

    def test_the_engine_and_its_workers_end_with_a_killed_scheduler(self):
        import signal
        import subprocess

        import psutil

        script = (
            'import sys, time\n'
            'from pathlib import Path\n'
            'from mage_ai.orchestration.queue.config import QueueConfig\n'
            'from mage_ai.orchestration.queue.engine_queue import EngineQueue\n'
            'from mage_ai.tests.orchestration.queue import test_engine_queue as t\n'
            'q = EngineQueue(QueueConfig.load(config=dict(concurrency=2)), Path(sys.argv[1]))\n'
            'q.start()\n'
            'q.enqueue("block_run_99", t.sleep_then_touch, 60, sys.argv[2])\n'
            'while not q.running_jobs():\n'
            '    time.sleep(0.05)\n'
            'print("PIDS", q.process.pid, q.jobs["block_run_99"], flush=True)\n'
            'time.sleep(120)\n'
        )
        scheduler = subprocess.Popen(
            [sys.executable, '-c', script, str(ENGINE), self.path('never')],
            stdout=subprocess.PIPE, text=True,
        )
        line = ''
        while not line.startswith('PIDS'):
            line = scheduler.stdout.readline()
            self.assertTrue(line, 'the scheduler process ended early')
        engine_pid, worker_pid = (int(p) for p in line.split()[1:])
        scheduler.send_signal(signal.SIGKILL)
        scheduler.wait()

        def gone(pid):
            try:
                return psutil.Process(pid).status() == psutil.STATUS_ZOMBIE
            except psutil.NoSuchProcess:
                return True

        self.assertTrue(wait_until(lambda: gone(engine_pid) and gone(worker_pid), 15))
        self.assertFalse(os.path.exists(self.path('never')))

    def test_workers_of_a_burst_end_after_idling_and_the_warm_one_stays(self):
        import psutil

        from mage_ai.orchestration.queue import engine_queue

        self.queue.stop()
        with patch.object(engine_queue, 'WARM_WORKERS', 1), \
                patch.dict(os.environ, MAGE_JOBS_IDLE_SECONDS='1'):
            self.queue = EngineQueue(QueueConfig.load(config=dict(concurrency=3)), ENGINE)
            self.queue.start()
        for n in range(3):
            self.queue.enqueue(f'block_run_{20 + n}', sleep_then_touch, 2, self.path(f'b{n}'))
        self.assertTrue(wait_until(lambda: len(self.queue.running_jobs()) == 3))
        workers = lambda: len(psutil.Process(self.queue.process.pid).children())  # noqa: E731
        self.assertEqual(workers(), 3)
        self.assertTrue(wait_until(lambda: all(
            os.path.exists(self.path(f'b{n}')) for n in range(3))))
        self.assertTrue(wait_until(lambda: workers() == 1, seconds=20))

    def test_jobs_of_an_engine_that_died_are_gone(self):
        import psutil

        self.queue.enqueue('block_run_11', sleep_then_touch, 30, self.path('never'))
        self.assertTrue(wait_until(lambda: self.queue.running_jobs() == ['block_run_11']))
        worker = self.queue.jobs['block_run_11']
        self.queue.process.kill()
        self.assertTrue(wait_until(lambda: not self.queue.has_job('block_run_11')))
        # Its worker ends too, so the block run does not run twice at once.
        self.assertTrue(wait_until(lambda: not psutil.pid_exists(worker)
                                   or psutil.Process(worker).status() == psutil.STATUS_ZOMBIE,
                                   15))
        # The next job starts a new engine.
        self.queue.enqueue('block_run_12', touch, self.path('again'))
        self.assertTrue(wait_until(lambda: os.path.exists(self.path('again'))))
