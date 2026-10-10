import multiprocessing
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest

import psutil

from mage_ai.orchestration.queue.process_queue import _kill_process_tree
from mage_ai.shared.processes import exit_with_parent, stop_on_terminate

SLEEPER = [sys.executable, '-c', 'import time; time.sleep(120)']


def _wait_gone(pids, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        alive = [pid for pid in pids if _alive(pid)]
        if not alive:
            return []
        time.sleep(0.2)
    return [pid for pid in pids if _alive(pid)]


def _alive(pid):
    try:
        return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def _write(path, text):
    with open(path, 'w') as file:
        file.write(text)


def _read_pids(path, count, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.exists(path):
            with open(path) as file:
                parts = file.read().split()
            if len(parts) == count:
                return [int(part) for part in parts]
        time.sleep(0.1)
    raise AssertionError(f'{path} was not written')


def _watched_child(directory):
    exit_with_parent(lambda: _write(os.path.join(directory, 'cleaned'), 'yes'), interval=0.2)
    grandchild = subprocess.Popen(SLEEPER)
    _write(os.path.join(directory, 'child.tmp'), f'{os.getpid()} {grandchild.pid}')
    os.replace(os.path.join(directory, 'child.tmp'), os.path.join(directory, 'child'))
    time.sleep(120)


def _parent_of_watched_child(directory):
    child = multiprocessing.get_context('spawn').Process(target=_watched_child, args=(directory,))
    child.start()
    child.join()


def _terminated_child(directory):
    stop_on_terminate(lambda: _write(os.path.join(directory, 'cleaned'), 'yes'))
    grandchild = subprocess.Popen(SLEEPER)
    _write(os.path.join(directory, 'child.tmp'), f'{os.getpid()} {grandchild.pid}')
    os.replace(os.path.join(directory, 'child.tmp'), os.path.join(directory, 'child'))
    time.sleep(120)


class ProcessLifetimeTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.context = multiprocessing.get_context('spawn')

    def test_child_exits_with_its_parent(self):
        parent = self.context.Process(target=_parent_of_watched_child, args=(self.directory,))
        parent.start()
        child, grandchild = _read_pids(os.path.join(self.directory, 'child'), 2)
        self.assertTrue(_alive(child))

        # SIGKILL: the parent runs no cleanup of its own.
        parent.kill()
        parent.join()

        self.assertEqual(_wait_gone([child, grandchild]), [])
        self.assertTrue(os.path.exists(os.path.join(self.directory, 'cleaned')))

    def test_child_stays_while_its_parent_lives(self):
        parent = self.context.Process(target=_parent_of_watched_child, args=(self.directory,))
        parent.start()
        child, grandchild = _read_pids(os.path.join(self.directory, 'child'), 2)
        time.sleep(1)
        try:
            self.assertTrue(_alive(child))
            self.assertTrue(_alive(grandchild))
        finally:
            _kill_process_tree(parent.pid)
            parent.join()
        self.assertEqual(_wait_gone([child, grandchild]), [])

    @unittest.skipIf(sys.platform == 'win32', 'SIGTERM is POSIX')
    def test_sigterm_runs_cleanup_and_stops_children(self):
        child = self.context.Process(target=_terminated_child, args=(self.directory,))
        child.start()
        child_pid, grandchild = _read_pids(os.path.join(self.directory, 'child'), 2)

        os.kill(child_pid, signal.SIGTERM)
        child.join(15)

        self.assertEqual(child.exitcode, 128 + signal.SIGTERM)
        self.assertEqual(_wait_gone([grandchild]), [])
        self.assertTrue(os.path.exists(os.path.join(self.directory, 'cleaned')))

    def test_kill_process_tree(self):
        script = (
            'import subprocess, sys, time; '
            f'p = subprocess.Popen({SLEEPER!r}); '
            'print(p.pid, flush=True); time.sleep(120)'
        )
        process = subprocess.Popen(
            [sys.executable, '-c', script], stdout=subprocess.PIPE, text=True,
        )
        grandchild = int(process.stdout.readline())

        _kill_process_tree(process.pid)
        process.wait(15)

        self.assertEqual(_wait_gone([grandchild]), [])
        # A process that is gone already is not an error.
        _kill_process_tree(process.pid)
