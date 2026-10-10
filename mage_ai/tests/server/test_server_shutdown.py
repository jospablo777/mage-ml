"""
A stopped web server leaves none of its processes behind.

A scheduler that outlived its server kept running pipelines next to the scheduler of the
next server, so block runs ran twice and failed writing their outputs.
"""
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

import psutil

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def _descendants(pid):
    try:
        return psutil.Process(pid).children(recursive=True)
    except psutil.NoSuchProcess:
        return []


def _alive(process: psutil.Process) -> bool:
    try:
        return process.is_running() and process.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


@unittest.skipIf(sys.platform == 'win32', 'SIGTERM is POSIX')
class ServerShutdownTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.port = _free_port()
        env = dict(
            os.environ,
            DISABLE_AUTO_BROWSER_OPEN='1',
            DISABLE_USAGE_STATISTICS='1',
            MAGE_DATA_DIR=os.path.join(self.directory, 'data'),
            PYTHONPATH=ROOT,
        )
        env.pop('REQUIRE_USER_AUTHENTICATION', None)
        self.server = subprocess.Popen(
            [
                sys.executable,
                os.path.join(ROOT, 'mage_ai', 'cli', 'main.py'),
                'start',
                os.path.join(self.directory, 'project'),
                '--host',
                '127.0.0.1',
                '--port',
                str(self.port),
            ],
            cwd=self.directory,
            env=env,
            stderr=subprocess.STDOUT,
            stdout=subprocess.DEVNULL,
        )

    def tearDown(self):
        for process in _descendants(self.server.pid):
            try:
                process.kill()
            except psutil.NoSuchProcess:
                pass
        if self.server.poll() is None:
            self.server.kill()
        self.server.wait(10)
        shutil.rmtree(self.directory, ignore_errors=True)

    def _wait_until_serving(self):
        deadline = time.monotonic() + 180
        url = f'http://127.0.0.1:{self.port}/api/status'
        while time.monotonic() < deadline:
            self.assertIsNone(self.server.poll(), 'The server exited while starting.')
            try:
                with urllib.request.urlopen(url, timeout=2):
                    pass
            except OSError:
                time.sleep(1)
                continue
            # The scheduler starts its job queue a moment after the server answers.
            deadline_children = time.monotonic() + 60
            while time.monotonic() < deadline_children and len(
                _descendants(self.server.pid)
            ) < 2:
                time.sleep(0.5)
            return _descendants(self.server.pid)
        self.fail('The server did not start.')

    def _assert_all_gone(self, processes, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            alive = [process for process in processes if _alive(process)]
            if not alive:
                return
            time.sleep(0.5)
        self.fail(
            'Processes left behind: '
            + ', '.join(f'{p.pid} {" ".join(p.cmdline())[:120]}' for p in alive)
        )

    def test_sigterm_stops_the_scheduler_and_its_processes(self):
        children = self._wait_until_serving()
        self.assertTrue(children)

        self.server.send_signal(signal.SIGTERM)
        self.server.wait(60)

        self._assert_all_gone(children)

    def test_killed_server_leaves_no_processes(self):
        children = self._wait_until_serving()
        self.assertTrue(children)

        self.server.kill()
        self.server.wait(10)

        self._assert_all_gone(children)


# SIGTERM arrives while a finalizer runs; the server must still stop.
SIGNAL_IN_FINALIZER = """
import asyncio, gc, os, signal, sys, time
from mage_ai.server import server

class Collected:
    def __del__(self):
        os.kill(os.getpid(), signal.SIGTERM)
        time.sleep(0.2)

async def main():
    server.stop_main_task_on_terminate()
    item = Collected()
    item.cycle = item
    del item
    gc.collect()
    await asyncio.sleep(20)

signal.signal(signal.SIGTERM, server._exit_on_terminate)
try:
    asyncio.run(main())
except asyncio.CancelledError:
    print('stopped' if server._terminated else 'cancelled')
"""


@unittest.skipIf(sys.platform == 'win32', 'SIGTERM is POSIX')
class TerminateInFinalizerTest(unittest.TestCase):
    def test_sigterm_during_a_finalizer_stops_the_event_loop(self):
        """
        The handler raised SystemExit; Python ignores exceptions raised in __del__, and a
        server stopped while a zmq context was collected kept running.
        """
        started = time.monotonic()
        result = subprocess.run(
            [sys.executable, '-c', SIGNAL_IN_FINALIZER],
            capture_output=True, cwd=ROOT, env=dict(os.environ, PYTHONPATH=ROOT),
            text=True, timeout=60,
        )
        self.assertEqual(result.stdout.strip(), 'stopped', result.stderr[-2000:])
        self.assertLess(time.monotonic() - started, 15)
