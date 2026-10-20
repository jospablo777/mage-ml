"""
`mage service run`: a service on this machine's engine, without Docker, with the
environment's values, a private token and no change to committed files.
"""
import json
import os
import shutil
import stat
import subprocess
import tempfile
import textwrap
import time
import unittest
import urllib.request
from pathlib import Path

from mage_ai.pipeline_services import engine, lifecycle, local_run
from mage_ai.tests.base_test import TestCase
from mage_ai.tests.pipeline_services.test_export import SERVICE_BINARY, ExportTestCase


class EnvFileTest(TestCase):
    def test_key_values_comments_quotes_and_export_are_read(self):
        path = Path(self.repo_path) / '.env.dev'
        path.write_text('# secrets\nAPI_TOKEN="s3cr3t"\nexport REGION=eu\n\nEMPTY=\n')

        self.assertEqual(
            local_run.read_env_file(path), dict(API_TOKEN='s3cr3t', REGION='eu', EMPTY=''),
        )

    def test_a_line_without_equals_names_its_line(self):
        path = Path(self.repo_path) / '.env.broken'
        path.write_text('GOOD=1\nnot a setting\n')

        with self.assertRaisesRegex(local_run.LocalRunError, 'line 2'):
            local_run.read_env_file(path)


class EngineLookupTest(TestCase):
    def test_an_unusable_engine_path_and_no_engine_say_what_to_do(self):
        from unittest.mock import patch

        with patch.dict(os.environ, MAGE_ENGINE_PATH='/nonexistent/mage-service'):
            with self.assertRaisesRegex(engine.EngineNotFound, 'not an executable'):
                engine.engine_path()
        with patch.dict(os.environ, {}, clear=False), \
                patch.object(engine, '_candidates', return_value=iter([])):
            os.environ.pop('MAGE_ENGINE_PATH', None)
            with self.assertRaisesRegex(
                engine.EngineNotFound, r'uv pip install "mage-ml\[services\]"',
            ):
                engine.engine_path()


    def test_the_newer_of_the_source_builds_is_used(self):
        from unittest.mock import patch

        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        for profile, mtime in (('release', 1_000), ('debug', 2_000)):
            binary = root / profile / engine.ENGINE_NAME
            binary.parent.mkdir()
            binary.write_text('#!/bin/sh\n')
            binary.chmod(0o755)
            os.utime(binary, (mtime, mtime))

        with patch.object(engine, 'SOURCE_ENGINE', root), \
                patch.object(engine.shutil, 'which', return_value=None), \
                patch.object(engine.sys, 'executable', str(root / 'none' / 'python')):
            found = [p for p in engine._candidates() if p is not None]

        self.assertEqual(found[-2:], [root / 'debug' / engine.ENGINE_NAME,
                                      root / 'release' / engine.ENGINE_NAME])

    def test_an_engine_that_cannot_read_the_service_fails_before_serving(self):
        """A stale engine started, printed a schema error and left a dead server."""
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        stale = root / engine.ENGINE_NAME
        stale.write_text(textwrap.dedent('''\
            #!/bin/sh
            if [ "$1" = "--version" ]; then echo "mage-service 0.0.9"; exit 0; fi
            echo "error service.json has schema version 2, newer than this engine reads" >&2
            exit 1
        '''))
        stale.chmod(0o755)

        with self.assertRaises(local_run.LocalRunError) as raised:
            local_run._validate(stale, dict(os.environ))

        message = str(raised.exception)
        self.assertIn('mage-service 0.0.9', message)
        self.assertIn('newer than this engine reads', message)
        self.assertIn('uv pip install -U mageml-engine', message)


class RunCommandTest(TestCase):
    def test_without_an_engine_the_command_says_how_to_get_one(self):
        from unittest.mock import patch

        from typer.testing import CliRunner

        from mage_ai.cli.main import app

        with patch.object(local_run, 'prepare',
                          side_effect=engine.EngineNotFound('install mageml-engine')):
            result = CliRunner().invoke(app, ['service', 'run', 'sales',
                                              '--port', str(_free_port()),
                                              '--project', self.repo_path])

        self.assertEqual(result.exit_code, 1)
        self.assertIn('install mageml-engine', result.output)

    def test_a_port_in_use_is_named_before_anything_is_built(self):
        import socket
        from unittest.mock import patch

        from typer.testing import CliRunner

        from mage_ai.cli.main import app

        with socket.socket() as taken, patch.object(local_run, 'prepare') as prepare:
            taken.bind(('127.0.0.1', 0))
            taken.listen()
            port = taken.getsockname()[1]
            result = CliRunner().invoke(app, ['service', 'run', 'sales', '--port', str(port),
                                              '--project', self.repo_path])

        self.assertEqual(result.exit_code, 1)
        self.assertIn(f'Port {port} is in use', result.output)
        prepare.assert_not_called()


@unittest.skipUnless(SERVICE_BINARY.is_file(), 'needs the mage-service binary (cargo build)')
class LocalRunTest(ExportTestCase):
    def setUp(self):
        super().setUp()
        root = Path(self.repo_path)
        for path in (root / 'services', root / '.mage'):
            self.addCleanup(shutil.rmtree, path, True)
        self.chain()
        lifecycle.new(self.repo_path, 'sales', [self.pipeline.uuid])

    def test_a_service_runs_locally_with_its_environment_and_a_private_token(self):
        folder = Path(self.repo_path) / 'services' / 'sales'
        (folder / '.env.dev').write_text('SALES_REGION=eu\n')
        lock = lifecycle.lock_path(self.repo_path, 'sales')

        run = local_run.prepare(
            self.repo_path, 'sales', port=_free_port(), engine_binary=SERVICE_BINARY,
        )

        self.assertFalse(lock.exists())
        self.assertEqual(run.env['SALES_REGION'], 'eu')
        self.assertEqual(stat.S_IMODE(os.stat(run.token_file).st_mode), 0o600)
        token = run.token_file.read_text()
        process = subprocess.Popen(run.command, env=run.env,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        try:
            answer = _post_run(run.port, token, self.pipeline.uuid)
            listed = _cli(self.repo_path, 'runs', 'sales')
            log = _cli(self.repo_path, 'logs', 'sales', '--run', answer['id'])
            latest = _cli(self.repo_path, 'logs', 'sales')
            called = _cli(self.repo_path, 'call', 'sales')
        finally:
            process.terminate()
            process.wait(timeout=60)

        # The run's fields are at the top of the answer (RunDetail flattens them).
        self.assertEqual(answer['status'], 'completed', answer)
        self.assertEqual({b['status'] for b in answer['blocks']}, {'completed'})
        # mage service runs and logs find the local run's port and token.
        self.assertIn(answer['id'], listed)
        self.assertIn('completed', listed)
        self.assertEqual(log, latest)
        self.assertIn('completed.', called)
        self.assertIn('Output of', called)
        # A second local run keeps the token and the run history.
        again = local_run.prepare(self.repo_path, 'sales', engine_binary=SERVICE_BINARY)
        self.assertEqual(again.token_file.read_text(), token)
        self.assertTrue(any(again.data.iterdir()))

    def test_another_environment_must_exist(self):
        with self.assertRaisesRegex(local_run.LocalRunError, 'no environment prod'):
            local_run.prepare(self.repo_path, 'sales', environment='prod',
                              engine_binary=SERVICE_BINARY)


def _cli(project: str, *arguments: str) -> str:
    from typer.testing import CliRunner

    from mage_ai.cli.main import app

    result = CliRunner().invoke(app, ['service', *arguments, '--project', project])
    assert result.exit_code == 0, result.output
    return result.output


def _free_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def _post_run(port: int, token: str, pipeline: str) -> dict:
    deadline = time.time() + 60
    while True:
        try:
            request = urllib.request.Request(
                f'http://127.0.0.1:{port}/v1/pipelines/{pipeline}/runs?wait=120',
                data=b'{}', method='POST',
                headers={'Authorization': f'Bearer {token}',
                         'Content-Type': 'application/json'},
            )
            with urllib.request.urlopen(request, timeout=150) as response:
                return json.loads(response.read())
        except OSError:
            if time.time() > deadline:
                raise
            time.sleep(0.5)


class SuperviseTest(TestCase):
    def test_sigterm_to_the_command_stops_the_engine(self):
        """SIGTERM to mage service run left the engine serving, port and run history held."""
        import signal
        import sys

        folder = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, folder, True)
        pid_file = folder / 'engine.pid'
        engine_script = folder / 'engine.py'
        engine_script.write_text(textwrap.dedent(f'''
            import os, signal, sys, time
            open({str(pid_file)!r}, 'w').write(str(os.getpid()))
            signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))
            time.sleep(120)
        '''))
        parent = subprocess.Popen([sys.executable, '-c', textwrap.dedent(f'''
            import sys
            from mage_ai.pipeline_services import local_run
            sys.exit(local_run.supervise([sys.executable, {str(engine_script)!r}], None))
        ''')])
        deadline = time.time() + 30
        while not (pid_file.is_file() and pid_file.read_text()) and time.time() < deadline:
            time.sleep(0.1)
        engine_pid = int(pid_file.read_text())

        parent.send_signal(signal.SIGTERM)

        self.assertEqual(parent.wait(timeout=30), 0)
        with self.assertRaises(ProcessLookupError):
            for _ in range(50):
                os.kill(engine_pid, 0)
                time.sleep(0.1)


class ReloadTest(TestCase):
    def test_a_change_starts_the_service_again_and_a_failed_build_waits(self):
        import sys
        import threading

        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        (root / 'blocks').mkdir()
        block = root / 'blocks' / 'load.py'
        block.write_text('x = 1\n')
        (root / '.git').mkdir()
        pids = root / 'pids'
        engine_script = root / 'engine.py'
        engine_script.write_text(textwrap.dedent(f'''
            import os, time
            with open({str(pids)!r}, 'a') as f:
                f.write(str(os.getpid()) + '\\n')
            time.sleep(120)
        '''))
        starts = []

        def start():
            starts.append(block.read_text())
            if 'broken' in block.read_text():
                raise local_run.LocalRunError('SyntaxError in blocks/load.py')
            return local_run.LocalRun(
                name='s', box=root, data=root, token_file=root / 't', port=0,
                command=[sys.executable, str(engine_script)], env=dict(os.environ),
                inputs=['blocks'],
            )

        said = []
        stop = threading.Event()
        loop = threading.Thread(target=local_run.run_with_reload,
                                args=(str(root), start, said.append),
                                kwargs=dict(interval=0.05, stop=stop))
        loop.start()

        def wait_for(condition):
            deadline = time.time() + 20
            while not condition() and time.time() < deadline:
                time.sleep(0.05)
            self.assertTrue(condition(), said)

        def engines():
            return pids.read_text().split() if pids.is_file() else []

        try:
            wait_for(lambda: len(engines()) == 1)
            (root / '.git' / 'index').write_text('ignored')
            time.sleep(0.3)
            self.assertEqual(len(starts), 1)

            block.write_text('broken = (\n')
            wait_for(lambda: any('does not build' in s for s in said))
            block.write_text('x = 2\n')
            wait_for(lambda: len(engines()) == 2)
        finally:
            stop.set()
            loop.join(timeout=30)

        self.assertFalse(loop.is_alive())
        self.assertIn('Changed: blocks/load.py. Starting the service again.', said)
        for pid in engines():
            with self.assertRaises(ProcessLookupError):
                for _ in range(50):
                    os.kill(int(pid), 0)
                    time.sleep(0.1)
