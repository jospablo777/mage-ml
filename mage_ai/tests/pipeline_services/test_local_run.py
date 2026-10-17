"""
`mage service run`: a service on this machine's engine, without Docker, with the
environment's values, a private token and no change to committed files.
"""
import json
import os
import shutil
import stat
import subprocess
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
            with self.assertRaisesRegex(engine.EngineNotFound, 'uv pip install mageml-engine'):
                engine.engine_path()


class RunCommandTest(TestCase):
    def test_without_an_engine_the_command_says_how_to_get_one(self):
        from unittest.mock import patch

        from typer.testing import CliRunner

        from mage_ai.cli.main import app

        with patch.object(local_run, 'prepare',
                          side_effect=engine.EngineNotFound('install mageml-engine')):
            result = CliRunner().invoke(app, ['service', 'run', 'sales',
                                              '--project', self.repo_path])

        self.assertEqual(result.exit_code, 1)
        self.assertIn('install mageml-engine', result.output)


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
