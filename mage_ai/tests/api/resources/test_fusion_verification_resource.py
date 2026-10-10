"""
Verify fusion from the pipeline settings: the API starts the verifier in its own process,
reports its log and result, refuses a second run and stops it on request. The verifier is
replaced by a short script with the same command-line contract.
"""
import asyncio
import json
import sys
import textwrap
from unittest.mock import patch

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.api.resources import FusionVerificationResource as resource_module
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user

FAKE_VERIFIER = textwrap.dedent('''
    import json, sys, time
    report, passed, seconds = sys.argv[1], sys.argv[2] == 'pass', float(sys.argv[3])
    print('Verify fusion: block by block: pipeline run 1.', flush=True)
    time.sleep(seconds)
    print('Verify fusion: fused: pipeline run 2.', flush=True)
    json.dump({'passed': passed, 'blocks': [{'block_uuid': 'load', 'result': 'same'}],
               'first_difference': None}, open(report, 'w'))
''')


def fake_command(outcome: str, seconds: float = 0.0):
    def command(repo_path, pipeline_uuid, report_path, variables, exact):
        return [sys.executable, '-c', FAKE_VERIFIER, report_path, outcome, str(seconds)]
    return command


class FusionVerificationResourceTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        resource_module._verifications.clear()
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        self.owner = create_user(_owner=True)

    async def start(self, user=None, **payload):
        return await BaseOperation(
            action=OperationType.CREATE,
            payload=dict(fusion_verification=dict(pipeline_uuid=self.pipeline.uuid, **payload)),
            resource='fusion_verifications',
            user=user or self.owner,
        ).execute()

    async def status(self):
        return (await BaseOperation(
            action=OperationType.DETAIL,
            pk=self.pipeline.uuid,
            resource='fusion_verifications',
            user=self.owner,
        ).execute())['fusion_verification']

    async def wait(self, timeout: float = 30.0):
        for _ in range(int(timeout * 10)):
            current = await self.status()
            if current['status'] != 'running':
                return current
            await asyncio.sleep(0.1)
        self.fail('The verification did not end.')

    async def test_a_verification_reports_its_log_and_result(self):
        self.assertEqual((await self.status())['status'], 'none')
        with patch.object(resource_module, 'verification_command', fake_command('pass')):
            started = (await self.start())['fusion_verification']
        self.assertEqual(started['status'], 'running')
        done = await self.wait()
        self.assertEqual(done['status'], 'passed')
        self.assertIn('fused: pipeline run 2.', done['log'])
        self.assertEqual(done['report']['blocks'][0]['block_uuid'], 'load')
        self.assertIsNotNone(done['finished_at'])

        with patch.object(resource_module, 'verification_command', fake_command('fail')):
            await self.start()
        self.assertEqual((await self.wait())['status'], 'failed')

    async def test_a_running_verification_is_not_started_twice_and_can_be_stopped(self):
        with patch.object(resource_module, 'verification_command', fake_command('pass', 30)):
            await self.start()
            second = await self.start()
        self.assertEqual(second['error']['code'], 409)
        await BaseOperation(
            action=OperationType.DELETE,
            pk=self.pipeline.uuid,
            resource='fusion_verifications',
            user=self.owner,
        ).execute()
        stopped = await self.wait(timeout=10)
        self.assertEqual(stopped['status'], 'stopped')
        self.assertIsNone(stopped['report'])

    async def test_viewers_cannot_start_and_unknown_pipelines_are_refused(self):
        denied = await self.start(user=create_user())
        self.assertEqual(denied['error']['code'], 403)
        missing = await BaseOperation(
            action=OperationType.CREATE,
            payload=dict(fusion_verification=dict(pipeline_uuid='nope')),
            resource='fusion_verifications',
            user=self.owner,
        ).execute()
        self.assertEqual(missing['error']['code'], 404)

    def test_the_command_runs_the_cli_with_the_report_variables_and_exact(self):
        command = resource_module.verification_command(
            '/repo', 'sales', '/tmp/r.json', {'day': '2026-10-10'}, True,
        )
        self.assertEqual(command[1:5], ['-m', 'mage_ai.cli.main', 'verify-fusion', '/repo'])
        self.assertIn('--yes', command)
        self.assertEqual(json.loads(command[command.index('--runtime-vars') + 1]),
                         {'day': '2026-10-10'})
        self.assertEqual(command[-1], '--exact')
