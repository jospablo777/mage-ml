"""
Run records through the API: what a run ran with, a comparison with another run, and a
reproduction started in its own process (replaced here by a short script with the same
command-line contract).
"""
import asyncio
import sys
import textwrap
from pathlib import Path
from unittest.mock import patch

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.api.resources import RunRecordResource as resource_module
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration.fusion_verify import run_in_process
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user

LOAD = '''
import pandas as pd


@data_loader
def load(*args, **kwargs):
    return pd.DataFrame({'id': range(int(kwargs.get('rows', 2)))})
'''

FAKE_REPRODUCE = textwrap.dedent('''
    import json, sys
    print('Reproducing pipeline run.', flush=True)
    json.dump({'passed': True, 'blocks': []}, open(sys.argv[1], 'w'))
''')


class RunRecordResourceTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        resource_module._reproductions.clear()
        self.owner = create_user(_owner=True)
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        block = Block.create(
            f'{self.pipeline.uuid}_load', 'data_loader', self.repo_path, pipeline=self.pipeline,
        )
        Path(block.file_path).write_text(textwrap.dedent(LOAD))
        self.block_uuid = block.uuid

    def run_pipeline(self, **variables):
        run = trigger_pipeline(self.pipeline.uuid, variables=variables)
        return run_in_process(run, log=lambda _line: None)

    async def detail(self, run_id, **query):
        return (await BaseOperation(
            action=OperationType.DETAIL, pk=run_id, query=query, resource='run_records',
            user=self.owner,
        ).execute())['run_record']

    async def test_a_run_record_shows_and_compares(self):
        first = self.run_pipeline(rows=2)
        second = self.run_pipeline(rows=3)

        record = await self.detail(first.id)
        self.assertEqual(record['captures'], 1)
        self.assertGreater(record['code']['files'], 0)
        self.assertGreater(record['environment']['packages'], 0)
        self.assertEqual(record['variables'], ['rows'])
        self.assertEqual(set(record['outputs'][self.block_uuid]), {'output_0'})

        compared = await self.detail(second.id, compare_with=[str(first.id)])
        self.assertEqual(compared['comparison']['variables']['changed'], ['rows'])
        self.assertTrue(compared['comparison']['same_code'])
        self.assertEqual(compared['comparison']['outputs'][0]['result'], 'differs')

    async def test_a_reproduction_runs_in_its_own_process(self):
        run = self.run_pipeline()

        def command(repo_path, pipeline_uuid, run_id, report):
            return [sys.executable, '-c', FAKE_REPRODUCE, report]

        with patch.object(resource_module, 'reproduce_command', command):
            started = (await BaseOperation(
                action=OperationType.CREATE,
                payload=dict(run_record=dict(pipeline_run_id=run.id)),
                resource='run_records',
                user=self.owner,
            ).execute())['run_record']
        self.assertEqual(started['reproduction']['status'], 'running')
        for _ in range(100):
            record = await self.detail(run.id)
            if record['reproduction']['status'] != 'running':
                break
            await asyncio.sleep(0.1)
        self.assertEqual(record['reproduction']['status'], 'passed')
        self.assertIn('Reproducing pipeline run.', record['reproduction']['log'])
