"""
Compile a pipeline from the editor: the API starts a compilation, reports its progress
and its last result, refuses an unknown pipeline and needs an editor to start one.
"""
import asyncio
import textwrap
from pathlib import Path
from unittest.mock import patch

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import compilation
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user

LOAD = textwrap.dedent('''
    use mage::prelude::*;

    fn load_data() -> Result<DataFrame> {
        Ok(df!("a" => [1i64, 2, 3])?)
    }
''')


def built(pipeline, unit):
    unit.state = 'compiled'


class PipelineCompilationResourceTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        compilation._compilations.clear()
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        block = Block.create(f'{self.pipeline.uuid}_load', 'data_loader', self.repo_path,
                             language='rust')
        Path(block.file_path).write_text(LOAD)
        self.pipeline.add_block(block)
        self.owner = create_user(_owner=True)

    async def start(self, user=None, pipeline_uuid=None):
        return await BaseOperation(
            action=OperationType.CREATE,
            payload=dict(pipeline_compilation=dict(
                pipeline_uuid=pipeline_uuid or self.pipeline.uuid,
            )),
            resource='pipeline_compilations',
            user=user or self.owner,
        ).execute()

    async def status(self):
        return (await BaseOperation(
            action=OperationType.DETAIL,
            pk=self.pipeline.uuid,
            resource='pipeline_compilations',
            user=self.owner,
        ).execute())['pipeline_compilation']

    async def test_a_compilation_reports_its_progress_and_last_result(self):
        before = await self.status()
        self.assertEqual((before['state'], before['total'], before['done']), ('none', 1, 0))
        self.assertIsNone(before['last_compiled'])
        self.assertEqual(before['summary']['languages'], ['rust'])

        with patch.object(compilation, '_compile_unit', built):
            started = (await self.start())['pipeline_compilation']
            self.assertIn(started['state'], ('running', 'completed'))
            for _ in range(100):
                done = await self.status()
                if done['state'] != 'running':
                    break
                await asyncio.sleep(0.05)
        self.assertEqual((done['state'], done['done']), ('completed', 1))
        self.assertTrue(done['last_compiled']['up_to_date'])
        self.assertIsNotNone(done['finished_at'])

        detail = (await BaseOperation(
            action=OperationType.DETAIL,
            pk=self.pipeline.uuid,
            resource='pipelines',
            user=self.owner,
        ).execute())['pipeline']
        self.assertTrue(detail['last_compiled']['up_to_date'])

    async def test_an_unknown_pipeline_is_an_error(self):
        response = await self.start(pipeline_uuid='no_such_pipeline')
        self.assertEqual(response['error']['code'], 404)
        self.assertIn('does not exist', response['error']['message'])
