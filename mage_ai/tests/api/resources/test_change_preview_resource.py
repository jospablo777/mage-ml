"""
Change previews through the API: one started against the last completed run, listed and
read with each block's comparison, and the refusals (no completed run, another pipeline's
run, a user who is no editor).
"""
import asyncio
import textwrap
from pathlib import Path
from unittest.mock import patch

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import previews
from mage_ai.orchestration.fusion_verify import run_in_process
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user

LOAD = '''
import pandas as pd


@data_loader
def load(*args, **kwargs):
    return pd.DataFrame({'id': [1, 2, 3], 'price': [10.0, 20.0, 30.0]})
'''


class ChangePreviewResourceTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        self.load = Block.create(
            f'{self.pipeline.uuid}_load', 'data_loader', self.repo_path, pipeline=self.pipeline,
        )
        Path(self.load.file_path).write_text(textwrap.dedent(LOAD))
        self.pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        self.owner = create_user(_owner=True)
        # The preview runs here, not in a process of its own.
        start = patch.object(previews, 'start', side_effect=lambda pipeline, preview_id: (
            previews.run(Pipeline.get(pipeline.uuid, repo_path=self.repo_path), preview_id,
                         log=lambda _line: None)
        ))
        start.start()
        self.addCleanup(start.stop)

    async def call(self, action, user=None, pk=None, **payload):
        return await BaseOperation(
            action=action,
            payload=dict(change_preview=payload) if action == OperationType.CREATE else None,
            pk=pk,
            query=dict(pipeline_uuid=[self.pipeline.uuid]),
            resource='change_previews',
            user=user or self.owner,
        ).execute()

    async def test_a_preview_is_started_listed_and_read_with_its_comparison(self):
        refused = await self.call(OperationType.CREATE, pipeline_uuid=self.pipeline.uuid)
        self.assertIn('no completed run', str(refused['error']))

        run = await asyncio.to_thread(
            lambda: run_in_process(trigger_pipeline(self.pipeline.uuid, variables={}),
                                   log=lambda _line: None),
        )
        Path(self.load.file_path).write_text(
            textwrap.dedent(LOAD).replace('30.0', '31.0'),
        )
        created = (await self.call(OperationType.CREATE,
                                   pipeline_uuid=self.pipeline.uuid))['change_preview']
        self.assertEqual(created['baseline_run_id'], run.id)

        listed = (await self.call(OperationType.LIST))['change_previews']
        self.assertEqual([p['id'] for p in listed], [created['id']])
        found = (await self.call(OperationType.DETAIL, pk=created['id']))['change_preview']
        self.assertEqual(found['state'], 'done', found.get('error'))
        [block] = found['blocks']
        diff = block['outputs']['output_0']['diff']
        self.assertEqual((diff['changed'], diff['columns']), (1, {'price': 1}))

        other = await self.call(OperationType.CREATE, pipeline_uuid=self.pipeline.uuid,
                                baseline_run_id=run.id + 1000)
        self.assertEqual(other['error']['code'], 404)
        missing = await self.call(OperationType.DETAIL, pk='0' * 16)
        self.assertEqual(missing['error']['code'], 404)

    async def test_a_user_who_is_no_editor_cannot_start_one(self):
        refused = await self.call(OperationType.CREATE, user=create_user(),
                                  pipeline_uuid=self.pipeline.uuid)
        self.assertEqual(refused['error']['code'], 403)
