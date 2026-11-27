"""
A pipeline's partitions through the API: their states, running the missing ones, accepting
code, and the refusals (a pipeline without partitions, a viewer running them).
"""
from unittest.mock import patch

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import partition_state
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user

DECLARATION = dict(key='region', values=['eu', 'us'])


class PipelinePartitionResourceTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        partition_state._codes.clear()
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        self.owner = create_user(_owner=True)
        code = patch('mage_ai.orchestration.run_records.code_digest', return_value='code-1')
        code.start()
        self.addCleanup(code.stop)

    async def declare(self):
        await self.pipeline.update(dict(partitions=DECLARATION))

    async def act(self, user=None, **payload):
        return await BaseOperation(
            action=OperationType.CREATE,
            payload=dict(pipeline_partition=dict(pipeline_uuid=self.pipeline.uuid, **payload)),
            resource='pipeline_partitions',
            user=user or self.owner,
        ).execute()

    async def states(self):
        return await BaseOperation(
            action=OperationType.DETAIL,
            pk=self.pipeline.uuid,
            resource='pipeline_partitions',
            user=self.owner,
        ).execute()

    async def test_states_and_running_the_missing(self):
        await self.declare()
        found = (await self.states())['pipeline_partition']
        self.assertEqual(found['counts']['missing'], 2)
        self.assertEqual([p['partition'] for p in found['partitions']], ['eu', 'us'])

        ran = (await self.act(action='run', select='missing'))['pipeline_partition']

        self.assertEqual([r['partition'] for r in ran['result']['created']], ['eu', 'us'])
        self.assertEqual(ran['counts']['queued'], 2)
        runs = PipelineRun.query.filter(PipelineRun.pipeline_uuid == self.pipeline.uuid).all()
        self.assertEqual(sorted(r.partition for r in runs), ['eu', 'us'])

    async def test_accepting_code_and_a_bad_request(self):
        await self.declare()
        done = (await self.act(action='accept_code', select=['eu']))['pipeline_partition']
        self.assertEqual(done['result'], dict(accepted=['eu']))
        refused = await self.act(action='delete', select='missing')
        self.assertIn('action is run or accept_code', str(refused['error']))
        unknown = await self.act(action='run', select=['apac'])
        self.assertIn("'apac' is not one of the values", str(unknown['error']))

    async def test_a_pipeline_without_partitions_says_so(self):
        found = await self.states()
        self.assertIn('declares no partitions', str(found['error']))

    async def test_a_user_who_is_no_editor_cannot_run_them(self):
        await self.declare()
        viewer = create_user()
        refused = await self.act(user=viewer, action='run', select='missing')
        self.assertEqual(refused['error']['code'], 403)
