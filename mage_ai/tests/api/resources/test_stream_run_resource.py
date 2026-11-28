"""
A stream run's status through the API keeps what the engine reports, the initial copy
table by table among it.
"""
from unittest.mock import patch

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.constants import PipelineType
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.streaming import dev_run
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user

COPY = [
    dict(table='public.orders', estimate=1200, copied=1200, done=True),
    dict(table='public.lines', estimate=None, copied=40, done=False),
]


class StreamRunResourceTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self.faker.unique.name(),
                                        pipeline_type=PipelineType.STREAMING,
                                        repo_path=self.repo_path)
        self.owner = create_user(_owner=True)

    async def test_the_status_keeps_the_initial_copy(self):
        status = dict(pipeline_uuid=self.pipeline.uuid, running=True, state='running',
                      lag={'0': 0}, copy=COPY, logs=[])
        with patch.object(dev_run, 'status', return_value=status):
            found = await BaseOperation(
                action=OperationType.DETAIL,
                pk='current',
                resource='stream_runs',
                resource_parent='pipelines',
                resource_parent_id=self.pipeline.uuid,
                user=self.owner,
            ).execute()
        run = found['stream_run']
        self.assertEqual(run['copy'], COPY)
        self.assertEqual(run['state'], 'running')
