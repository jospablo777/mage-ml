"""
Datasets through the API: registered, found by search with their status, read with their
dependencies, removed; a bad registration says why, and only editors change them.
"""
from pathlib import Path

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation import datasets
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_pipeline_with_blocks, create_user


class DatasetResourceTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        self.addCleanup(lambda: Path(self.repo_path, datasets.FILE).unlink(missing_ok=True))
        self.pipeline = create_pipeline_with_blocks(self.faker.unique.name(), self.repo_path)
        self.owner = create_user(_owner=True)

    async def call(self, action, user=None, pk=None, query=None, **payload):
        return await BaseOperation(
            action=action,
            payload=dict(dataset=payload) if payload else None,
            pk=pk,
            query=query or {},
            resource='datasets',
            user=user or self.owner,
        ).execute()

    async def test_register_search_read_and_remove(self):
        made = await self.call(
            OperationType.CREATE, id='orders', pipeline=self.pipeline.uuid, block='block1',
            owner='ana@example.com', max_age='1d',
        )
        self.assertEqual(made['dataset']['id'], 'orders')
        await self.call(OperationType.CREATE, id='scores', pipeline=self.pipeline.uuid,
                        block='block2')

        found = (await self.call(OperationType.LIST, query=dict(query=['ana'])))['datasets']
        self.assertEqual([d['id'] for d in found], ['orders'])
        self.assertEqual(found[0]['freshness']['reason'], 'The block has not completed a run.')

        detail = (await self.call(OperationType.DETAIL, pk='scores'))['dataset']
        self.assertEqual(detail['dependencies']['upstream'], [dict(id='orders', via='block')])
        self.assertEqual(detail['runs'], [])

        refused = await self.call(OperationType.CREATE, id='x', pipeline=self.pipeline.uuid,
                                  block='nope')
        self.assertIn('has no block nope', refused['error']['message'])
        await self.call(OperationType.DELETE, pk='orders')
        self.assertEqual([d['id'] for d in datasets.load(self.repo_path)], ['scores'])
        missing = await self.call(OperationType.DETAIL, pk='orders')
        self.assertEqual(missing['error']['code'], 404)

    async def test_a_user_who_is_no_editor_cannot_register(self):
        refused = await self.call(OperationType.CREATE, user=create_user(), id='orders',
                                  pipeline=self.pipeline.uuid, block='block1')
        self.assertEqual(refused['error']['code'], 403)
