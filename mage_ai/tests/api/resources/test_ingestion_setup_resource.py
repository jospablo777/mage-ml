"""
Guided ingestion through the API: each action reaches its step, a bad request says why, a
password never comes back, and only editors set ingestion up.
"""
from unittest.mock import patch

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation import ingestion
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user

SOURCE = dict(
    host='db', database='shop', user='reader', password='hunter2',  # pragma: allowlist secret
)


class IngestionSetupResourceTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        self.owner = create_user(_owner=True)

    async def call(self, user=None, **payload):
        return await BaseOperation(
            action=OperationType.CREATE,
            payload=dict(ingestion_setup=payload),
            resource='ingestion_setups',
            user=user or self.owner,
        ).execute()

    async def test_each_action_reaches_its_step_and_no_password_comes_back(self):
        checked = dict(checks=[dict(name='connect', ok=True, detail='ok')], ready=True,
                       tables=[dict(table='public.orders', key=['id'])])
        with patch.object(ingestion, 'check_source', return_value=checked) as check:
            found = (await self.call(action='check_source', source=SOURCE))['ingestion_setup']
        check.assert_called_once_with(SOURCE)
        self.assertEqual(found['tables'], checked['tables'])
        self.assertNotIn('hunter2', str(found))

        reviewed = (await self.call(
            action='review', tables=[dict(table='public.orders', key=['id'])],
            schema='raw', prefix='copy_',
        ))['ingestion_setup']
        self.assertIn('raw.copy_<table>', reviewed['review'][1])
        # A MySQL source's review says where its changes come from.
        mysql = (await self.call(
            action='review', kind='mysql', tables=[dict(table='shop.orders', key=['id'])],
        ))['ingestion_setup']
        self.assertIn('(the binary log)', mysql['review'][0])

        refused = await self.call(action='drop')
        self.assertIn('action is check_source', str(refused['error']))
        with patch.object(ingestion, 'sample', side_effect=ingestion.IngestionError('nope')):
            failed = await self.call(action='sample', source=SOURCE, table='public.orders')
        self.assertEqual((failed['error']['code'], failed['error']['message']), (400, 'nope'))

    async def test_create_answers_with_the_blocks_the_run_panel_starts(self):
        made = dict(pipeline_uuid='sync_shop', review=['Copies every row'],
                    blocks=[dict(uuid='sync_shop_source', type='data_loader'),
                            dict(uuid='sync_shop_copy', type='data_exporter')])
        with patch.object(ingestion, 'create', return_value=made):
            found = (await self.call(
                action='create', name='sync_shop', source=SOURCE, destination=SOURCE,
                tables=[dict(table='public.orders', key=['id'])],
            ))['ingestion_setup']
        self.assertEqual(found['pipeline_uuid'], 'sync_shop')
        self.assertEqual(found['blocks'], made['blocks'])
        self.assertNotIn('hunter2', str(found))

    async def test_a_user_who_is_no_editor_cannot_set_ingestion_up(self):
        refused = await self.call(user=create_user(), action='check_source', source=SOURCE)
        self.assertEqual(refused['error']['code'], 403)
