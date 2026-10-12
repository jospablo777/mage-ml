"""A resource that is not found says which one, not only "Record not found."."""
from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user


class NotFoundMessageTest(AsyncDBTestCase):
    async def detail(self, resource, pk):
        return await BaseOperation(
            action=OperationType.DETAIL, pk=pk, resource=resource,
            user=create_user(_owner=True),
        ).execute()

    async def test_a_missing_record_is_named_with_its_id(self):
        error = (await self.detail('pipeline_runs', 987654))['error']

        self.assertEqual(error['code'], 404)
        self.assertEqual(error['message'], 'Pipeline run 987654 was not found.')

    async def test_a_specific_message_is_kept(self):
        error = (await self.detail('language_setups', 'cobol'))['error']

        self.assertEqual(error['message'], 'Setup is checked for r, rust, not cobol.')
