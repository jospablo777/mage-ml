"""
The integration sources and destinations the editor lists carry their README, which it shows
next to the settings: the presenters dropped it, so no source's documentation ever showed.
"""
from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user


class IntegrationListsTest(AsyncDBTestCase):
    async def listed(self, resource):
        response = await BaseOperation(
            action=OperationType.LIST,
            resource=resource,
            user=create_user(_owner=True),
        ).execute()
        return {item['uuid']: item for item in response[resource]}

    async def test_sources_come_with_their_documentation_and_settings(self):
        sources = await self.listed('integration_sources')
        amplitude = sources['amplitude']
        self.assertIn('How events sync', amplitude['docs'])
        self.assertEqual(amplitude['templates']['config']['region'], 'US')

    async def test_destinations_come_with_their_documentation(self):
        destinations = await self.listed('integration_destinations')
        self.assertTrue(destinations['postgresql']['docs'])
