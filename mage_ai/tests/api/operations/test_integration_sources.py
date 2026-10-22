"""
The integration sources and destinations the editor lists carry their README, which it shows
next to the settings: the presenters dropped it, so no source's documentation ever showed.
"""
import json
import subprocess
import sys
import unittest

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


LISTS_THEN_TERMINAL = '''
import asyncio
import json
import sys

from mage_ai.data_integrations.destinations.constants import DESTINATIONS
from mage_ai.data_integrations.sources.constants import SOURCES
from mage_ai.server.api.integration_sources import get_collection
from mage_ai.server.terminal_server import MageTermManager

listed = len(get_collection('sources', SOURCES)) + len(get_collection('destinations', DESTINATIONS))
imported = sorted(
    name for name in sys.modules
    if name.startswith(('mage_integrations.sources.', 'mage_integrations.destinations.'))
)


async def shell():
    term = await MageTermManager(shell_command=['bash']).get_terminal_async('t')
    await asyncio.sleep(0.5)
    term.ptyproc.write('echo hi\\r')
    await asyncio.sleep(0.2)
    alive = term.ptyproc.isalive()
    term.kill()
    return alive


print(json.dumps(dict(listed=listed, imported=imported, alive=asyncio.run(shell()))))
'''


class IntegrationListsInTheServerTest(unittest.TestCase):
    def test_listing_connectors_imports_none_and_terminals_still_start(self):
        result = subprocess.run(
            [sys.executable, '-c', LISTS_THEN_TERMINAL],
            capture_output=True, text=True, timeout=300,
        )
        self.assertEqual(result.returncode, 0, result.stderr[-3000:])
        found = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertGreater(found['listed'], 60)
        # Importing every connector loaded their client libraries into the web server.
        self.assertEqual(found['imported'], [])
        # And after that, each new terminal's shell died of SIGSEGV between fork and exec.
        self.assertTrue(found['alive'])
