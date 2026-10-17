"""
The header asks for the git branch on every page and block run; setting up the git manager
(keys, git config, git commands) ran on the server's event loop and held every request.
"""
import asyncio
import time
from types import SimpleNamespace
from unittest.mock import patch

from mage_ai.api.resources.GitBranchResource import GitBranchResource
from mage_ai.tests.base_test import TestCase


class SlowGit:
    @property
    def current_branch(self):
        time.sleep(0.3)
        return 'main'


class GitBranchResourceTest(TestCase):
    def test_the_branch_is_read_without_holding_the_event_loop(self):
        preferences = SimpleNamespace(
            is_git_integration_enabled=lambda: True, sync_config={},
        )

        def slow_manager(*args, **kwargs):
            time.sleep(0.3)
            return SlowGit()

        async def scenario():
            ticks = []

            async def ticker():
                for _ in range(30):
                    ticks.append(time.monotonic())
                    await asyncio.sleep(0.02)

            with patch.object(GitBranchResource, 'get_git_manager', side_effect=slow_manager), \
                    patch('mage_ai.api.resources.GitBranchResource.get_preferences',
                          return_value=preferences):
                ticking = asyncio.ensure_future(ticker())
                await asyncio.sleep(0.05)
                resource = await GitBranchResource.member(
                    'test', None, meta={'_format': 'with_basic_details'},
                )
                await ticking
            gaps = [b - a for a, b in zip(ticks, ticks[1:])]
            return resource, max(gaps)

        resource, longest_gap = asyncio.run(scenario())
        self.assertEqual(resource.model['name'], 'main')
        self.assertLess(longest_gap, 0.2)
