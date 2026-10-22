"""
The server status before sign-in: the app reads whether to sign in, and the server's paths,
process ids, scheduler and activity went to anyone who could reach it.
"""
from unittest.mock import patch

from mage_ai.api.operations import constants
from mage_ai.tests.api.operations.test_base import BaseApiTestCase
from mage_ai.tests.factory import create_user


class StatusOperationTests(BaseApiTestCase):
    async def status(self, user):
        response = await self.build_operation(
            action=constants.LIST, resource='statuses', user=user,
        ).execute()
        self.assertNotIn('error', response, response)
        return response['statuses'][0]

    @patch('mage_ai.api.resources.StatusResource.REQUIRE_USER_AUTHENTICATION', True)
    async def test_a_caller_who_has_not_signed_in_reads_whether_to_sign_in_only(self):
        public = await self.status(None)
        self.assertTrue(public['require_user_authentication'])
        self.assertIsNone(public.get('repo_path'))
        self.assertIsNone(public.get('server_process_id'))

        signed_in = await self.status(create_user(_owner=True))
        self.assertEqual(signed_in['repo_path'], self.repo_path)
        self.assertIsNotNone(signed_in['server_process_id'])
