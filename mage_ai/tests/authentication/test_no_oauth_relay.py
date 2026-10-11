"""
MageML signs in without Mage's servers: GitHub and Active Directory used Mage's own OAuth
applications, which sent the authorization code, and so the token, through api.mage.ai.
"""
import os
import unittest
from unittest.mock import patch

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.OauthResource import OauthResource
from mage_ai.authentication.providers.active_directory import ADProvider


class NoOauthRelayTest(unittest.IsolatedAsyncioTestCase):
    async def test_github_sign_in_through_mage_is_refused_with_the_alternatives(self):
        with patch('mage_ai.api.resources.OauthResource.get_ghe_hostname', return_value=None), \
                patch('mage_ai.api.resources.OauthResource.access_tokens_for_client',
                      return_value=[]):
            with self.assertRaises(ApiError) as raised:
                await OauthResource.member('github', None, query={
                    'redirect_uri': ['http://localhost:6789/version-control'],
                })
        message = raised.exception.message
        self.assertIn('personal access token', message)
        self.assertIn('GHE_HOSTNAME=https://github.com', message)

    def test_active_directory_needs_your_own_azure_application(self):
        with patch.dict(os.environ, dict(ACTIVE_DIRECTORY_DIRECTORY_ID='tenant'), clear=False):
            os.environ.pop('ACTIVE_DIRECTORY_CLIENT_ID', None)
            os.environ.pop('ACTIVE_DIRECTORY_CLIENT_SECRET', None)
            with self.assertRaisesRegex(Exception, 'your own Azure application'):
                ADProvider()

        with patch.dict(os.environ, dict(
            ACTIVE_DIRECTORY_DIRECTORY_ID='tenant',
            ACTIVE_DIRECTORY_CLIENT_ID='client',
            ACTIVE_DIRECTORY_CLIENT_SECRET='secret',
        )):
            response = ADProvider().get_auth_url_response(
                redirect_uri='http://localhost:6789/sign-in',
            )
        self.assertIn('client_id=client', response['url'])
        self.assertNotIn('mage.ai', response['url'])
        self.assertIn('localhost%3A6789%2Foauth', response['url'])
