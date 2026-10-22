"""
GET /api/status, open to all as a health check: a caller who has not signed in reads only
whether to sign in, and a signed-in caller the whole status, which the app needs (the file
editor reads repo_path). The route skipped the session, so signed-in callers got the short
status too.
"""
import json
from unittest.mock import patch

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from mage_ai.authentication.oauth2 import encode_token, generate_access_token
from mage_ai.orchestration.db.models.oauth import Oauth2Application
from mage_ai.server.api.v1 import ApiListHandler
from mage_ai.settings import OAUTH2_APPLICATION_CLIENT_ID
from mage_ai.tests.base_test import DBTestCase
from mage_ai.tests.factory import create_user


class StatusRouteTest(AsyncHTTPTestCase, DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)
        AsyncHTTPTestCase.setUp(self)
        for target in ('mage_ai.api.middleware.REQUIRE_USER_AUTHENTICATION',
                       'mage_ai.api.policies.BasePolicy.REQUIRE_USER_AUTHENTICATION'):
            patcher = patch(target, True)
            patcher.start()
            self.addCleanup(patcher.stop)

    def get_app(self):
        return Application([(
            r'/api/status(?:es)?', ApiListHandler,
            dict(resource='statuses', bypass_oauth_check=True, is_health_check=True),
        )])

    def status(self, cookie=None):
        response = self.fetch('/api/status', headers={'Cookie': cookie} if cookie else {})
        self.assertEqual(response.code, 200, response.body)
        return json.loads(response.body)['statuses'][0]

    def test_a_signed_in_caller_reads_the_whole_status(self):
        public = self.status()
        self.assertIsNone(public['repo_path'])
        self.assertTrue(public['require_user_authentication'])

        client = Oauth2Application.query_client(OAUTH2_APPLICATION_CLIENT_ID) or \
            Oauth2Application.create(client_id=OAUTH2_APPLICATION_CLIENT_ID, name='frontend')
        token = generate_access_token(create_user(_owner=True), client)
        signed_in = self.status(f'oauth_token={encode_token(token.token, token.expires)}')
        self.assertEqual(signed_in['repo_path'], self.repo_path)
        self.assertIsNotNone(signed_in['server_process_id'])
