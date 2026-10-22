"""
OAuthMiddleware with authentication on: a session cookie that does not decode (another
JWT_SECRET, a cut value, an old format) is an invalid token, answered 401 so the frontend
signs in again, not an exception that answers 500 to every request.
"""
from unittest.mock import patch

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from mage_ai.api.middleware import OAuthMiddleware
from mage_ai.orchestration.db import db_connection
from mage_ai.orchestration.db.models.oauth import Oauth2Application
from mage_ai.settings import OAUTH2_APPLICATION_CLIENT_ID
from mage_ai.tests.base_test import DBTestCase


class Echo(OAuthMiddleware):
    def get(self):
        error = self.request.error
        self.write(dict(error=error and error['type'], user=bool(self.request.current_user)))


class MiddlewareTest(AsyncHTTPTestCase, DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)
        AsyncHTTPTestCase.setUp(self)
        if not Oauth2Application.query_client(OAUTH2_APPLICATION_CLIENT_ID):
            Oauth2Application.create(client_id=OAUTH2_APPLICATION_CLIENT_ID, name='frontend')
        db_connection.session.commit()
        patcher = patch('mage_ai.api.middleware.REQUIRE_USER_AUTHENTICATION', True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def get_app(self):
        return Application([(r'/echo', Echo)])

    def test_a_cookie_that_does_not_decode_is_an_invalid_token(self):
        for cookie in ('oauth_token=not-a-jwt', 'oauth_token=eyJhbGciOiJIUzI1NiJ9.e30.bad'):
            with self.subTest(cookie=cookie):
                response = self.fetch('/echo', headers={
                    'X-API-KEY': OAUTH2_APPLICATION_CLIENT_ID, 'Cookie': cookie,
                })
                self.assertEqual(response.code, 401, response.body)
                self.assertIn(b'invalid_oauth_token', response.body)
