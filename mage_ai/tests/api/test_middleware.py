"""
With authentication on: a session cookie that does not decode (another JWT_SECRET, a cut
value, an old format) is an invalid token, answered 401 so the frontend signs in again, not
an exception that answers 500 to every request; and the traceback of a failure goes to a
signed-in user only, the server's log otherwise.
"""
import json
from unittest.mock import patch

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from mage_ai.api.middleware import OAuthMiddleware
from mage_ai.orchestration.db import db_connection
from mage_ai.orchestration.db.models.oauth import Oauth2Application
from mage_ai.server.api.base import BaseHandler
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


class Fails(BaseHandler):
    def prepare(self):
        signed_in = self.get_argument('signed_in', None)
        self.request.current_user = object() if signed_in else None

    def get(self):
        raise RuntimeError('the source failed')


class ErrorAnswerTest(AsyncHTTPTestCase):
    def get_app(self):
        return Application([(r'/fails', Fails)])

    def test_only_a_signed_in_user_gets_the_traceback(self):
        with patch('mage_ai.server.api.base.REQUIRE_USER_AUTHENTICATION', True), \
                self.assertLogs('mage_ai.server.api.base', level='ERROR') as logged:
            stranger = json.loads(self.fetch('/fails').body)['error']
        # The exception's message, which callers with a trigger token need; no traceback,
        # whose frames name the server's files.
        self.assertEqual(stranger['exception'], 'the source failed')
        self.assertNotIn('Traceback', json.dumps(stranger))
        self.assertNotIn(__file__, json.dumps(stranger))
        self.assertIn(__file__, '\n'.join(logged.output))

        with patch('mage_ai.server.api.base.REQUIRE_USER_AUTHENTICATION', True):
            editor = json.loads(self.fetch('/fails?signed_in=1').body)['error']
        self.assertIn('Traceback', editor['message'])
        self.assertIn(__file__, editor['message'])
