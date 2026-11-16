"""
The session cookie (I-25): set by the server on sign-in as HttpOnly with SameSite=Strict,
with a readable cookie of its expiry only, and cleared on sign-out.
"""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import TestCase

from mage_ai.authentication import session_cookie
from mage_ai.authentication.oauth2 import encode_token


class FakeHandler:
    def __init__(self, protocol='http', headers=None):
        self.request = SimpleNamespace(protocol=protocol, host='mage.example.com:6789',
                                       headers=headers or {})
        self.set = []
        self.cleared = []

    def set_cookie(self, name, value, **options):
        self.set.append((name, value, options))

    def clear_cookie(self, name, **options):
        self.cleared.append((name, options))


class SessionCookieTest(TestCase):
    def token(self):
        expires = datetime.now(timezone.utc) + timedelta(days=30)
        return encode_token('access', expires), int(expires.timestamp())

    def test_a_sign_in_sets_an_http_only_cookie_and_its_expiry(self):
        handler = FakeHandler()
        token, expires = self.token()

        session_cookie.apply(handler, 'sessions', 'create', dict(session=dict(token=token)))

        (name, value, options), (expiry_name, expiry, expiry_options) = handler.set
        self.assertEqual((name, value), ('oauth_token', token))
        self.assertTrue(options['httponly'])
        self.assertEqual(options['samesite'], 'Strict')
        self.assertFalse(options['secure'])
        self.assertEqual((expiry_name, expiry), ('oauth_token_expires', str(expires)))
        self.assertNotIn('httponly', expiry_options)
        # The copy a page set, with its host as Domain, goes.
        self.assertIn(('oauth_token', dict(path='/', domain='mage.example.com')),
                      handler.cleared)

    def test_https_behind_a_proxy_makes_it_secure(self):
        handler = FakeHandler(headers={'X-Forwarded-Proto': 'https'})
        token, _ = self.token()
        session_cookie.apply(handler, 'sessions', 'create', dict(session=dict(token=token)))
        self.assertTrue(all(options['secure'] for _, _, options in handler.set))

    def test_a_sign_out_clears_them_and_errors_change_nothing(self):
        handler = FakeHandler()
        session_cookie.apply(handler, 'sessions', 'update', dict(session=dict()))
        self.assertEqual({name for name, _ in handler.cleared},
                         {'oauth_token', 'oauth_token_expires'})

        handler = FakeHandler()
        session_cookie.apply(handler, 'sessions', 'create', dict(error=dict(code=401)))
        session_cookie.apply(handler, 'pipelines', 'create', dict(pipeline=dict(token='x')))
        self.assertEqual((handler.set, handler.cleared), ([], []))
