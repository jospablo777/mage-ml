import json

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from mage_ai.server.api.base import BaseHandler


class FailingHandler(BaseHandler):
    def prepare(self):
        # Signed in: the traceback goes to signed-in users only (test_middleware.py).
        self.request.current_user = object()

    async def get(self, **kwargs):
        error = ValueError('the resource failed')
        error.add_note('Block load (data_loaders/load.py) failed.')
        raise error


class ApiErrorTest(AsyncHTTPTestCase):
    def get_app(self):
        return Application([(r'/api/(?P<resource>\w+)', FailingHandler)])

    def test_an_error_is_written_as_json(self):
        response = self.fetch('/api/things')

        self.assertEqual(response.code, 200)
        body = json.loads(response.body)
        self.assertEqual(body['error']['exception'], 'the resource failed')
        self.assertEqual(body['error']['code'], 500)

    def test_the_error_carries_its_own_traceback_and_notes(self):
        body = json.loads(self.fetch('/api/things').body)['error']

        self.assertIn('in get', body['message'])
        self.assertIn("raise error", body['message'])
        self.assertIn('ValueError: the resource failed', body['message'])
        self.assertIn('Block load (data_loaders/load.py) failed.', body['message'])
        # Not the stack of the error handler, which said nothing about the failure.
        self.assertNotIn('write_error', body['message'])
        self.assertEqual(body['errors'], [])
