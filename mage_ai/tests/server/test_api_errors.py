import json
from unittest.mock import AsyncMock, patch

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from mage_ai.server.api.base import BaseHandler


class FailingHandler(BaseHandler):
    async def get(self, **kwargs):
        raise ValueError('the resource failed')


class ApiErrorTest(AsyncHTTPTestCase):
    def get_app(self):
        return Application([(r'/api/(?P<resource>\w+)', FailingHandler)])

    def test_an_error_is_written_as_json(self):
        """asyncio.run raised in the server's event loop, so the response was empty."""
        with patch('mage_ai.server.api.base.UsageStatisticLogger') as logger:
            logger.return_value.error = AsyncMock()
            response = self.fetch('/api/things')

        self.assertEqual(response.code, 200)
        body = json.loads(response.body)
        self.assertEqual(body['error']['exception'], 'the resource failed')
        self.assertEqual(body['error']['code'], 500)
        logger.return_value.error.assert_awaited_once()
