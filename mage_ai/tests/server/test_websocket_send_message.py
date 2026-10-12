"""Outputs sent to the notebook keep their shape after secrets are hidden in them."""
import json
import os
import unittest
from unittest.mock import patch

from mage_ai.server.websocket_server import WebSocketServer


class FakeClient:
    def __init__(self):
        self.messages = []

    def write_message(self, message):
        self.messages.append(json.loads(message))


class SendMessageTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()
        clients = patch.object(WebSocketServer, 'clients', {self.client})
        mapping = patch.object(
            WebSocketServer, 'running_executions_mapping', {'m1': dict(block_uuid='draw')},
        )
        hide = patch('mage_ai.server.websocket_server.HIDE_ENV_VAR_VALUES', True)
        for p in (clients, mapping, hide):
            p.start()
            self.addCleanup(p.stop)

    def test_an_image_stays_a_string(self):
        """
        Hiding secrets turned the image's base64 text into a list, and the notebook showed
        an empty image: it renders only a string.
        """
        WebSocketServer.send_message(dict(msg_id='m1', data='iVBORw0KGgo=', type='image/png'))

        self.assertEqual(self.client.messages[0]['data'], 'iVBORw0KGgo=')

    def test_secrets_are_still_hidden_in_lines_and_text(self):
        with patch.dict(os.environ, {'SHOP_DB_PASSWORD': 's3cret-value-1234'}):
            WebSocketServer.send_message(
                dict(msg_id='m1', data='password s3cret-value-1234', type='text/html'),
            )
            WebSocketServer.send_message(
                dict(msg_id='m1', data=['a', 'b s3cret-value-1234'], type='text/plain'),
            )

        html, lines = (m['data'] for m in self.client.messages)
        self.assertIsInstance(html, str)
        self.assertNotIn('s3cret-value-1234', html)
        self.assertIsInstance(lines, list)
        self.assertNotIn('s3cret-value-1234', lines[1])


if __name__ == '__main__':
    unittest.main()
