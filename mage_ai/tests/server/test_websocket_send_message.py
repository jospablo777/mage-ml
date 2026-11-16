"""
Outputs sent to the notebook keep their shape after secrets are hidden in them; a closed
client does not stop the others from getting them; running blocks are tracked for clients
that reconnect (I-125).
"""
import json
import os
import unittest
from unittest.mock import patch

import tornado.websocket

from mage_ai.server.websocket_server import WebSocketServer


class FakeClient:
    def __init__(self):
        self.messages = []

    def write_message(self, message):
        self.messages.append(json.loads(message))


class ClosedClient:
    def write_message(self, message):
        raise tornado.websocket.WebSocketClosedError()


class SendMessageTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()
        clients = patch.object(WebSocketServer, 'clients', {self.client})
        mapping = patch.object(
            WebSocketServer, 'running_executions_mapping', {'m1': dict(block_uuid='draw')},
        )
        hide = patch('mage_ai.server.websocket_server.HIDE_ENV_VAR_VALUES', True)
        running = patch.object(WebSocketServer, 'running_blocks', {})
        for p in (clients, mapping, hide, running):
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


    def test_a_closed_client_does_not_stop_the_others(self):
        closed = ClosedClient()
        WebSocketServer.clients.clear()
        WebSocketServer.clients.update([closed, self.client])

        WebSocketServer.send_message(dict(msg_id='m1', execution_state='idle'))

        self.assertEqual(self.client.messages[0]['execution_state'], 'idle')
        self.assertNotIn(closed, WebSocketServer.clients)

    def test_a_block_runs_from_busy_to_idle(self):
        WebSocketServer.send_message(dict(msg_id='m1', execution_state='busy'))
        self.assertEqual(
            list(WebSocketServer.running_blocks.values()),
            [dict(block_type=None, pipeline_uuid=None, uuid='draw')],
        )

        WebSocketServer.send_message(dict(msg_id='m1', execution_state='idle'))
        self.assertEqual(WebSocketServer.running_blocks, {})

    def test_a_kernel_restart_ends_every_run(self):
        WebSocketServer.send_message(dict(msg_id='m1', execution_state='busy'))
        WebSocketServer.clear_running()
        self.assertEqual(WebSocketServer.running_blocks, {})


if __name__ == '__main__':
    unittest.main()
