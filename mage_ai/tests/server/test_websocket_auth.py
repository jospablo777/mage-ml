"""
With authentication on, a WebSocket connection gets the kernel's outputs or a terminal only
once its user is signed in: by the session cookie of its handshake or by a message with a
valid token.
"""
import json
from types import SimpleNamespace
from unittest.mock import patch

from tornado import gen
from tornado.testing import AsyncHTTPTestCase, gen_test
from tornado.web import Application
from tornado.websocket import websocket_connect

from mage_ai.server.terminal_server import MageTermManager, TerminalWebsocketServer
from mage_ai.server.websocket_server import WebSocketServer

OUTPUT = dict(
    msg_id='m1', data='revenue 1,234', type='text/plain', execution_state='idle',
    execution_metadata=dict(block_uuid='load_orders', pipeline_uuid='orders'),
)


def signed_in(cookie_header):
    return SimpleNamespace(id=1) if 'oauth_token=good' in (cookie_header or '') else None


class WebSocketAuthTest(AsyncHTTPTestCase):
    def setUp(self):
        super().setUp()
        WebSocketServer.clients.clear()
        self.addCleanup(WebSocketServer.clients.clear)
        for target, value in (
            ('mage_ai.server.websocket_server.REQUIRE_USER_AUTHENTICATION', True),
            ('mage_ai.server.terminal_server.REQUIRE_USER_AUTHENTICATION', True),
            ('mage_ai.api.middleware.user_from_cookie', signed_in),
            ('mage_ai.server.terminal_server.has_at_least_editor_role', lambda *a: True),
            ('mage_ai.server.terminal_server.get_project_uuid', lambda: 'p'),
        ):
            patcher = patch(target, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def get_app(self):
        self.terms = MageTermManager(shell_command=['bash'], max_terminals=2)
        return Application([
            (r'/websocket/', WebSocketServer),
            (r'/websocket/terminal', TerminalWebsocketServer, {'term_manager': self.terms}),
        ])

    def tearDown(self):
        self.io_loop.run_sync(self.terms.shutdown)
        super().tearDown()

    async def connect(self, path, cookie=None):
        url = f'ws://127.0.0.1:{self.get_http_port()}{path}'
        from tornado.httpclient import HTTPRequest

        headers = {'Cookie': cookie} if cookie else {}
        return await websocket_connect(HTTPRequest(url, headers=headers))

    async def received(self, connection, seconds=0.5):
        try:
            return await gen.with_timeout(self.io_loop.time() + seconds,
                                          connection.read_message())
        except gen.TimeoutError:
            return None

    @gen_test
    async def test_a_connection_that_is_not_signed_in_gets_no_outputs(self):
        stranger = await self.connect('/websocket/')
        await gen.sleep(0.1)
        WebSocketServer.send_message(dict(OUTPUT))
        self.assertIsNone(await self.received(stranger))
        stranger.close()

    @gen_test
    async def test_a_signed_in_connection_gets_the_outputs(self):
        user = await self.connect('/websocket/', cookie='oauth_token=good')
        await gen.sleep(0.1)
        WebSocketServer.send_message(dict(OUTPUT))
        message = json.loads(await self.received(user))
        self.assertIn('revenue 1,234', str(message['data']))
        user.close()

    @gen_test
    async def test_without_authentication_every_connection_gets_the_outputs(self):
        with patch('mage_ai.server.websocket_server.REQUIRE_USER_AUTHENTICATION', False):
            anyone = await self.connect('/websocket/')
            await gen.sleep(0.1)
            WebSocketServer.send_message(dict(OUTPUT))
            self.assertIsNotNone(await self.received(anyone))
            anyone.close()

    @gen_test
    async def test_a_terminal_needs_a_signed_in_editor(self):
        stranger = await self.connect('/websocket/terminal?term_name=admin')
        self.assertIsNone(await self.received(stranger))
        self.assertEqual(self.terms.terminals, {})
        stranger.close()

        editor = await self.connect('/websocket/terminal?term_name=admin',
                                    cookie='oauth_token=good')
        self.assertEqual(json.loads(await self.received(editor, 5)), ['setup', {}])
        self.assertIn('admin', self.terms.terminals)
        editor.close()

    @gen_test
    async def test_a_disabled_terminal_starts_no_shell(self):
        with patch('mage_ai.server.terminal_server.DISABLE_TERMINAL', True):
            editor = await self.connect('/websocket/terminal?term_name=admin',
                                        cookie='oauth_token=good')
            self.assertIsNone(await self.received(editor))
        self.assertEqual(self.terms.terminals, {})
        editor.close()
