"""Kernel usage requests close their client, so the garbage collector never waits on it."""
import asyncio
import unittest

import zmq

from mage_ai.kernels.models import close_client, request_usage


class FakeChannel:
    def __init__(self, reply=None, error=None):
        self.reply = reply
        self.error = error
        self.sent = []

    def send(self, message):
        self.sent.append(message)

    def get_msg(self, timeout=None):
        if self.error:
            raise self.error
        return self.reply


class FakeClient:
    def __init__(self, channel):
        self.control_channel = channel
        self.context = zmq.Context()
        self._created_context = True
        # A socket with a message the kernel never took: closing it with the default
        # linger would wait forever.
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect('tcp://127.0.0.1:1')
        self.socket.send(b'usage_request')
        self.stopped = False

    def stop_channels(self):
        self.stopped = True


class FakeSession:
    def msg(self, kind, content):
        return {'msg_type': kind, 'content': content}


class FakeKernel:
    def __init__(self, channel):
        self.session = FakeSession()
        self.clients = []
        self.channel = channel

    def client(self):
        client = FakeClient(self.channel)
        self.clients.append(client)
        return client


class RequestUsageTest(unittest.TestCase):
    def test_a_reply_gives_the_usage_and_closes_the_client(self):
        kernel = FakeKernel(FakeChannel(reply={'content': {'cpu': 1.5}}))

        usage = asyncio.run(request_usage(kernel))

        self.assertEqual(usage, {'cpu': 1.5})
        client = kernel.clients[0]
        self.assertTrue(client.stopped)
        self.assertTrue(client.context.closed)

    def test_a_kernel_that_does_not_answer_still_closes_the_client(self):
        kernel = FakeKernel(FakeChannel(error=TimeoutError('no reply')))

        self.assertIsNone(asyncio.run(request_usage(kernel)))

        self.assertTrue(kernel.clients[0].context.closed)

    def test_a_shared_context_is_left_open(self):
        client = FakeClient(FakeChannel())
        client._created_context = False

        close_client(client)

        self.assertFalse(client.context.closed)
        client.context.destroy(linger=0)
