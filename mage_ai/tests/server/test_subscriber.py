import asyncio
import inspect
import threading
import unittest
from queue import Empty
from unittest.mock import Mock, patch

from mage_ai.server.subscriber import get_messages


class SubscriberTest(unittest.IsolatedAsyncioTestCase):
    async def test_kernel_polling_leaves_event_loop_available(self):
        self.assertTrue(inspect.iscoroutinefunction(get_messages))
        started = threading.Event()
        release = threading.Event()
        received = asyncio.Event()
        reader_threads = []
        callback_threads = []
        message = {'content': {'text': 'output'}}

        def read_message(timeout):
            reader_threads.append(threading.get_ident())
            started.set()
            if not release.wait(timeout):
                raise Empty
            return message

        def callback(result):
            self.assertEqual(result, message)
            callback_threads.append(threading.get_ident())
            received.set()

        client = Mock(get_iopub_msg=read_message)
        with patch('mage_ai.server.subscriber.get_active_kernel_client', return_value=client):
            subscriber = asyncio.create_task(get_messages(callback))
            try:
                await asyncio.wait_for(asyncio.to_thread(started.wait, 2), timeout=3)
                self.assertTrue(started.is_set())
                self.assertFalse(received.is_set())
                release.set()
                await asyncio.wait_for(received.wait(), timeout=3)
            finally:
                release.set()
                subscriber.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await subscriber

        self.assertTrue(all(t != threading.get_ident() for t in reader_threads))
        self.assertEqual(set(callback_threads), {threading.get_ident()})

    async def test_empty_poll_does_not_stop_subscription(self):
        received = asyncio.Event()
        client = Mock()
        client.get_iopub_msg.side_effect = [Empty, {'content': {'text': 'output'}}]
        with patch('mage_ai.server.subscriber.get_active_kernel_client', return_value=client):
            subscriber = asyncio.create_task(get_messages(lambda _: received.set()))
            try:
                await asyncio.wait_for(received.wait(), timeout=3)
            finally:
                subscriber.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await subscriber
