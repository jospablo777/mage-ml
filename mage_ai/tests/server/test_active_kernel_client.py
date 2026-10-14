"""
The active kernel's client is closed when another replaces it: a client left to the garbage
collector froze the web server, whose thread runs the collector, while its zmq context waited
for messages a restarted kernel never took.
"""
import unittest
from unittest.mock import MagicMock, patch

from mage_ai.server import active_kernel as module


class ActiveKernelClientTest(unittest.TestCase):
    def setUp(self):
        self.kernel = MagicMock()
        self.first, self.second = MagicMock(name='first'), MagicMock(name='second')
        self.kernel.client.side_effect = [self.second]
        saved = (module.active_kernel.kernel, module.active_kernel.kernel_client)
        self.addCleanup(self.restore, saved)
        module.active_kernel.kernel = self.kernel
        module.active_kernel.kernel_client = self.first

    def restore(self, saved):
        module.active_kernel.kernel, module.active_kernel.kernel_client = saved

    def test_a_restart_closes_the_client_it_replaces(self):
        with patch('mage_ai.kernels.models.close_client') as close:
            module.restart_kernel()

        self.assertIs(module.active_kernel.kernel_client, self.second)
        close.assert_called_once_with(self.first)

    def test_a_start_closes_the_client_it_replaces(self):
        with patch('mage_ai.kernels.models.close_client') as close:
            module.start_kernel()

        close.assert_called_once_with(self.first)

    def test_while_the_output_reader_runs_it_closes_replaced_clients_on_its_thread(self):
        """
        A zmq socket closed from another thread while the reader waits on it fails, or crashes
        libzmq: the reader closes replaced clients itself, between reads.
        """
        module.set_output_reader_running(True)
        self.addCleanup(module.set_output_reader_running, False)
        with patch('mage_ai.kernels.models.close_client') as close:
            module.restart_kernel()
            close.assert_not_called()
            self.assertEqual(module.active_kernel.retired_clients, [self.first])

            module.close_retired_clients()

        close.assert_called_once_with(self.first)
        self.assertEqual(module.active_kernel.retired_clients, [])


class OutputReaderTest(unittest.IsolatedAsyncioTestCase):
    async def test_the_reader_closes_a_replaced_client_before_its_next_read(self):
        import asyncio

        from mage_ai.server import subscriber

        first, second = MagicMock(name='first'), MagicMock(name='second')
        reads = []
        closed = []
        clients = iter([first, second, second])

        def get_client():
            return next(clients)

        def read(name):
            def get_iopub_msg(timeout):
                reads.append((name, list(closed)))
                if name == 'first':
                    # A restart replaces the client while this read waits on it.
                    module._replace_client(second)
                    return {}
                raise asyncio.CancelledError
            return get_iopub_msg

        first.get_iopub_msg.side_effect = read('first')
        second.get_iopub_msg.side_effect = read('second')
        saved = module.active_kernel.kernel_client
        self.addCleanup(setattr, module.active_kernel, 'kernel_client', saved)
        module.active_kernel.kernel_client = first
        with patch.object(subscriber, 'get_active_kernel_client', side_effect=get_client), \
                patch('mage_ai.kernels.models.close_client', side_effect=closed.append):
            with self.assertRaises(asyncio.CancelledError):
                await subscriber.get_messages(callback=lambda message: None)

        # The first client was closed only after its read returned, before the next read.
        self.assertEqual(reads, [('first', []), ('second', [first])])
        self.assertFalse(module.active_kernel.output_reader_running)
