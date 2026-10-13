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
