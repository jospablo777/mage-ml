import tempfile
import unittest

from mage_ai.server.kernels import kernel_managers


class KernelExecutionTest(unittest.TestCase):
    def test_kernel_messages_are_signed(self):
        for name, manager in kernel_managers.items():
            with self.subTest(kernel=name):
                self.assertTrue(manager.session.key)

    def test_python_kernel_executes_with_message_signing(self):
        from jupyter_client import KernelManager

        with tempfile.TemporaryDirectory() as directory:
            manager = KernelManager(connection_file=f'{directory}/kernel.json')
            try:
                manager.start_kernel(cwd=directory)
                client = manager.blocking_client()
                client.start_channels()
                try:
                    client.wait_for_ready(timeout=30)
                    response = client.execute_interactive(
                        'assert sum([1, 2, 3]) == 6', timeout=30,
                    )
                    self.assertEqual(response['content']['status'], 'ok')
                    self.assertTrue(client.session.key)
                finally:
                    client.stop_channels()
            finally:
                if manager.has_kernel:
                    manager.shutdown_kernel(now=True)
