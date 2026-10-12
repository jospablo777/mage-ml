import os
import subprocess
import sys
import textwrap
import unittest
import warnings
from unittest.mock import patch

from mage_ai.kernels.default.models import KernelProcess
from mage_ai.kernels.default.utils import get_process_info


class GetProcessInfoTest(unittest.TestCase):
    def test_reads_the_current_process_without_deprecation_warnings(self):
        # psutil 6 deprecated Process.connections() in favor of net_connections().
        with warnings.catch_warnings():
            warnings.simplefilter('error', DeprecationWarning)
            info = get_process_info(os.getpid())

        self.assertEqual(info['pid'], os.getpid())
        self.assertIsInstance(info['connections'], list)
        self.assertGreater(info['memory_info'].rss, 0)

    def test_returns_none_for_a_missing_process(self):
        self.assertIsNone(get_process_info(2**22 + 12345))



class KernelActiveTest(unittest.TestCase):
    def test_a_kernel_that_exited_is_inactive_and_leaves_nothing_for_the_collector(self):
        """
        The check left its client to the garbage collector, whose Context.destroy() waited
        without end for the message the exited kernel never took: the web server froze.
        It also called such a kernel active.
        """
        script = textwrap.dedent("""
            import gc, json, os, socket, tempfile
            from mage_ai.kernels.default.utils import is_kernel_process_active
            ports = []
            for _ in range(5):
                with socket.socket() as s:
                    s.bind(('127.0.0.1', 0))
                    ports.append(s.getsockname()[1])
            info = dict(
                shell_port=ports[0], iopub_port=ports[1], stdin_port=ports[2],
                control_port=ports[3], hb_port=ports[4], ip='127.0.0.1', key='',
                transport='tcp', signature_scheme='hmac-sha256', kernel_name='',
            )
            path = os.path.join(tempfile.mkdtemp(), 'kernel.json')
            with open(path, 'w') as f:
                json.dump(info, f)
            print(is_kernel_process_active(os.getpid(), path))
            gc.collect()
            print('collected')
        """)
        result = subprocess.run(
            [sys.executable, '-c', script], capture_output=True, text=True, timeout=60,
        )
        self.assertEqual(result.stdout.split(), ['False', 'collected'], result.stderr)

    def test_a_kernel_that_answers_is_active(self):
        script = textwrap.dedent("""
            import gc
            from jupyter_client import KernelManager
            from mage_ai.kernels.default.utils import is_kernel_process_active
            manager = KernelManager(kernel_name='python3')
            manager.start_kernel()
            try:
                client = manager.client()
                client.start_channels()
                client.wait_for_ready(timeout=60)
                client.stop_channels()
                print(is_kernel_process_active(manager.provisioner.pid, manager.connection_file))
            finally:
                manager.shutdown_kernel(now=True)
            gc.collect()
            print('collected')
        """)
        result = subprocess.run(
            [sys.executable, '-c', script], capture_output=True, text=True, timeout=120,
        )
        self.assertEqual(result.stdout.split(), ['True', 'collected'], result.stderr[-2000:])


class TerminateInactiveTest(unittest.TestCase):
    def test_only_kernels_this_server_started_are_stopped(self):
        """
        The cleanup stopped every kernel on the machine that it took for inactive, such as
        the notebooks of an editor.
        """
        mine = dict(pid=101, ppid=os.getpid(), active=False)
        others = dict(pid=102, ppid=os.getppid(), active=False)
        answering = dict(pid=103, ppid=os.getpid(), active=True)

        with patch('mage_ai.kernels.default.models.terminate_process', return_value=True) as stop:
            KernelProcess.terminate_inactive([mine, others, answering])

        self.assertEqual([c.args[0] for c in stop.call_args_list], [101])


if __name__ == '__main__':
    unittest.main()
