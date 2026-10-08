import os
import unittest
import warnings

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


if __name__ == '__main__':
    unittest.main()
