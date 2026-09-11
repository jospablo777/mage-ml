import multiprocessing
import queue
import runpy
import unittest
from pathlib import Path
from unittest.mock import patch

import mage_ai.shared.queues


class QueueTest(unittest.TestCase):
    def load_queues(self, magic):
        with patch('mage_ai.settings.server.KERNEL_MAGIC', magic):
            return runpy.run_path(str(Path(mage_ai.shared.queues.__file__)))

    def test_default_kernel_uses_standard_queue(self):
        exports = self.load_queues(False)
        self.assertIs(exports['Empty'], queue.Empty)
        self.assertIs(exports['Queue'], multiprocessing.Queue)

    def test_magic_kernel_without_faster_fifo_uses_standard_queue(self):
        with patch.dict('sys.modules', {'faster_fifo': None}):
            exports = self.load_queues(True)
        self.assertIs(exports['Empty'], queue.Empty)
        self.assertIs(exports['Queue'], multiprocessing.Queue)
