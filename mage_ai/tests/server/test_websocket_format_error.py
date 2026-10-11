"""The notebook's traceback hides MageML's frames and keeps the block's, the error and its notes."""
import unittest

from mage_ai.server.websocket_server import WebSocketServer


class FormatErrorTest(unittest.TestCase):
    def test_the_error_and_its_notes_survive(self):
        error = [
            '-' * 75,
            'ModuleNotFoundError                       Traceback (most recent call last)',
            'Cell In[3], line 40\n     38 execute_custom_code()\n',
            'File /app/mage_ai/server/kernels.py:120, in execute_custom_code()\n    120 run()\n',
            'File /app/mage_ai/data_preparation/models/block/__init__.py:2074, in _execute_block\n',
            'File /p/data_loaders/missing_package.py:1\n----> 1 import lightgbm\n',
            "ModuleNotFoundError: No module named 'lightgbm'",
            'Block missing_package (data_loaders/missing_package.py) failed.',
            'lightgbm is not installed in the Python environment MageML runs in.',
        ]

        shown = WebSocketServer.format_error(error, block_uuid='missing_package')

        text = '\n'.join(shown)
        self.assertIn('data_loaders/missing_package.py:1', text)
        self.assertIn("ModuleNotFoundError: No module named 'lightgbm'", text)
        self.assertIn('Block missing_package (data_loaders/missing_package.py) failed.', text)
        self.assertIn('lightgbm is not installed', text)
        # MageML's own frames are hidden.
        self.assertNotIn('kernels.py', text)
        self.assertNotIn('models/block/__init__.py', text)
