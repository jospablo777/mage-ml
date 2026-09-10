import os
import unittest
from unittest.mock import patch

from mage_ai.shared.environments import is_debug


class EnvironmentsTest(unittest.TestCase):
    def test_debug_flag(self):
        for value in ['', '0', 'false', 'release', '2']:
            with self.subTest(value=value), patch.dict(os.environ, DEBUG=value):
                self.assertFalse(is_debug())
        for value in ['1', 'true', 'YES', 'on']:
            with self.subTest(value=value), patch.dict(os.environ, DEBUG=value):
                self.assertTrue(is_debug())
