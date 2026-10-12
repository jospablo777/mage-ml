from unittest.mock import call, patch

from mage_ai.shared.retry import retry
from mage_ai.tests.base_test import TestCase


class RetryTests(TestCase):
    @patch('time.sleep')
    def test_retry(self, mock_sleep):
        attempts = []

        @retry(retries=2, max_delay=40)
        def test_func():
            attempts.append(1)
            raise ValueError('error')

        with self.assertRaisesRegex(ValueError, 'error'):
            test_func()
        # Three attempts, with the delay doubling between them. The check used to sit
        # inside the assertRaises block, after the raise, so it never ran.
        self.assertEqual(len(attempts), 3)
        self.assertEqual(mock_sleep.call_args_list, [call(5), call(10)])
