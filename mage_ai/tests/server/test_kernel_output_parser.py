"""Printed output reaches the notebook whole unless MAX_PRINT_OUTPUT_LINES caps it."""
import unittest
from unittest.mock import patch

from mage_ai.server import kernel_output_parser


def stdout_message(lines):
    return dict(
        content=dict(name='stdout', text='\n'.join(f'line {i}' for i in range(lines))),
        header=dict(msg_type='stream'),
        parent_header=dict(msg_id='1'),
        metadata={},
    )


class PrintedOutputTest(unittest.TestCase):
    def test_long_output_is_not_cut_by_default(self):
        with patch.object(kernel_output_parser, 'MAX_PRINT_OUTPUT_LINES', None):
            parsed = kernel_output_parser.parse_output_message(stdout_message(5000))
        self.assertEqual(len(parsed['data']), 5000)
        self.assertEqual(parsed['data'][-1], 'line 4999')

    def test_a_set_cap_still_cuts(self):
        with patch.object(kernel_output_parser, 'MAX_PRINT_OUTPUT_LINES', 10):
            parsed = kernel_output_parser.parse_output_message(stdout_message(50))
        self.assertEqual(parsed['data'][-1], '... (output truncated)')
        self.assertEqual(len(parsed['data']), 11)
