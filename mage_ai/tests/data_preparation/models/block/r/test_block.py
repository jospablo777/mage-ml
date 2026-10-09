from unittest.mock import patch

from mage_ai.data_preparation.models.block.r import RBlock, RRun
from mage_ai.data_preparation.models.constants import BlockLanguage, BlockType
from mage_ai.tests.base_test import TestCase

R_CODE = '#* @transformer\nf <- function(df_1) df_1\n'


def r_block():
    return RBlock('r_block', 'r_block', BlockType.TRANSFORMER, language=BlockLanguage.R)


class RBlockTestsTest(TestCase):
    def execute(self, block, tests):
        run = RRun(outputs=[1], tests=tests)
        with patch('mage_ai.data_preparation.models.block.r.execute_r_code', return_value=run):
            return block._execute_block({}, custom_code=R_CODE, global_vars={}, input_vars=[])

    def test_passing_r_tests_are_reported(self):
        block = r_block()
        self.assertEqual(self.execute(block, [
            {'name': 'has_rows', 'passed': True, 'message': None},
        ]), [1])

        with patch('builtins.print') as printed:
            block.run_tests(outputs=[1], update_tests=True)

        self.assertIn('1/1 tests passed.', [c.args[0] for c in printed.call_args_list if c.args])

    def test_failing_r_tests_fail_the_block(self):
        block = r_block()
        self.execute(block, [
            {'name': 'has_rows', 'passed': True, 'message': None},
            {'name': 'ids_are_unique', 'passed': False, 'message': 'ids must be unique'},
        ])

        with patch('builtins.print') as printed:
            with self.assertRaisesRegex(Exception, 'Failed to pass tests for block r_block'):
                block.run_tests(outputs=[1])

        output = '\n'.join(str(c.args[0]) for c in printed.call_args_list if c.args)
        self.assertIn('FAIL: ids_are_unique (block: r_block)', output)
        self.assertIn('AssertionError: ids must be unique', output)
        self.assertIn('1/2 tests passed.', output)

    def test_r_code_is_never_run_as_python(self):
        """run_tests with update_tests ran the block's code with exec."""
        block = r_block()
        self.execute(block, [])

        with patch('builtins.exec') as executed:
            block.run_tests(outputs=[1], update_tests=True, custom_code=R_CODE)

        executed.assert_not_called()
