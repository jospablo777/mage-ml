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


class RConditionalBlockTest(TestCase):
    def test_an_r_conditional_block_decides_with_the_r_condition(self):
        from mage_ai.data_preparation.models.block import Block
        from mage_ai.data_preparation.models.block.r import (
            RBlockError,
            RConditionalBlock,
        )
        from mage_ai.data_preparation.models.pipeline import Pipeline

        pipeline = Pipeline.create('r condition', repo_path=self.repo_path)
        condition = Block.create(
            'enough_rows', BlockType.CONDITIONAL, self.repo_path,
            language=BlockLanguage.R, pipeline=pipeline,
        )
        self.assertIsInstance(condition, RConditionalBlock)
        self.assertTrue(condition.file_path.endswith('.r'))
        self.assertIn('#* @condition', open(condition.file_path).read())

        target = 'mage_ai.data_preparation.models.block.r.execute_r_code'
        with patch(target, return_value=RRun(outputs=[True])) as run:
            self.assertTrue(condition.execute_conditional(None, global_vars={}))
        self.assertEqual(run.call_args[0][0], BlockType.CONDITIONAL)
        with patch(target, return_value=RRun(outputs=[False])):
            self.assertFalse(condition.execute_conditional(None, global_vars={}))
        with patch(target, return_value=RRun(outputs=[])):
            with self.assertRaisesRegex(RBlockError, 'returned no decision'):
                condition.execute_conditional(None, global_vars={})
