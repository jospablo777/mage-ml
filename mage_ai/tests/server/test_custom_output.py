import ast
import os
from unittest.mock import MagicMock, patch

from mage_ai.server.utils import output_display
from mage_ai.tests.base_test import TestCase

CUSTOM_OUTPUT_PATH = os.path.join(
    os.path.dirname(output_display.__file__),
    'custom_output.py',
)

interpolate_code_content = getattr(output_display, '__interpolate_code_content')


def read_template() -> str:
    with open(CUSTOM_OUTPUT_PATH, 'r') as file:
        return file.read()


class CustomOutputTemplateTests(TestCase):
    def test_template_is_valid_python(self):
        ast.parse(read_template())

    def test_template_does_not_import_setting_with_copy_warning_directly(self):
        """
        pandas 3.0 removed SettingWithCopyWarning, so importing it unconditionally raises
        an ImportError while rendering a block's output.
        """
        template = read_template()

        self.assertNotIn('SettingWithCopyWarning', template)
        self.assertNotIn('pd.__version__', template)
        self.assertIn('ignore_setting_with_copy_warning()', template)

    def test_interpolated_code_is_valid_python(self):
        code = interpolate_code_content(
            'custom_output.py',
            [
                ('block_uuid', "'test_block'"),
                ('extension_uuid', 'None'),
                ('has_reduce_output', False),
                ('is_dynamic', False),
                ('is_dynamic_child', False),
                ('is_print_statement', False),
                ('last_line', 'df'),
                ('pipeline_uuid', "'test_pipeline'"),
                ('repo_path', "'/home/src/test'"),
                ('widget', False),
            ],
        )

        ast.parse(code)
        self.assertIn('ignore_setting_with_copy_warning()', code)

    def test_add_internal_output_info_produces_valid_python(self):
        block = MagicMock()
        block.uuid = 'test_block'
        block.pipeline.uuid = 'test_pipeline'
        block.pipeline.repo_path = '/home/src/test'

        with patch.multiple(
            output_display,
            has_reduce_output_from_upstreams=MagicMock(return_value=False),
            is_dynamic_block=MagicMock(return_value=False),
            is_dynamic_block_child=MagicMock(return_value=False),
        ):
            code = output_display.add_internal_output_info(block, 'df = load()\ndf')

        ast.parse(code)
        self.assertIn('ignore_setting_with_copy_warning()', code)

    def test_warning_suppression_runs_against_the_installed_pandas(self):
        from mage_ai.shared.pandas_utils import ignore_setting_with_copy_warning

        # Fails with ImportError on pandas 3.x before the fix.
        ignore_setting_with_copy_warning()


R_BLOCK = '''library(dplyr)

#* @data_loader
load_orders <- function(...) {
  # One row per order.
  read_sql("SELECT * FROM orders")
}
'''

RUST_BLOCK = '''use mage::prelude::*;

#[derive(Debug, Clone)]
struct Row {
    id: i64,
}

fn transform(data: LazyFrame) -> Result<LazyFrame> {
    Ok(data)
}
'''


class RemoveCommentsTests(TestCase):
    def test_python_comment_lines_are_removed(self):
        code = '# a comment\nx = 1  # stays\n    # indented comment\ny = 2'
        self.assertEqual(
            output_display.remove_comments(code.split('\n')),
            ['x = 1  # stays', 'y = 2'],
        )

    def test_lines_inside_strings_that_start_with_hash_are_kept(self):
        code = "text = '''\n# a heading\n#* @data_loader\n#[derive(Debug)]\n'''\n# gone\ntext"
        self.assertEqual(
            output_display.remove_comments(code.split('\n')),
            ["text = '''", '# a heading', '#* @data_loader', '#[derive(Debug)]', "'''", 'text'],
        )

    def test_code_that_does_not_tokenize_drops_comment_lines_as_before(self):
        code = "x = '''\n# open string"
        self.assertEqual(output_display.remove_comments(code.split('\n')), ["x = '''"])

    def _executed_code(self, block_code: str) -> str:
        block = MagicMock()
        block.uuid = 'test_block'
        block.pipeline.uuid = 'test_pipeline'
        block.pipeline.repo_path = '/home/src/test'
        script = output_display.add_execution_code(
            'test_pipeline', 'test_block', block_code, 'dict()', '/home/src/test',
        )
        with patch.multiple(
            output_display,
            has_reduce_output_from_upstreams=MagicMock(return_value=False),
            is_dynamic_block=MagicMock(return_value=False),
            is_dynamic_block_child=MagicMock(return_value=False),
        ):
            return output_display.add_internal_output_info(block, script)

    def test_r_annotations_reach_the_kernel(self):
        """
        The notebook ran R blocks without their `#* @data_loader` line, so R blocks that
        name their function freely failed with "The block has no function".
        """
        code = self._executed_code(R_BLOCK)
        ast.parse(code)
        self.assertIn('#* @data_loader\nload_orders <- function(...) {', code)
        self.assertIn('  # One row per order.', code)

    def test_rust_attributes_reach_the_kernel(self):
        code = self._executed_code(RUST_BLOCK)
        ast.parse(code)
        self.assertIn('#[derive(Debug, Clone)]\nstruct Row {', code)
