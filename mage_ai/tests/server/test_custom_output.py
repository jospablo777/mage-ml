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
