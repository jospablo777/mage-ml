"""Errors that name the failed block and say what to do."""
import os
import sys
import textwrap
import traceback
from pathlib import Path

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.shared import error_hints
from mage_ai.tests.base_test import DBTestCase, TestCase


class HintTest(TestCase):
    def test_a_missing_module_names_the_package_to_install(self):
        error = ModuleNotFoundError("No module named 'sklearn'", name='sklearn')
        hint = error_hints.hint_for(error)
        self.assertIn('scikit-learn is not installed', hint)
        self.assertIn('uv pip install --python', hint)
        self.assertIn('requirements.txt', hint)

    def test_the_install_command_quotes_an_interpreter_path_with_spaces(self):
        executable = sys.executable
        sys.executable = '/opt/My Env/bin/python'
        try:
            hint = error_hints.hint_for(ModuleNotFoundError('x', name='polars_ds'))
        finally:
            sys.executable = executable
        self.assertIn("--python '/opt/My Env/bin/python' polars-ds", hint)

    def test_mageml_modules_get_no_install_hint(self):
        self.assertIsNone(error_hints.hint_for(ModuleNotFoundError('x', name='mage_ai.nope')))

    def test_common_errors_have_hints(self):
        self.assertIn('version', error_hints.hint_for(
            ImportError("cannot import name 'thing' from 'pandas'", name='pandas'),
        ))
        self.assertIn('memory', error_hints.hint_for(MemoryError()))
        self.assertIn('get_repo_path', error_hints.hint_for(
            FileNotFoundError(2, 'No such file or directory', 'data/orders.csv'),
        ))
        self.assertIsNone(error_hints.hint_for(
            FileNotFoundError(2, 'No such file or directory', '/abs/orders.csv'),
        ))

        class OperationalError(Exception):
            pass

        self.assertIn('io_config.yaml', error_hints.hint_for(
            OperationalError('could not connect to server: Connection refused'),
        ))
        self.assertIsNone(error_hints.hint_for(ValueError('bad value')))

    def test_a_block_error_is_annotated_once(self):
        class FakeBlock:
            uuid = 'load'
            file_path = '/project/data_loaders/load.py'
            repo_path = '/project'

        error = ModuleNotFoundError('x', name='sklearn')
        error_hints.annotate_block_error(error, FakeBlock())
        error_hints.annotate_block_error(error, FakeBlock())
        self.assertEqual(error.__notes__[0], 'Block load (data_loaders/load.py) failed.')
        self.assertEqual(len(error.__notes__), 2)


    def test_exit_codes_name_their_usual_cause(self):
        self.assertIn('out of memory', error_hints.describe_exit(-9))
        self.assertIn('out of memory', error_hints.describe_exit(137, 'Rscript'))
        self.assertTrue(error_hints.describe_exit(137, 'Rscript').startswith('Rscript was'))
        self.assertIn('segmentation fault', error_hints.describe_exit(-11))
        self.assertEqual(error_hints.describe_exit(-15), 'the process was stopped (SIGTERM)')
        self.assertEqual(error_hints.describe_exit(-6), 'the process was stopped by SIGABRT')
        self.assertEqual(error_hints.describe_exit(2), 'the process exited with code 2')

class BlockErrorTest(DBTestCase):
    def test_a_failed_block_names_its_file_line_and_the_fix(self):
        pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        block = Block.create(
            f'{pipeline.uuid}_load', 'data_loader', self.repo_path, pipeline=pipeline,
        )
        Path(block.file_path).write_text(textwrap.dedent('''
            @data_loader
            def load(*args, **kwargs):
                import package_that_is_not_installed
                return 1
        '''))
        block = Pipeline.get(pipeline.uuid, repo_path=self.repo_path).get_block(block.uuid)

        # assertRaises drops the traceback, which is what this test reads.
        try:
            block.execute_sync()
        except ModuleNotFoundError as error:
            text = ''.join(traceback.format_exception(error))
        else:
            self.fail('the block did not fail')
        self.assertIn(f'File "{block.file_path}", line 4, in load', text)
        self.assertIn('import package_that_is_not_installed', text)
        relative = os.path.relpath(block.file_path, self.repo_path)
        self.assertIn(f'Block {block.uuid} ({relative}) failed.', text)
        self.assertIn('package-that-is-not-installed is not installed', text)
