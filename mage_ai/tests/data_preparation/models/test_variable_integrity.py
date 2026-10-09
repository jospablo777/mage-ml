import os
import shutil
from unittest.mock import patch

import pandas as pd
import polars as pl
from pandas.testing import assert_frame_equal

from mage_ai.data_preparation.models.variable import Variable
from mage_ai.data_preparation.storage.local_storage import LocalStorage
from mage_ai.data_preparation.variable_manager import VariableManager
from mage_ai.tests.base_test import DBTestCase
from mage_ai.tests.factory import create_pipeline

NO_DATA_MANAGER = (
    patch('mage_ai.data.models.manager.DataManager.writeable', return_value=False),
    patch('mage_ai.data.models.manager.DataManager.readable', return_value=False),
)


class VariableReadErrorTest(DBTestCase):
    """
    Reads that execution makes with raise_exception=True raise when an output's files are
    missing. A missing JSON output, or a missing output directory, used to read as {}, so
    the next block ran on an empty dict.
    """

    def setUp(self):
        super().setUp()
        for p in NO_DATA_MANAGER:
            p.start()
            self.addCleanup(p.stop)
        self.pipeline = create_pipeline(self.faker.unique.name(), self.repo_path)

    def variable(self, uuid: str) -> Variable:
        return Variable(uuid, self.pipeline.dir_path, 'block')

    def test_missing_json_output_raises(self):
        for uuid, value in (('d', {'a': 1}), ('l', [1, 2]), ('n', 5), ('t', 'text')):
            variable = self.variable(uuid)
            variable.write_data(value)
            for name in os.listdir(variable.variable_path):
                if name.endswith('data.json'):
                    os.remove(os.path.join(variable.variable_path, name))

            with self.assertRaisesRegex(Exception, 'Failed to read', msg=uuid):
                self.variable(uuid).read_data(raise_exception=True)

    def test_missing_output_directory_raises(self):
        for uuid, value in (
            ('frame', pd.DataFrame({'a': [1]})),
            ('polars', pl.DataFrame({'a': [1]})),
            ('dict', {'a': 1}),
        ):
            variable = self.variable(uuid)
            variable.write_data(value)
            shutil.rmtree(variable.variable_path)

            with self.assertRaisesRegex(Exception, 'Failed to read', msg=uuid):
                self.variable(uuid).read_data(raise_exception=True)

    def test_reads_without_raise_exception_keep_their_default(self):
        variable = self.variable('gone')
        variable.write_data({'a': 1})
        shutil.rmtree(variable.variable_path)

        self.assertEqual(self.variable('gone').read_data(), {})

    def test_read_json_file_keeps_a_falsy_default(self):
        storage = LocalStorage()
        missing = os.path.join(self.repo_path, 'missing.json')

        self.assertEqual(storage.read_json_file(missing, default_value=[]), [])
        self.assertEqual(storage.read_json_file(missing), {})
        with self.assertRaises(FileNotFoundError):
            storage.read_json_file(missing, raise_exception=True)


class VariableAtomicWriteTest(DBTestCase):
    """
    A write that fails partway leaves the previous output as it was. Each file used to be
    written in place, so a failure left a truncated file, or new files next to old ones.
    """

    def setUp(self):
        super().setUp()
        for p in NO_DATA_MANAGER:
            p.start()
            self.addCleanup(p.stop)
        self.pipeline = create_pipeline(self.faker.unique.name(), self.repo_path)

    def variable(self, uuid: str = 'output_0') -> Variable:
        return Variable(uuid, self.pipeline.dir_path, 'block')

    def entries(self):
        return sorted(os.listdir(self.variable().variable_dir_path))

    def test_a_failed_write_keeps_the_previous_output(self):
        before = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
        self.variable().write_data(before)
        contents_before = self.contents()

        # The column types are written before the data file fails.
        with patch.object(LocalStorage, 'write_parquet', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                self.variable().write_data(pd.DataFrame({'c': [1.5]}))

        assert_frame_equal(self.variable().read_data(raise_exception=True), before)
        self.assertEqual(self.contents(), contents_before)
        self.assertEqual(self.entries(), ['output_0'])

    def contents(self):
        path = self.variable().variable_path
        return {
            name: open(os.path.join(path, name), 'rb').read()
            for name in sorted(os.listdir(path))
            if name != 'resource_usage.json'
        }

    def test_a_failed_write_of_a_new_output_leaves_nothing(self):
        with patch.object(LocalStorage, 'write_json_file', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                self.variable('output_1').write_data({'a': 1})

        self.assertEqual(self.entries(), [])

    def test_a_rewrite_replaces_every_file(self):
        self.variable().write_data(pd.DataFrame({'a': [1]}))
        self.variable().write_data(pl.DataFrame({'b': ['x']}))

        back = self.variable().read_data(raise_exception=True)

        self.assertIsInstance(back, pl.DataFrame)
        self.assertEqual(back.to_dicts(), [{'b': 'x'}])
        # Files of the pandas output, such as its column types, are gone.
        self.assertNotIn('data_column_types.json', os.listdir(self.variable().variable_path))
        self.assertEqual(self.entries(), ['output_0'])

    def test_staging_directories_are_not_listed_as_variables(self):
        manager = VariableManager(repo_path=self.repo_path)
        variable = self.variable()
        variable.write_data({'a': 1})
        os.makedirs(os.path.join(variable.variable_dir_path, '.output_0.abcd1234.staging'))

        self.assertEqual(
            manager.get_variables_by_block(self.pipeline.uuid, 'block'),
            ['output_0'],
        )

    def test_file_writes_replace_the_file_whole(self):
        storage = LocalStorage()
        path = os.path.join(self.repo_path, 'atomic', 'data.parquet')
        storage.write_parquet(pd.DataFrame({'a': [1]}), path)

        def write_part_and_fail(frame, target, *args, **kwargs):
            with open(target, 'wb') as file:
                file.write(b'PAR1 partial')
            raise OSError('killed')

        with patch.object(pd.DataFrame, 'to_parquet', write_part_and_fail):
            with self.assertRaises(OSError):
                storage.write_parquet(pd.DataFrame({'a': [2]}), path)

        assert_frame_equal(storage.read_parquet(path), pd.DataFrame({'a': [1]}))
        self.assertEqual(os.listdir(os.path.dirname(path)), ['data.parquet'])

    def test_objects_inside_dicts_survive_the_swap_and_a_moved_project(self):
        """
        Objects inside dicts, such as a Timestamp, are saved to their own files. Their paths
        were stored absolute, so they pointed into the staging directory after a swap, and
        anywhere else after the project moved.
        """
        value = {'last_id': 1, 'at': pd.Timestamp('2026-07-20 18:15:34+00:00')}
        manager = VariableManager(repo_path=self.repo_path)
        manager.add_variable(self.pipeline.uuid, 'block', 'output_0', value)
        manager.add_variable(self.pipeline.uuid, 'block', 'output_0', value)

        self.assertEqual(self.variable().read_data(raise_exception=True), value)

        moved = os.path.join(os.path.dirname(self.pipeline.dir_path), 'moved')
        shutil.copytree(self.pipeline.dir_path, moved)
        shutil.rmtree(self.pipeline.dir_path)
        self.assertEqual(
            Variable('output_0', moved, 'block').read_data(raise_exception=True), value,
        )

    def test_resource_usage_describes_the_final_directory(self):
        variable = self.variable()
        variable.write_data({'a': 1})

        usage = open(variable.resource_usage_path()).read()

        self.assertNotIn('.staging', usage)
        self.assertIn(variable.variable_path, usage)

    def test_dynamic_child_outputs_are_staged_next_to_their_directory(self):
        Variable('output_0', self.pipeline.dir_path, 'block').write_data([1, 2])
        child = Variable('output_0/0', self.pipeline.dir_path, 'block')

        child.write_data({'a': 1})

        self.assertEqual(child.read_data(raise_exception=True), {'a': 1})
        self.assertEqual(Variable('output_0', self.pipeline.dir_path, 'block').read_data(), [1, 2])
