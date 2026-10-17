import json
import os
import tempfile

import pandas as pd
import polars as pl
import pyarrow.parquet as pq

from mage_ai.io.file import FileIO
from mage_ai.tests.base_test import TestCase

MICROSECONDS = pd.Timestamp('2024-01-01 12:00:00.123456')


class FileExportTest(TestCase):
    def path(self, name: str) -> str:
        return os.path.join(tempfile.mkdtemp(), name)

    def test_parquet_keeps_microseconds(self):
        """The default coerced timestamps to milliseconds, which dropped .000456."""
        path = self.path('out.parquet')

        FileIO(verbose=False).export(pd.DataFrame({'at': [MICROSECONDS]}), path)

        self.assertEqual(pq.read_table(path).column('at').to_pylist()[0], MICROSECONDS)

    def test_json_writes_iso_dates(self):
        """Epoch milliseconds, the old default, raise a FutureWarning in pandas 3."""
        path = self.path('out.json')

        FileIO(verbose=False).export(pd.DataFrame({'at': [MICROSECONDS]}), path)

        with open(path) as f:
            self.assertEqual(json.load(f)['at']['0'], '2024-01-01T12:00:00.123456')

    def test_polars_frame_to_xml(self):
        """Polars has no write_xml, so this raised AttributeError."""
        path = self.path('out.xml')

        FileIO(verbose=False).export(pl.DataFrame({'id': [1, 2]}), path)

        with open(path) as f:
            self.assertIn('<id>2</id>', f.read())


class FileDirectoriesLoadTest(TestCase):
    def directory(self) -> str:
        directory = tempfile.mkdtemp()
        pd.DataFrame({'id': [1, 2]}).to_csv(os.path.join(directory, 'a.csv'), index=False)
        pd.DataFrame({'id': [3]}).to_parquet(os.path.join(directory, 'b.parquet'))
        open(os.path.join(directory, '_SUCCESS'), 'w').close()
        open(os.path.join(directory, '.part-0.crc'), 'w').close()
        return directory

    def test_each_file_is_read_in_its_own_format(self):
        """The first file's format was used for every file (upstream #6160)."""
        df = FileIO(verbose=False).load(file_directories=[self.directory()])

        self.assertEqual(df['id'].tolist(), [1, 2, 3])

    def test_polars_frames_are_concatenated_with_polars(self):
        df = FileIO(verbose=False).load(file_directories=[self.directory()], polars=True)

        self.assertIsInstance(df, pl.DataFrame)
        self.assertEqual(df['id'].to_list(), [1, 2, 3])

    def test_an_unknown_file_is_named(self):
        directory = self.directory()
        open(os.path.join(directory, 'notes.txt'), 'w').close()

        with self.assertRaisesRegex(ValueError, 'notes.txt'):
            FileIO(verbose=False).load(file_directories=[directory])

    def test_an_empty_directory_says_so(self):
        with self.assertRaisesRegex(FileNotFoundError, 'No data files'):
            FileIO(verbose=False).load(file_directories=[tempfile.mkdtemp()])
