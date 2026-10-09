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
