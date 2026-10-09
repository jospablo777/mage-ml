import pandas as pd
import polars as pl

from mage_ai.io.base import BaseIO
from mage_ai.io.sql import BaseSQL
from mage_ai.tests.base_test import TestCase


class CapturingSQL(BaseSQL):
    """Stops at the upload and keeps the frame the exporter built."""

    def __init__(self):
        BaseIO.__init__(self, verbose=False)
        self.uploaded = None

    def default_schema(self) -> str:
        return 'public'

    def upload_dataframe_fast(self, df, *args, **kwargs):
        self.uploaded = df


class SQLExportTest(TestCase):
    def test_polars_frames_are_exported(self):
        """clean_df_for_export called DataFrame.copy, which Polars frames lack."""
        client = CapturingSQL()

        client.export(pl.DataFrame({'id': [1, 2], 'name': ['a', None]}), table_name='t')

        self.assertIsInstance(client.uploaded, pd.DataFrame)
        self.assertEqual(client.uploaded['id'].tolist(), [1, 2])

    def test_lazy_frames_are_collected(self):
        client = CapturingSQL()

        client.export(pl.LazyFrame({'id': [1]}), table_name='t')

        self.assertEqual(client.uploaded['id'].tolist(), [1])

    def test_columns_that_clean_to_the_same_name_raise(self):
        """One of the two columns was silently dropped."""
        frame = pd.DataFrame({'Total Sales': [1], 'total_sales': [2]})

        with self.assertRaisesRegex(ValueError, 'same name after cleaning'):
            CapturingSQL().export(frame, table_name='t')

    def test_caller_frame_is_not_modified(self):
        frame = pd.DataFrame({'Total Sales': [1.5]})

        CapturingSQL().export(frame, table_name='t')

        self.assertEqual(frame.columns.tolist(), ['Total Sales'])
