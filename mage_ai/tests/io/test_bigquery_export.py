from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from mage_ai.io.base import BaseIO
from mage_ai.io.bigquery import BigQuery
from mage_ai.tests.base_test import TestCase


class BigQueryWriteTableTest(TestCase):
    def test_nulls_in_repeated_and_record_columns_and_the_caller_frame(self):
        """
        Missing ARRAY and STRUCT values become [] and {}. The caller's frame used to be
        changed in place, and .loc assignment of lists into a float column raised.
        """
        client = BigQuery.__new__(BigQuery)
        BaseIO.__init__(client, verbose=False)
        client.client = MagicMock()
        client.get_column_types = MagicMock(return_value={
            'my col': 'ARRAY<STRING>', 'record': 'STRUCT<a INT64>',
            'items': 'ARRAY<STRUCT<a INT64>>',
        })
        frame = pd.DataFrame({
            'my col': [np.nan, np.nan],
            'record': pd.Series([{'a': 1}, None], dtype=object),
            'items': pd.Series([None, [{'a': 2}]], dtype=object),
        })
        original = frame.copy()

        client._BigQuery__write_table(frame, 'project.dataset.table')

        uploaded = client.client.load_table_from_dataframe.call_args[0][0]
        self.assertEqual(uploaded.columns.tolist(), ['my_col', 'record', 'items'])
        self.assertEqual(uploaded['my_col'].tolist(), [[], []])
        self.assertEqual(uploaded['record'].tolist(), [{'a': 1}, {}])
        self.assertEqual(uploaded['items'].tolist(), [[{}], [{'a': 2}]])
        pd.testing.assert_frame_equal(frame, original)
