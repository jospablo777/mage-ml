"""
Exporters that send rows as Python values. pandas 3 stores missing text as NaN, so
these must turn every missing value into None before the rows leave Mage.
"""
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from mage_ai.io.mongodb import MongoDB
from mage_ai.io.oracledb import OracleDB
from mage_ai.tests.base_test import TestCase


def frame_with_missing_values() -> pd.DataFrame:
    return pd.DataFrame({
        'name': pd.Series(['a', None], dtype='str'),
        'score': [1.5, np.nan],
        'count': pd.array([1, None], dtype='Int64'),
    })


class OracleUploadTest(TestCase):
    def test_missing_values_bind_as_null(self):
        """fillna('') raised TypeError for float and Int64 columns under pandas 3."""
        cursor = MagicMock()
        client = OracleDB.__new__(OracleDB)

        client.upload_dataframe(cursor, frame_with_missing_values(), [], {}, 'schema.table')

        rows = cursor.executemany.call_args[0][1]
        self.assertEqual(rows, [('a', 1.5, 1), (None, None, None)])


class MongoDBExportTest(TestCase):
    def test_missing_values_are_stored_as_null(self):
        """NaN was stored as a BSON double."""
        client = MongoDB.__new__(MongoDB)
        client.database = MagicMock()
        client.collection = 'rows'

        client.export(frame_with_missing_values())

        records = client.database['rows'].insert_many.call_args[0][0]
        self.assertEqual(records[1], dict(name=None, score=None, count=None))
