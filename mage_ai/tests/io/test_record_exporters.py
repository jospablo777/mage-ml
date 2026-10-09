"""
Exporters that send rows as Python values. pandas 3 stores missing text as NaN, so
these must turn every missing value into None before the rows leave Mage.
"""
from unittest.mock import MagicMock, PropertyMock, patch

import numpy as np
import pandas as pd

from mage_ai.io.base import BaseIO
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
        BaseIO.__init__(client, verbose=False)

        client.upload_dataframe(cursor, frame_with_missing_values(), [], {}, 'schema.table')

        rows = cursor.executemany.call_args[0][1]
        self.assertEqual(rows, [('a', 1.5, 1), (None, None, None)])


class MongoDBExportTest(TestCase):
    def test_missing_values_are_stored_as_null(self):
        """NaN was stored as a BSON double."""
        client = MongoDB.__new__(MongoDB)
        BaseIO.__init__(client, verbose=False)
        client.database = MagicMock()
        client.collection = 'rows'

        client.export(frame_with_missing_values())

        records = client.database['rows'].insert_many.call_args[0][0]
        self.assertEqual(records[1], dict(name=None, score=None, count=None))


NUMERIC = pd.DataFrame({'id': [2**53 + 1, 5], 'score': [1.5, 2.5]})


class InsertRowsTest(TestCase):
    def test_numeric_frames_keep_integers(self):
        """iterrows turned every value of a numeric-only frame into float."""
        from mage_ai.io.export_utils import insert_rows

        rows = insert_rows(NUMERIC)

        self.assertEqual(rows, [(2**53 + 1, 1.5), (5, 2.5)])
        self.assertIs(type(rows[1][0]), int)

    def test_missing_values_and_serialized_objects(self):
        from mage_ai.io.export_utils import insert_rows

        frame = frame_with_missing_values().assign(
            doc=pd.Series([{'a': 1}, None], dtype=object),
            quoted=pd.Series(['"x"', None], dtype='str'),
        )

        rows = insert_rows(
            frame, serialize=lambda v: '{"a": 1}' if isinstance(v, dict) else v,
            strip_quotes=True,
        )

        self.assertEqual(rows, [('a', 1.5, 1, '{"a": 1}', 'x'), (None,) * 5])


class SQLRowExportersTest(TestCase):
    def rows_sent(self, client_class, frame, *args):
        cursor = MagicMock()
        client = client_class.__new__(client_class)
        BaseIO.__init__(client, verbose=False)

        client.upload_dataframe(cursor, frame, *args)

        return cursor.executemany.call_args[0][1]

    def test_mysql(self):
        from mage_ai.io.mysql import MySQL

        rows = self.rows_sent(MySQL, NUMERIC, [], {}, 'db.table')

        self.assertEqual(rows, [(2**53 + 1, 1.5), (5, 2.5)])
        self.assertEqual(
            self.rows_sent(MySQL, frame_with_missing_values(), [], {}, 'db.table')[1],
            (None, None, None),
        )

    def test_mssql(self):
        from mage_ai.io.mssql import MSSQL

        rows = self.rows_sent(MSSQL, NUMERIC, [], {}, 'db.table')

        self.assertEqual(rows, [(2**53 + 1, 1.5), (5, 2.5)])

    def test_trino_with_an_all_null_object_column(self):
        """The serialization check read the first value of an empty column."""
        from mage_ai.io.trino import Trino

        frame = NUMERIC.assign(note=pd.Series([None, None], dtype=object))

        rows = self.rows_sent(Trino, frame, {'id': 'integer', 'score': 'floating',
                                             'note': 'empty'}, 'db.table')

        self.assertEqual(rows, [(2**53 + 1, 1.5, None), (5, 2.5, None)])


class RedshiftExportTest(TestCase):
    def test_numeric_frames_are_written_as_numbers(self):
        """df.values made the integers NumPy floats, which were quoted as text."""
        from mage_ai.io.redshift import Redshift

        client = Redshift.__new__(Redshift)
        BaseIO.__init__(client, verbose=False)
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        client.table_exists = MagicMock(return_value=False)

        with patch.object(Redshift, 'conn', new_callable=PropertyMock, return_value=connection):
            client.export(NUMERIC, schema_name='public', table_name='t', verbose=False)

        insert = [c.args[0] for c in cursor.execute.call_args_list if 'INSERT' in c.args[0]]
        self.assertIn(f'({2**53 + 1}, 1.5)', insert[0])
