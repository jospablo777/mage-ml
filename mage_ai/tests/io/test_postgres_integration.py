"""
Round trip against a real PostgreSQL server.

Everything else under mage_ai/tests/io mocks the driver, so nothing exercised
the export and load path until now. Reads MAGE_TEST_POSTGRES_* and skips when
they are unset, so a plain checkout still runs the suite.

Local run:

    docker run -d --rm --name mage-test-pg -p 5433:5432 \\
      -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test postgres:16-alpine

    MAGE_TEST_POSTGRES_HOST=127.0.0.1 MAGE_TEST_POSTGRES_PORT=5433 \\
      MAGE_TEST_POSTGRES_DBNAME=test MAGE_TEST_POSTGRES_USER=postgres \\
      MAGE_TEST_POSTGRES_PASSWORD=test \\
      uv run pytest mage_ai/tests/io/test_postgres_integration.py

CI sets the same variables from a service container.
"""
import os
import unittest
import uuid

import numpy as np
import pandas as pd

CONNECTION = dict(
    dbname=os.getenv('MAGE_TEST_POSTGRES_DBNAME'),
    host=os.getenv('MAGE_TEST_POSTGRES_HOST'),
    password=os.getenv('MAGE_TEST_POSTGRES_PASSWORD'),
    port=os.getenv('MAGE_TEST_POSTGRES_PORT'),
    user=os.getenv('MAGE_TEST_POSTGRES_USER'),
)
SCHEMA = os.getenv('MAGE_TEST_POSTGRES_SCHEMA', 'public')

SKIP_REASON = 'MAGE_TEST_POSTGRES_* is not set'


@unittest.skipUnless(all(CONNECTION.values()), SKIP_REASON)
class PostgresIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from mage_ai.io.postgres import Postgres

        cls.loader = Postgres(verbose=False, **CONNECTION)
        cls.loader.open()

    @classmethod
    def tearDownClass(cls):
        cls.loader.close()

    def setUp(self):
        # A fresh name per test keeps runs independent when they share a server.
        self.table = 'mage_test_%s' % uuid.uuid4().hex[:12]

    def tearDown(self):
        self.loader.execute('DROP TABLE IF EXISTS %s.%s' % (SCHEMA, self.table))
        self.loader.conn.commit()

    def _export(self, df, **kwargs):
        self.loader.export(
            df,
            SCHEMA,
            self.table,
            if_exists='replace',
            index=False,
            verbose=False,
            **kwargs,
        )
        self.loader.conn.commit()

    def _load(self):
        return self.loader.load(
            'SELECT * FROM %s.%s ORDER BY id' % (SCHEMA, self.table),
            verbose=False,
        )

    def test_round_trip_preserves_values(self):
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'label': ['alpha', 'beta', 'gamma'],
            'amount': [1.5, 2.5, 3.5],
            'active': [True, False, True],
        })

        self._export(df)
        loaded = self._load()

        self.assertEqual(len(loaded), 3)
        self.assertEqual(loaded['id'].tolist(), [1, 2, 3])
        self.assertEqual(loaded['label'].tolist(), ['alpha', 'beta', 'gamma'])
        self.assertEqual(loaded['amount'].tolist(), [1.5, 2.5, 3.5])
        self.assertEqual(loaded['active'].tolist(), [True, False, True])

    def test_reserved_column_names_are_prefixed(self):
        # BaseIO clean_column_name prefixes SQL reserved words with _.
        self._export(pd.DataFrame({'id': [1], 'name': ['alpha'], 'value': ['v']}))

        loaded = self._load()

        self.assertIn('_name', loaded.columns)
        self.assertNotIn('name', loaded.columns)
        self.assertEqual(loaded['_name'].tolist(), ['alpha'])

    def test_string_columns_survive_the_pandas_3_dtype(self):
        # pandas 3 gives these columns a StringDtype. The type mapping that
        # picks a PostgreSQL column type reads the dtype.
        df = pd.DataFrame({'id': [1, 2], 'label': ['first', 'second']})
        self.assertNotEqual(df['label'].dtype, object)

        self._export(df)

        self.assertEqual(self._load()['label'].tolist(), ['first', 'second'])

    def test_nulls_round_trip(self):
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'label': ['alpha', None, 'gamma'],
            'amount': [1.5, np.nan, 3.5],
        })

        self._export(df)
        loaded = self._load()

        self.assertTrue(pd.isna(loaded.loc[1, 'label']))
        self.assertTrue(pd.isna(loaded.loc[1, 'amount']))
        self.assertEqual(loaded.loc[0, 'label'], 'alpha')

    def test_datetime_column_round_trip(self):
        df = pd.DataFrame({
            'id': [1, 2],
            'joined': pd.to_datetime(['2024-01-15 10:30:00', '2024-06-01 08:00:00']),
        })

        self._export(df)
        loaded = self._load()

        self.assertEqual(
            [str(v) for v in loaded['joined'].tolist()],
            ['2024-01-15 10:30:00', '2024-06-01 08:00:00'],
        )

    def test_table_exists_reports_correctly(self):
        self.assertFalse(self.loader.table_exists(SCHEMA, self.table))

        self._export(pd.DataFrame({'id': [1]}))

        self.assertTrue(self.loader.table_exists(SCHEMA, self.table))

    def test_if_exists_replace_overwrites_rows(self):
        self._export(pd.DataFrame({'id': [1, 2, 3]}))
        self._export(pd.DataFrame({'id': [9]}))

        self.assertEqual(self._load()['id'].tolist(), [9])

    def test_if_exists_append_adds_rows(self):
        self._export(pd.DataFrame({'id': [1, 2]}))

        self.loader.export(
            pd.DataFrame({'id': [3]}),
            SCHEMA,
            self.table,
            if_exists='append',
            index=False,
            verbose=False,
        )
        self.loader.conn.commit()

        self.assertEqual(self._load()['id'].tolist(), [1, 2, 3])

    def test_execute_runs_raw_sql(self):
        self._export(pd.DataFrame({'id': [1, 2, 3]}))

        self.loader.execute('DELETE FROM %s.%s WHERE id = 2' % (SCHEMA, self.table))
        self.loader.conn.commit()

        self.assertEqual(self._load()['id'].tolist(), [1, 3])

    def test_load_returns_empty_frame_for_no_rows(self):
        self._export(pd.DataFrame({'id': [1]}))

        empty = self.loader.load(
            'SELECT * FROM %s.%s WHERE id = -1' % (SCHEMA, self.table),
            verbose=False,
        )

        self.assertEqual(len(empty), 0)


if __name__ == '__main__':
    unittest.main()
