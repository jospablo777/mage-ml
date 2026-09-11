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
import unittest.mock
import uuid

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

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


    def test_feature_batch_round_trip_with_conflict_handling(self):
        """
        The shape an incremental feature job exports: a big integer key, a UTC
        timestamp at microsecond resolution, boolean indicators widened to integers,
        and a unique constraint that makes a rerun a no-op.
        """
        from mage_ai.io.constants import UNIQUE_CONFLICT_METHOD_IGNORE

        opened_at = pd.to_datetime(
            ['2026-07-20 18:15:34.258586+00:00'] * 3,
            utc=True,
        )
        df = pd.DataFrame({
            'id': pd.Series([345038, 345039, 345040], dtype='int64'),
            'opened_at': opened_at,
            'flag': pd.Series([True, False, True], dtype='bool').astype('int16'),
            'total_order': pd.Series([0, 1, 0], dtype='int64'),
        })

        for _ in range(2):
            self.loader.export(
                df,
                SCHEMA,
                self.table,
                if_exists='append',
                index=False,
                verbose=False,
                unique_constraints=['id', 'opened_at'],
                unique_conflict_method=UNIQUE_CONFLICT_METHOD_IGNORE,
            )
            self.loader.conn.commit()

        loaded = self._load()

        self.assertEqual(len(loaded), 3, 'the second export should have been ignored')
        self.assertEqual(loaded['id'].tolist(), [345038, 345039, 345040])
        self.assertEqual(loaded['flag'].tolist(), [1, 0, 1])
        self.assertEqual(
            loaded['opened_at'].iloc[0],
            pd.Timestamp('2026-07-20 18:15:34.258586+00:00'),
        )

    def test_microseconds_survive_the_copy_path(self):
        # No unique constraint means the COPY path, which serializes through CSV.
        df = pd.DataFrame({
            'id': [1],
            'seen_at': pd.to_datetime(['2026-07-20 18:15:34.258586+00:00'], utc=True),
        })

        self._export(df)

        self.assertEqual(
            self._load()['seen_at'].iloc[0],
            pd.Timestamp('2026-07-20 18:15:34.258586+00:00'),
        )

    def test_large_keys_do_not_get_a_smallint_column(self):
        self._export(pd.DataFrame({'id': pd.Series([345038, 345039], dtype='int64')}))

        self.assertEqual(self._load()['id'].tolist(), [345038, 345039])

    def test_query_parameters_bind_by_name(self):
        """A cursor dictionary from a previous block is bound as query parameters."""
        self._export(pd.DataFrame({'id': [1, 2, 3]}))

        loaded = self.loader.load(
            'SELECT * FROM %s.%s WHERE id > %%(low)s AND id <= %%(high)s ORDER BY id'
            % (SCHEMA, self.table),
            params=dict(low=1, high=3),
            verbose=False,
        )

        self.assertEqual(loaded['id'].tolist(), [2, 3])

    def test_numpy_scalars_bind_as_query_parameters(self):
        # A cursor dictionary read off a dataframe can still carry numpy scalars.
        self._export(pd.DataFrame({'id': [1, 2, 3]}))

        loaded = self.loader.load(
            'SELECT * FROM %s.%s WHERE id > %%(low)s ORDER BY id' % (SCHEMA, self.table),
            params=dict(low=np.int64(1)),
            verbose=False,
        )

        self.assertEqual(loaded['id'].tolist(), [2, 3])

    def test_timedelta_column_exports_as_nanoseconds(self):
        df = pd.DataFrame({
            'id': [1, 2],
            'elapsed': pd.to_timedelta(['1 days', '0 days 00:00:01.5']),
        })

        self._export(df)

        self.assertEqual(self._load()['elapsed'].tolist(), [86400000000000, 1500000000])

    def test_float_predictions_keep_their_value(self):
        # numpy 2's repr made a float bind as the literal text "np.float64(0.5)".
        df = pd.DataFrame({
            'id': pd.Series([1, 2], dtype='int64'),
            'prediction': pd.Series([0.125, 0.875], dtype='float64'),
        })

        self.loader.export(
            df,
            SCHEMA,
            self.table,
            if_exists='replace',
            index=False,
            verbose=False,
            unique_constraints=['id'],
            unique_conflict_method='IGNORE',
        )
        self.loader.conn.commit()

        self.assertEqual(self._load()['prediction'].tolist(), [0.125, 0.875])


    def test_nulls_round_trip_through_the_insert_path(self):
        """
        A unique constraint switches the exporter from COPY to bound INSERT
        parameters, which need None rather than NaN.
        """
        df = pd.DataFrame({
            'id': pd.Series([1, 2, 3], dtype='int64'),
            'label': pd.Series(['alpha', None, 'gamma']),
            'amount': pd.Series([1.5, np.nan, 3.5], dtype='float64'),
            'seen_at': pd.to_datetime(['2026-01-01', None, '2026-01-03'], utc=True),
        })

        self.loader.export(
            df,
            SCHEMA,
            self.table,
            if_exists='replace',
            index=False,
            verbose=False,
            unique_constraints=['id'],
            unique_conflict_method='IGNORE',
        )
        self.loader.conn.commit()

        loaded = self._load()

        self.assertEqual(loaded['label'].tolist()[0], 'alpha')
        for column in ('label', 'amount', 'seen_at'):
            with self.subTest(column=column):
                self.assertTrue(pd.isna(loaded.loc[1, column]))

    def test_nulls_round_trip_through_the_copy_path(self):
        """The COPY path writes NaN and None as the same empty CSV field."""
        df = pd.DataFrame({
            'id': pd.Series([1, 2, 3], dtype='int64'),
            'label': pd.Series(['alpha', None, 'gamma']),
            'amount': pd.Series([1.5, np.nan, 3.5], dtype='float64'),
            'seen_at': pd.to_datetime(['2026-01-01', None, '2026-01-03'], utc=True),
        })

        self._export(df)

        loaded = self._load()

        self.assertEqual(loaded['label'].tolist()[0], 'alpha')
        self.assertEqual(loaded['amount'].tolist()[2], 3.5)
        for column in ('label', 'amount', 'seen_at'):
            with self.subTest(column=column):
                self.assertTrue(pd.isna(loaded.loc[1, column]))


    def test_a_large_batch_is_sent_in_few_round_trips(self):
        """
        The insert path used to issue one statement per row, which made a batch cost
        one network round trip per row.
        """
        rows = 5000
        df = pd.DataFrame({
            'id': pd.Series(range(rows), dtype='int64'),
            'seen_at': pd.to_datetime(['2026-07-20 18:15:34.258586+00:00'] * rows, utc=True),
            'amount': pd.Series([1.5] * rows, dtype='float64'),
        })

        with unittest.mock.patch(
            'mage_ai.io.postgres.execute_values', wraps=execute_values,
        ) as sender:
            self.loader.export(
                df,
                SCHEMA,
                self.table,
                if_exists='replace',
                index=False,
                verbose=False,
                unique_constraints=['id'],
                unique_conflict_method='IGNORE',
            )
            self.loader.conn.commit()

        self.assertEqual(len(self._load()), rows)
        # One call, which pages internally. Anything per-row would be 5000 statements.
        self.assertEqual(sender.call_count, 1)
        self.assertEqual(sender.call_args.kwargs['page_size'], 1000)

    def test_conflict_update_overwrites_the_stored_row(self):
        keys = dict(id=pd.Series([1, 2], dtype='int64'))

        for amount in (1.5, 9.5):
            self.loader.export(
                pd.DataFrame(dict(**keys, amount=pd.Series([amount] * 2, dtype='float64'))),
                SCHEMA,
                self.table,
                if_exists='append',
                index=False,
                verbose=False,
                unique_constraints=['id'],
                unique_conflict_method='UPDATE',
            )
            self.loader.conn.commit()

        loaded = self._load()

        self.assertEqual(len(loaded), 2)
        self.assertEqual(loaded['amount'].tolist(), [9.5, 9.5])


if __name__ == '__main__':
    unittest.main()
