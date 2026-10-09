import datetime as dt
import decimal

import pandas as pd
import polars as pl

from mage_ai.io.duckdb import DuckDB
from mage_ai.tests.base_test import TestCase

TABLE = """
CREATE TABLE t AS SELECT * FROM (VALUES
    (1, 9007199254740993, 1.25::DECIMAL(10, 2), 'a', DATE '2024-01-01',
     TIMESTAMPTZ '2024-01-01 12:00:00+00', [1, 2], INTERVAL 90 MINUTE),
    (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
) v(id, big, amount, name, day, happened_at, items, span)
"""


class DuckDBLoadTest(TestCase):
    def setUp(self):
        super().setUp()
        self.client = DuckDB(database=':memory:', verbose=False)
        self.client.execute(TABLE)

    def tearDown(self):
        self.client.close()
        super().tearDown()

    def test_default_load_is_unchanged(self):
        frame = self.client.load('SELECT * FROM t ORDER BY id', verbose=False)

        self.assertEqual(str(frame['big'].dtype), 'float64')
        self.assertEqual(str(frame['happened_at'].dtype), 'datetime64[us, UTC]')

    def test_exact_types(self):
        frame = self.client.load('SELECT * FROM t ORDER BY id', verbose=False, exact_types=True)

        self.assertIsInstance(frame['big'].dtype, pd.ArrowDtype)
        self.assertEqual(frame['big'].tolist(), [9007199254740993, pd.NA])
        self.assertEqual(frame['amount'].tolist()[0], decimal.Decimal('1.25'))
        self.assertEqual(frame['items'].tolist()[0], [1, 2])
        self.assertEqual(str(frame['happened_at'].dtype), 'timestamp[us, tz=UTC][pyarrow]')
        self.assertEqual(
            frame['happened_at'].tolist()[0],
            pd.Timestamp(dt.datetime(2024, 1, 1, 12, tzinfo=dt.timezone.utc)),
        )

    def test_polars(self):
        frame = self.client.load('SELECT * FROM t ORDER BY id', verbose=False, polars=True)

        self.assertIsInstance(frame, pl.DataFrame)
        self.assertEqual(frame['big'].to_list(), [9007199254740993, None])
        self.assertEqual(frame.schema['happened_at'], pl.Datetime('us', 'UTC'))
        self.assertEqual(frame['span'].to_list()[0], dt.timedelta(minutes=90))
        self.assertEqual(frame.schema['span'], pl.Duration('ns'))

    def test_polars_intervals_with_months(self):
        """Polars cannot import Arrow intervals; months have no fixed duration."""
        frame = self.client.load(
            "SELECT INTERVAL 1 MONTH + INTERVAL 2 DAY AS span", verbose=False, polars=True,
        )

        self.assertEqual(frame['span'].to_list(), [dict(months=1, days=2, nanoseconds=0)])

    def test_limit_and_params(self):
        frame = self.client.load(
            'SELECT id FROM t WHERE id >= ? ORDER BY id', verbose=False, polars=True,
            params=[1], limit=1,
        )

        self.assertEqual(frame['id'].to_list(), [1])


class DuckDBNullableIntegersTest(TestCase):
    """SQL blocks load with nullable_integers: read_sql's types, with exact integers."""

    def setUp(self):
        super().setUp()
        self.client = DuckDB(database=':memory:', verbose=False)

    def test_integers_stay_integers(self):
        query = """
            SELECT * FROM (VALUES
                (9223372036854775807::BIGINT, 18446744073709551615::UBIGINT,
                 170141183460469231731687303715884105727::HUGEINT, 1.5::DECIMAL(10, 2), 'a'),
                (NULL, NULL, NULL, NULL, NULL)
            ) t(big, ubig, huge, amount, name)
        """

        default = self.client.load(query, verbose=False)
        frame = self.client.load(query, verbose=False, nullable_integers=True)

        # read_sql made the integer columns float64.
        self.assertEqual(str(default['big'].dtype), 'float64')
        self.assertEqual(frame['big'].tolist(), [2**63 - 1, pd.NA])
        self.assertEqual(str(frame['big'].dtype), 'Int64')
        self.assertEqual(frame['ubig'].tolist(), [2**64 - 1, pd.NA])
        self.assertEqual(str(frame['ubig'].dtype), 'UInt64')
        self.assertEqual(frame['huge'].tolist(), [2**127 - 1, None])
        for name in ('amount', 'name'):
            self.assertEqual(frame[name].dtype, default[name].dtype)
