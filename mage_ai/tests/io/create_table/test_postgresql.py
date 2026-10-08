import datetime
import decimal

import numpy as np
import pandas as pd
import polars as pl

from mage_ai.io import postgres_types
from mage_ai.io.postgres import Postgres
from mage_ai.tests.base_test import DBTestCase


class TestTablePostgres(DBTestCase):
    def test_table_postgres(self):
        psg = Postgres('test', 'test', 'test', 'test', '123')
        df = pd.DataFrame({'varchar_time': '2002-01-01 00:00:00',
                           'datetime_time': '2002-01-02 00:00:00'}, index=[0])
        db_dtypes = {col: postgres_types.pandas_column_type(df[col]) for col in df.columns}
        overwrite_types = {'datetime_time': "TIMESTAMP"}

        query = psg.build_create_table_command(
            db_dtypes,
            'Test',
            'Test',
            unique_constraints=None,
            overwrite_types=overwrite_types,
        )

        self.assertEqual('CREATE TABLE Test.Test ("varchar_time" text,"datetime_time" TIMESTAMP);',
                         query)

    def test_array_text(self):
        cases = [
            ([], 'text[]', '{}'),
            ([123], 'integer[]', '{"123"}'),
            ([['àabc', 'deèéf']], 'text[]', '{{"àabc","deèéf"}}'),
            ([['08:00', '12:00'], ['15:00', '20:00']], 'text[]',
             '{{"08:00","12:00"},{"15:00","20:00"}}'),
            # The old cleaner swapped every bracket in the JSON text, inside values too.
            (['a[b]', '{c}'], 'text[]', '{"a[b]","{c}"}'),
            (['with "quotes"', 'back\\slash', None, 'NULL', ''], 'text[]',
             '{"with \\"quotes\\"","back\\\\slash",NULL,"NULL",""}'),
            (np.array([1, 2]), 'bigint[]', '{"1","2"}'),
            ([decimal.Decimal('1.10'), datetime.date(2000, 1, 1)], 'text[]',
             '{"1.10","2000-01-01"}'),
        ]
        for value, pg_type, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(postgres_types.render_value(value, pg_type), expected)


class TestPostgresColumnTypes(DBTestCase):
    def test_pandas_column_types(self):
        df = pd.DataFrame({
            'i16': pd.Series([1], dtype='int16'),
            'i32': pd.Series([1], dtype='int32'),
            'i64': pd.Series([1], dtype='int64'),
            'nullable': pd.array([1], dtype='Int64'),
            'u64': pd.Series([1], dtype='uint64'),
            'f32': pd.Series([1.5], dtype='float32'),
            'f64': pd.Series([1.5], dtype='float64'),
            'flag': pd.Series([True]),
            'when': pd.to_datetime(['2024-01-01']),
            'when_utc': pd.to_datetime(['2024-01-01']).tz_localize('UTC'),
            'span': pd.to_timedelta(['1 day']),
            'text': pd.Series(['a'], dtype='str'),
            'label': pd.Categorical(['a']),
            'dec': pd.Series([decimal.Decimal('1.5')], dtype=object),
            'raw': pd.Series([b'\x00'], dtype=object),
            'day': pd.Series([datetime.date(2024, 1, 1)], dtype=object),
            'clock': pd.Series([datetime.time(1, 2)], dtype=object),
            'doc': pd.Series([{'a': 1}], dtype=object),
            'ints': pd.Series([[1, 2]], dtype=object),
            'nested': pd.Series([[[1], [2]]], dtype=object),
            'empty': pd.Series([None], dtype=object),
        })

        types = {column: postgres_types.pandas_column_type(df[column]) for column in df.columns}

        self.assertEqual(types, {
            'i16': 'smallint',
            'i32': 'integer',
            'i64': 'bigint',
            'nullable': 'bigint',
            'u64': 'numeric(20, 0)',
            'f32': 'real',
            'f64': 'double precision',
            'flag': 'boolean',
            'when': 'timestamp',
            'when_utc': 'timestamptz',
            'span': 'interval',
            'text': 'text',
            'label': 'text',
            'dec': 'numeric',
            'raw': 'bytea',
            'day': 'date',
            'clock': 'time',
            'doc': 'jsonb',
            'ints': 'bigint[]',
            'nested': 'jsonb',
            'empty': 'text',
        })

    def test_polars_column_types(self):
        self.assertEqual(postgres_types.polars_column_type(pl.Int16), 'smallint')
        self.assertEqual(postgres_types.polars_column_type(pl.UInt64), 'numeric(20, 0)')
        self.assertEqual(postgres_types.polars_column_type(pl.Decimal(10, 2)), 'numeric(10, 2)')
        self.assertEqual(postgres_types.polars_column_type(pl.Datetime('us', 'UTC')), 'timestamptz')
        self.assertEqual(postgres_types.polars_column_type(pl.List(pl.String)), 'text[]')
        self.assertEqual(postgres_types.polars_column_type(pl.List(pl.List(pl.Int64))), 'jsonb')
        self.assertEqual(postgres_types.polars_column_type(pl.Struct({'a': pl.Int64})), 'jsonb')

    def test_copy_text_escapes_and_nulls(self):
        text = postgres_types.copy_text([['a\tb', None, 'c\\d', 'e\nf', '']])

        self.assertEqual(text, 'a\\tb\n\\N\nc\\\\d\ne\\nf\n\n')
