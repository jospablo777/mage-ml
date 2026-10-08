"""
Type mapping between pandas columns and SQL columns.

The exporters read a column's inferred dtype to pick a SQL type and to convert the
values. pandas 3 and numpy 2 changed what those columns look like: strings carry a
StringDtype, temporal columns keep their source resolution, Series.view is gone, and
narrowing a Python int that does not fit now raises instead of wrapping.
"""
import datetime
import decimal

import pandas as pd
import pyarrow as pa

from mage_ai.io import postgres_types
from mage_ai.io.export_utils import PandasTypes, clean_df_for_export, infer_dtypes
from mage_ai.io.postgres import Postgres
from mage_ai.tests.base_test import TestCase


def loader() -> Postgres:
    return Postgres(
        dbname='test', user='mage', password='password', host='db.internal', verbose=False,
    )


class InferDtypesTests(TestCase):
    def test_pipeline_column_shapes(self):
        df = pd.DataFrame({
            'case_id': pd.Series([345038, 345039], dtype='int64'),
            'opened_at': pd.to_datetime(['2026-07-20 18:15:34.258586+00:00'] * 2, utc=True),
            'flag': pd.Series([True, False], dtype='bool'),
            'score': pd.Series([0.25, 0.75], dtype='float64'),
            'country': pd.Series(['CR', 'US']),
        })

        self.assertEqual(infer_dtypes(df), {
            'case_id': PandasTypes.INTEGER,
            'opened_at': PandasTypes.DATETIME64,
            'flag': PandasTypes.BOOLEAN,
            'score': PandasTypes.FLOATING,
            'country': PandasTypes.STRING,
        })

    def test_text_columns_are_not_object_anymore(self):
        # The mapping used to rely on object dtype for text.
        self.assertNotEqual(pd.Series(['a']).dtype, object)
        self.assertEqual(infer_dtypes(pd.DataFrame({'a': ['x']}))['a'], PandasTypes.STRING)


class PostgresTypeMappingTests(TestCase):
    def get_type(self, series):
        return postgres_types.pandas_column_type(series)

    def test_integer_width_follows_the_dtype(self):
        """
        The width used to follow the values of the batch being written, so a table
        created from small keys rejected larger keys on a later append.
        """
        cases = [
            ('int8', 'smallint'),
            ('int16', 'smallint'),
            ('Int16', 'smallint'),
            ('uint8', 'smallint'),
            ('int32', 'integer'),
            ('uint16', 'integer'),
            ('int64', 'bigint'),
            ('Int64', 'bigint'),
            ('uint32', 'bigint'),
            ('uint64', 'numeric(20, 0)'),
        ]
        for dtype, expected in cases:
            with self.subTest(dtype=dtype):
                self.assertEqual(self.get_type(pd.Series([0, 1], dtype=dtype)), expected)

    def test_integer_columns_that_hold_python_ints(self):
        """
        numpy 2 raises OverflowError when a Python int is narrowed to a type that
        cannot hold it. Object columns and Arrow-backed columns report Python ints.
        """
        for dtype in ('object', 'int64[pyarrow]', 'Int64'):
            with self.subTest(dtype=dtype):
                series = pd.Series([1, 2**62], dtype=dtype)

                self.assertEqual(self.get_type(series), 'bigint')

    def test_remaining_column_types(self):
        cases = [
            (pd.Series(['CR', 'US']), 'text'),
            (pd.Series(['CR'], dtype='string[pyarrow]'), 'text'),
            (pd.Series([True, False]), 'boolean'),
            (pd.Series([True, None], dtype='boolean'), 'boolean'),
            (pd.Series([0.5, 1.5]), 'double precision'),
            (pd.Series([0.5], dtype='Float32'), 'real'),
            (pd.Series([0.5], dtype='double[pyarrow]'), 'double precision'),
            (pd.Series(pd.to_datetime(['2026-01-01'])), 'timestamp'),
            (pd.Series(pd.to_datetime(['2026-01-01'], utc=True)), 'timestamptz'),
            (
                pd.Series(pd.to_datetime(['2026-01-01']).tz_localize('America/Costa_Rica')),
                'timestamptz',
            ),
            (pd.Series(['a'], dtype='category'), 'text'),
            (pd.Series(pd.to_timedelta(['1 days'])), 'interval'),
            (pd.Series([1, 2.5], dtype=object), 'double precision'),
            (pd.Series([1, 'a'], dtype=object), 'text'),
        ]
        for series, expected in cases:
            with self.subTest(dtype=str(series.dtype)):
                self.assertEqual(self.get_type(series), expected)

    def test_arrow_backed_columns(self):
        cases = [
            (pa.date32(), [datetime.date(2026, 1, 1)], 'date'),
            (pa.decimal128(10, 2), [decimal.Decimal('1.50')], 'numeric'),
            (pa.list_(pa.int64()), [[1, 2]], 'bigint[]'),
            (pa.timestamp('us', 'UTC'), [pd.Timestamp('2026-01-01', tz='UTC')], 'timestamptz'),
            (pa.binary(), [b'\x00'], 'bytea'),
        ]
        for arrow_type, values, expected in cases:
            with self.subTest(arrow_type=str(arrow_type)):
                series = pd.Series(values, dtype=pd.ArrowDtype(arrow_type))

                self.assertEqual(self.get_type(series), expected)


class CleanForExportTests(TestCase):
    def setUp(self):
        super().setUp()
        self.loader = loader()

    def clean(self, df):
        return clean_df_for_export(df, self.loader.clean, infer_dtypes(df))

    def test_timedelta_becomes_integer_nanoseconds(self):
        """Series.view, which this used, was removed in pandas 3."""
        df = pd.DataFrame({'elapsed': pd.to_timedelta(['1 days', '0 days 00:00:01.5'])})

        cleaned = self.clean(df)

        self.assertEqual(cleaned['elapsed'].tolist(), [86400000000000, 1500000000])

    def test_period_becomes_its_ordinal(self):
        df = pd.DataFrame({'month': pd.Series(pd.period_range('2024-01', periods=2, freq='M'))})

        cleaned = self.clean(df)

        self.assertTrue(pd.api.types.is_integer_dtype(cleaned['month']))

    def test_categorical_becomes_text(self):
        df = pd.DataFrame({'grade': pd.Series(['a', 'b'], dtype='category')})

        self.assertEqual(self.clean(df)['grade'].tolist(), ['a', 'b'])

    def test_other_columns_keep_their_dtype(self):
        df = pd.DataFrame({
            'case_id': pd.Series([1, 2], dtype='int64'),
            'opened_at': pd.to_datetime(['2026-01-01'] * 2, utc=True),
            'flag': pd.Series([True, False], dtype='bool'),
        })

        cleaned = self.clean(df)

        self.assertEqual(
            cleaned.dtypes.astype(str).to_dict(),
            df.dtypes.astype(str).to_dict(),
        )
