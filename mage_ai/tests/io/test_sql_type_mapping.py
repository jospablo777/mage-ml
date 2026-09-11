"""
Type mapping between pandas columns and SQL columns.

The exporters read a column's inferred dtype to pick a SQL type and to convert the
values. pandas 3 and numpy 2 changed what those columns look like: strings carry a
StringDtype, temporal columns keep their source resolution, Series.view is gone, and
narrowing a Python int that does not fit now raises instead of wrapping.
"""
import pandas as pd

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
    def setUp(self):
        super().setUp()
        self.loader = loader()

    def get_type(self, series):
        return self.loader.get_type(series, infer_dtypes(pd.DataFrame({'c': series}))['c'])

    def test_integer_width_follows_the_value_range(self):
        cases = [
            ([0, 1], 'smallint'),
            ([-32768, 32767], 'smallint'),
            ([0, 345100], 'integer'),
            ([0, 2**40], 'bigint'),
        ]
        for values, expected in cases:
            with self.subTest(values=values):
                self.assertEqual(self.get_type(pd.Series(values, dtype='int64')), expected)

    def test_integer_columns_that_hold_python_ints(self):
        """
        numpy 2 raises OverflowError when a Python int is narrowed to a type that
        cannot hold it. Object columns and Arrow-backed columns report Python ints.
        """
        for dtype in ('object', 'int64[pyarrow]', 'Int64'):
            with self.subTest(dtype=dtype):
                series = pd.Series([1, 345100], dtype=dtype)

                self.assertEqual(self.get_type(series), 'integer')

    def test_remaining_column_types(self):
        cases = [
            (pd.Series(['CR', 'US']), 'text'),
            (pd.Series([True, False]), 'boolean'),
            (pd.Series([0.5, 1.5]), 'double precision'),
            (pd.Series(pd.to_datetime(['2026-01-01'])), 'timestamp'),
            (pd.Series(pd.to_datetime(['2026-01-01'], utc=True)), 'timestamptz'),
            (pd.Series(['a'], dtype='category'), 'text'),
            (pd.Series(pd.to_timedelta(['1 days'])), 'bigint'),
        ]
        for series, expected in cases:
            with self.subTest(dtype=str(series.dtype)):
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
