import datetime as dt
import decimal
import uuid

import pandas as pd

from mage_ai.io import clickhouse_types
from mage_ai.tests.base_test import TestCase


class ColumnTypeTest(TestCase):
    def test_pandas_dtypes(self):
        cases = [
            ('Int64', pd.Series(pd.array([1, None], dtype='Int64'))),
            ('Int32', pd.Series([1], dtype='int32')),
            ('UInt64', pd.Series([2**64 - 1], dtype='uint64')),
            ('Float32', pd.Series([1.5], dtype='float32')),
            ('Float64', pd.Series([1.5, None])),
            ('Bool', pd.Series(pd.array([True, None], dtype='boolean'))),
            ('String', pd.Series(['a'], dtype='str')),
            ('DateTime64(6)', pd.Series(pd.to_datetime(['2024-01-01'])).astype('datetime64[us]')),
            ('DateTime64(9)', pd.Series(pd.to_datetime(['2024-01-01'])).astype('datetime64[ns]')),
            ("DateTime64(6, 'UTC')", pd.Series(
                pd.to_datetime(['2024-01-01']).tz_localize('UTC'),
            ).astype('datetime64[us, UTC]')),
            # ClickHouse has no duration type; durations are stored in microseconds.
            ('Int64', pd.Series(pd.to_timedelta(['1s']))),
        ]
        for expected, series in cases:
            with self.subTest(expected):
                self.assertEqual(clickhouse_types.column_type(series), expected)

    def test_object_columns(self):
        cases = {
            'Date32': [dt.date(1900, 1, 1), None],
            'DateTime64(6)': [dt.datetime(2024, 1, 1), None],
            "DateTime64(6, 'UTC')": [dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)],
            'Decimal(29, 9)': [decimal.Decimal('12345678901234567890.123456789'), None],
            'Decimal(3, 2)': [decimal.Decimal('1.10'), decimal.Decimal('-0.01')],
            'UUID': [uuid.uuid4(), None],
            'Int64': [1, None, -5],
            'UInt64': [2**64 - 1, 0],
            'Int128': [2**100, -1],
            'UInt128': [2**128 - 1],
            'Int256': [2**200],
            'String': [{'a': 1}, [1, 2], 'x'],
        }
        for expected, values in cases.items():
            with self.subTest(expected):
                self.assertEqual(
                    clickhouse_types.column_type(pd.Series(values, dtype=object)), expected,
                )

    def test_values_without_an_exact_type_are_text(self):
        """ClickHouse decimals have no NaN, and mixed time zones have no one type."""
        for values in (
            [decimal.Decimal('NaN')],
            [dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)],
            [decimal.Decimal('1' * 80)],
        ):
            self.assertEqual(clickhouse_types.column_type(pd.Series(values, dtype=object)),
                             'String')

    def test_quote(self):
        self.assertEqual(clickhouse_types.quote('my col'), '`my col`')
        self.assertEqual(clickhouse_types.quote('back`tick'), '`back\\`tick`')


class ValuesForInsertTest(TestCase):
    def test_durations_become_microseconds(self):
        series = pd.Series(pd.to_timedelta(['1.5s', None]))

        values = clickhouse_types.values_for_insert(series, 'Nullable(Int64)')

        self.assertEqual(values.tolist(), [1_500_000, None])

    def test_lists_and_dicts_become_json_text(self):
        series = pd.Series([{'a': [1, None]}, ['x'], None, 'text'], dtype=object)

        values = clickhouse_types.values_for_insert(series, 'Nullable(String)')

        self.assertEqual(values.tolist(), ['{"a": [1, null]}', '["x"]', None, 'text'])

    def test_pyarrow_backed_columns_become_python_values(self):
        import pyarrow as pa

        series = pd.Series(pd.array([dt.date(2024, 1, 1), None], dtype=pd.ArrowDtype(pa.date32())))

        values = clickhouse_types.values_for_insert(series, 'Nullable(Date32)')

        self.assertEqual(values.tolist(), [dt.date(2024, 1, 1), None])
