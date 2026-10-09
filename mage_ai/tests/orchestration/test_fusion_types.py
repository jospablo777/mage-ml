"""
Values a fused block passes to the next block in memory must equal what the next block
would read from storage. Every value passes_in_memory accepts is stored, read back and
compared with strict dtypes, index and columns.
"""
import datetime as dt
import decimal

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
from pandas.testing import assert_frame_equal
from polars.testing import assert_frame_equal as assert_polars_frame_equal

from mage_ai.data_preparation.models.variable import Variable
from mage_ai.orchestration.fusion import passes_in_memory
from mage_ai.tests.base_test import DBTestCase


def pandas_frames():
    n = 4
    yield 'numbers', pd.DataFrame({
        'int': np.arange(n),
        'int32': np.arange(n, dtype='int32'),
        'float': [1.5, np.nan, -0.0, 1e300],
        'bool': [True, False, True, False],
    })
    yield 'nullable', pd.DataFrame({
        'Int64': pd.array([1, None, 3, 4], dtype='Int64'),
        'UInt8': pd.array([1, None, 3, 255], dtype='UInt8'),
        'boolean': pd.array([True, None, False, True], dtype='boolean'),
        'Float64': pd.array([1.5, None, 3.0, 4.0], dtype='Float64'),
    })
    yield 'strings', pd.DataFrame({
        'str': pd.Series(['a', None, 'ñ', ''], dtype='str'),
        'category': pd.Categorical(['x', 'y', None, 'x']),
    })
    yield 'times', pd.DataFrame({
        'naive': pd.to_datetime(['2024-01-01', None, '1900-01-01', '2262-04-11']),
        'zoned': pd.to_datetime(['2024-01-01 12:00'] * 4).tz_localize('America/Costa_Rica'),
        'us': pd.Series(pd.to_datetime(['2024-01-01'] * 4)).astype('datetime64[us]'),
        'delta': pd.to_timedelta([1, 2, None, 4], unit='s'),
    })
    yield 'arrow', pd.DataFrame({
        'decimal': pd.Series(
            [decimal.Decimal('1.10'), None, decimal.Decimal('-2.00'), decimal.Decimal('0')],
            dtype=pd.ArrowDtype(pa.decimal128(10, 2)),
        ),
        'date': pd.Series([dt.date(2024, 1, 1), None, dt.date(1, 1, 1), dt.date(9999, 12, 31)],
                          dtype=pd.ArrowDtype(pa.date32())),
        'list': pd.Series([[1, 2], None, [], [3]], dtype=pd.ArrowDtype(pa.list_(pa.int64()))),
        'int64[pyarrow]': pd.Series([1, None, 3, 4], dtype='int64[pyarrow]'),
    })
    yield 'named index', pd.DataFrame({'v': [1, 2]}, index=pd.Index([10, 20], name='id'))
    yield 'datetime index', pd.DataFrame(
        {'v': [1, 2]}, index=pd.to_datetime(['2024-01-01', '2024-01-02']),
    )
    yield 'filtered index', pd.DataFrame({'v': range(5)})[lambda df: df['v'] > 2]
    yield 'empty', pd.DataFrame({'a': pd.Series([], dtype='Int64'),
                                 'b': pd.Series([], dtype='str')})


def polars_frames():
    yield 'polars', pl.DataFrame({
        'int': [1, None, 3],
        'uint64': pl.Series([1, 2, 2 ** 64 - 1], dtype=pl.UInt64),
        'float': [1.5, None, float('nan')],
        'str': ['a', None, 'ñ'],
        'bool': [True, None, False],
        'date': [dt.date(2024, 1, 1), None, dt.date(1, 1, 1)],
        'zoned': pl.Series([dt.datetime(2024, 1, 1, 12)] * 3).dt.replace_time_zone('UTC'),
        'duration': [dt.timedelta(seconds=1), None, dt.timedelta(days=-1)],
        'decimal': pl.Series([decimal.Decimal('1.10'), None, decimal.Decimal('2.00')],
                             dtype=pl.Decimal(10, 2)),
        'list': [[1, 2], None, []],
        'struct': [{'a': 1, 'b': 'x'}, None, {'a': None, 'b': 'y'}],
        'categorical': pl.Series(['x', 'y', None], dtype=pl.Categorical),
        'binary': [b'\x00\xff', None, b''],
    })
    yield 'polars empty', pl.DataFrame({'a': pl.Series([], dtype=pl.Int64)})


class FusionTypeTest(DBTestCase):
    def read_back(self, name, value):
        uuid = name.replace(' ', '_')
        Variable(uuid, self.repo_path, 'fusion_types').write_data(value)
        return Variable(uuid, self.repo_path, 'fusion_types').read_data()

    def test_pandas_frames_that_pass_read_back_identically(self):
        for name, frame in pandas_frames():
            with self.subTest(name):
                self.assertTrue(passes_in_memory([frame]), name)
                assert_frame_equal(
                    self.read_back(name, frame), frame,
                    check_dtype=True, check_index_type=True, check_column_type=True,
                    check_freq=False, check_flags=True, check_exact=True,
                )

    def test_polars_frames_that_pass_read_back_identically(self):
        for name, frame in polars_frames():
            with self.subTest(name):
                self.assertTrue(passes_in_memory([frame]), name)
                assert_polars_frame_equal(self.read_back(name, frame), frame, check_exact=True)

    def test_values_storage_changes_are_read_from_storage(self):
        for value in [
            [pd.DataFrame({'o': [{'a': 1}, None]})],
            [pd.DataFrame({'o': pd.Series(['a', None], dtype=object)})],
            [pd.DataFrame({'v': [1]}, index=pd.Index(['a'], dtype=object))],
            [pd.DataFrame({('a', 'b'): [1]})],
            [pd.DataFrame({0: [1, 2], 1: [3.0, 4.0]})],
            [pl.DataFrame({'a': [1]}).lazy()],
            [np.arange(3)],
            [{'a': (1, 2)}],
            [pd.DataFrame({'a': [1]}), pd.DataFrame({'b': [2]})],
            [],
            None,
        ]:
            with self.subTest(repr(value)[:60]):
                self.assertFalse(passes_in_memory(value))
