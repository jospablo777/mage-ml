"""
Block outputs written to Mage's variable storage and read back, for each pandas and
Polars dtype a block can return.

Every value and dtype must come back unchanged, in full reads and in sample reads.
Outputs written by earlier versions of the storage must still read.
"""
import datetime
import decimal
import json
import os
import uuid
from unittest.mock import patch

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pandas.testing import assert_frame_equal, assert_series_equal
from polars.testing import assert_frame_equal as assert_polars_frame_equal

from mage_ai.data_preparation.models.utils import infer_variable_type
from mage_ai.data_preparation.models.variable import Variable
from mage_ai.data_preparation.models.variables.constants import (
    DATAFRAME_COLUMN_TYPES_FILE,
    DATAFRAME_PARQUET_FILE,
)
from mage_ai.data_preparation.storage.local_storage import LocalStorage
from mage_ai.tests.base_test import DBTestCase

OFFSET = datetime.timezone(datetime.timedelta(hours=-6))


def pandas_columns():
    return {
        'int64': pd.Series([1, -2, 2**62], dtype='int64'),
        'int8': pd.Series([1, -128, 127], dtype='int8'),
        'uint64': pd.Series([0, 1, 2**64 - 1], dtype='uint64'),
        'Int64': pd.array([1, None, 2**63 - 1], dtype='Int64'),
        'Int8': pd.array([1, None, -128], dtype='Int8'),
        'float64': pd.Series([1.5, np.nan, np.inf], dtype='float64'),
        'float32': pd.Series([1.5, -0.0, np.nan], dtype='float32'),
        'Float64': pd.array([1.5, None, 2.25], dtype='Float64'),
        'bool': pd.Series([True, False, True]),
        'boolean': pd.array([True, None, False], dtype='boolean'),
        'str': pd.Series(['a', None, 'ñandú 🐍'], dtype='str'),
        'str_blank': pd.Series(['', None, ' '], dtype='str'),
        'string_na': pd.Series(['a', None, 'b'], dtype=pd.StringDtype('pyarrow', na_value=pd.NA)),
        'category': pd.Categorical(['x', None, 'y']),
        'category_unused': pd.Categorical(['a', 'a', None], categories=['a', 'b', 'c']),
        'category_ordered': pd.Categorical(
            ['lo', 'hi', 'lo'], categories=['lo', 'hi'], ordered=True,
        ),
        'category_int': pd.Categorical([1, 2, None]),
        'category_float': pd.Categorical([1.5, None, 2.5]),
        'category_bool': pd.Categorical([True, False, None]),
        'category_dates': pd.Categorical(pd.to_datetime(['2024-01-01', None, '2024-01-01'])),
        'cut': pd.cut(pd.Series([1, 5, 9]), bins=[0, 3, 6, 10]),
        'datetime_ns': pd.Series(
            pd.to_datetime(['2024-01-01 00:00:00.123456789', None, '1677-09-22'], format='ISO8601'),
            dtype='datetime64[ns]',
        ),
        'datetime_us': pd.Series(
            [datetime.datetime(1, 1, 1), None, datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)],
            dtype='datetime64[us]',
        ),
        'datetime_s': pd.Series(
            [datetime.datetime(2024, 1, 1), None, datetime.datetime(3000, 1, 1)],
            dtype='datetime64[s]',
        ),
        'datetime_utc': pd.to_datetime(['2024-01-01', None, '2024-06-01']).tz_localize('UTC'),
        'datetime_named_tz': pd.to_datetime(['2024-03-10 01:30', None, '2024-11-03 01:30'])
        .tz_localize('America/New_York', ambiguous=True),
        'datetime_offset': pd.Series(
            [pd.Timestamp('2024-01-01', tz=OFFSET), None, pd.Timestamp('2024-01-02', tz=OFFSET)],
        ),
        'timedelta': pd.to_timedelta(['1 days 00:00:00.000001', None, '-3 hours']),
        'period': pd.Series(pd.period_range('2024-01', periods=3, freq='M')),
        'interval': pd.Series(pd.interval_range(0, 3)),
        'sparse': pd.Series(pd.arrays.SparseArray([0, 0, 1])),
        'complex': pd.Series([1 + 2j, 0j, -1.5j], dtype='complex128'),
        'object_date': pd.Series(
            [datetime.date(2024, 1, 1), None, datetime.date(1, 1, 1)], dtype=object,
        ),
        'object_time': pd.Series(
            [datetime.time(1, 2, 3, 4), None, datetime.time(23, 59)], dtype=object,
        ),
        'object_timetz': pd.Series([datetime.time(1, 2, tzinfo=OFFSET), None, None], dtype=object),
        'object_decimal': pd.Series(
            [decimal.Decimal('1.10'), None, decimal.Decimal('-1E+30')], dtype=object,
        ),
        'object_bytes': pd.Series([b'\x00a\x00', None, b''], dtype=object),
        'object_uuid': pd.Series([uuid.UUID(int=1), None, uuid.UUID(int=2**128 - 1)], dtype=object),
        'object_dict': pd.Series([{'a': 1, 'b': [1, 2]}, None, {}], dtype=object),
        'object_list': pd.Series([[1, 2], None, []], dtype=object),
        'object_nested': pd.Series([[[1], [2, 3]], None, [[]]], dtype=object),
        'object_int_and_str': pd.Series([1, 'a', None], dtype=object),
        'object_int_and_float': pd.Series([1, 2.5, None], dtype=object),
        'object_big_int': pd.Series([1, 2**70, None], dtype=object),
        'object_float_nan_and_none': pd.Series([1.5, float('nan'), None], dtype=object),
        'object_none': pd.Series([None, None, None], dtype=object),
        'arrow_int': pd.Series([1, None, 3], dtype='int64[pyarrow]'),
        'arrow_decimal': pd.Series(
            [decimal.Decimal('1.10'), None, decimal.Decimal('2.00')],
            dtype=pd.ArrowDtype(pa.decimal128(10, 2)),
        ),
        'arrow_list': pd.Series([[1, 2], None, []], dtype=pd.ArrowDtype(pa.list_(pa.int64()))),
        'arrow_struct': pd.Series(
            [{'a': 1}, None, {'a': 2}], dtype=pd.ArrowDtype(pa.struct([('a', pa.int64())])),
        ),
        'arrow_date': pd.Series(
            [datetime.date(2024, 1, 1), None, datetime.date(2024, 1, 2)],
            dtype=pd.ArrowDtype(pa.date32()),
        ),
        'arrow_binary': pd.Series([b'\x00', None, b''], dtype=pd.ArrowDtype(pa.binary())),
    }


def polars_columns():
    return {
        'i8': pl.Series([1, None, -128], dtype=pl.Int8),
        'i64': pl.Series([1, None, 2**63 - 1], dtype=pl.Int64),
        'i128': pl.Series([1, None, 2**100], dtype=pl.Int128),
        'u64': pl.Series([0, None, 2**64 - 1], dtype=pl.UInt64),
        'f32': pl.Series([1.5, None, float('nan')], dtype=pl.Float32),
        'f64': pl.Series([1.5, None, float('nan')], dtype=pl.Float64),
        'bool': pl.Series([True, None, False]),
        'str': pl.Series(['a', None, '']),
        'cat': pl.Series(['x', None, 'y'], dtype=pl.Categorical),
        'enum': pl.Series(['lo', None, 'hi'], dtype=pl.Enum(['lo', 'hi'])),
        'decimal': pl.Series(
            [decimal.Decimal('1.10'), None, decimal.Decimal('-99999999.99')],
            dtype=pl.Decimal(10, 2),
        ),
        'date': pl.Series([datetime.date(2024, 1, 1), None, datetime.date(1, 1, 1)]),
        'time': pl.Series([datetime.time(1, 2, 3, 4), None, datetime.time(23, 59)]),
        'datetime_ns': pl.Series(
            [datetime.datetime(2024, 1, 1), None, datetime.datetime(2024, 1, 2)],
            dtype=pl.Datetime('ns'),
        ),
        'datetime_ms_tz': pl.Series(
            [datetime.datetime(2024, 1, 1), None, datetime.datetime(2024, 1, 2)],
            dtype=pl.Datetime('ms', 'America/Costa_Rica'),
        ),
        'duration': pl.Series(
            [datetime.timedelta(microseconds=1), None, datetime.timedelta(days=-3)],
            dtype=pl.Duration('us'),
        ),
        'binary': pl.Series([b'\x00a\x00', None, b'']),
        'list': pl.Series([[1, 2], None, []], dtype=pl.List(pl.Int64)),
        'list_of_lists': pl.Series([[[1], [2, 3]], None, [[]]], dtype=pl.List(pl.List(pl.Int64))),
        'array': pl.Series([[1, 2], None, [3, 4]], dtype=pl.Array(pl.Int64, 2)),
        'struct': pl.Series([{'a': 1, 'b': 'x'}, None, {'a': None, 'b': None}]),
        'null': pl.Series([None, None, None], dtype=pl.Null),
    }


class VariableDtypeTest(DBTestCase):
    def variable(self, name: str, data) -> Variable:
        return Variable(
            name,
            os.path.join(self.repo_path, 'pipelines', 'dtypes'),
            'block1',
            variable_type=infer_variable_type(data)[0],
        )

    def write_and_read(self, name: str, data, **read_kwargs):
        with patch('mage_ai.data.models.manager.DataManager.writeable', return_value=False):
            with patch('mage_ai.data.models.manager.DataManager.readable', return_value=False):
                variable = self.variable(name, data)
                variable.write_data(data)
                return variable.read_data(raise_exception=True, **read_kwargs)

    def test_every_pandas_dtype_round_trips(self):
        for name, values in pandas_columns().items():
            with self.subTest(column=name):
                frame = pd.DataFrame({name: values})

                back = self.write_and_read(f'pandas_{name}', frame)
                sample = self.write_and_read(f'pandas_{name}', frame, sample=True, sample_count=2)

                assert_series_equal(back[name], frame[name], check_exact=True)
                assert_series_equal(sample[name], frame[name].iloc[:2], check_exact=True)

    def test_every_polars_dtype_round_trips(self):
        """
        Int128 comes from Int64 + UInt64 in Polars 2. pyarrow cannot read it, and the
        output used to read back as an empty frame.
        """
        for name, values in polars_columns().items():
            with self.subTest(column=name):
                frame = pl.DataFrame([values.alias(name)])

                back = self.write_and_read(f'polars_{name}', frame)
                sample = self.write_and_read(f'polars_{name}', frame, sample=True, sample_count=2)

                assert_polars_frame_equal(back, frame)
                assert_polars_frame_equal(sample, frame.head(2))

    def test_mixed_object_columns_keep_each_value(self):
        """
        Object columns were cast to the type of their first value, so 2.5 in a column
        starting with an int was stored as 2.
        """
        frame = pd.DataFrame({
            'numbers': pd.Series([1, 2.5, None], dtype=object),
            'labels': pd.Series([1, 'a', True], dtype=object),
            'big': pd.Series([2**70, -(2**70), None], dtype=object),
        })

        back = self.write_and_read('mixed', frame)

        self.assertEqual(back['numbers'].tolist()[:2], [1, 2.5])
        self.assertIs(type(back['numbers'].tolist()[0]), int)
        self.assertEqual(back['labels'].tolist(), [1, 'a', True])
        self.assertEqual(back['big'].tolist(), [2**70, -(2**70), None])

    def test_column_labels_and_index_round_trip(self):
        """
        Parquet needs string column names. Labels used to come back as text, so df[0] on a
        frame built from a NumPy array raised KeyError in the next block.
        """
        frames = {
            'numpy': pd.DataFrame(np.arange(6).reshape(3, 2)),
            'mixed_labels': pd.DataFrame({0: [1], 'a': [2], 2.5: [3]}),
            'aggregation': pd.DataFrame({'k': ['a', 'a', 'b'], 'v': [1, 2, 3]})
            .groupby('k')
            .agg(['sum', 'mean']),
            'pivot': pd.DataFrame({'k': ['a', 'b'], 'c': [1, 2], 'v': [3, 4]})
            .pivot(index='k', columns='c', values='v'),
            'timestamp_labels': pd.DataFrame({pd.Timestamp('2024-01-01'): [1]}),
            'named_columns': pd.DataFrame({'a': [1]}).rename_axis(columns='fields'),
            'multi_index_rows': pd.DataFrame(
                {'v': [1, 2]},
                index=pd.MultiIndex.from_tuples([('a', 1), ('b', 2)], names=['k', 'n']),
            ),
            'string_index': pd.DataFrame({'v': [1, 2]}, index=pd.Index(['x', 'y'], name='key')),
        }
        for name, frame in frames.items():
            with self.subTest(frame=name):
                assert_frame_equal(self.write_and_read(name, frame), frame, check_exact=True)
                assert_frame_equal(
                    self.write_and_read(name, frame, sample=True, sample_count=1),
                    frame.iloc[:1],
                    check_exact=True,
                )

    def test_columns_that_collide_as_text_raise(self):
        frame = pd.DataFrame([[1, 2]], columns=[1, '1'])

        with self.assertRaisesRegex(Exception, 'distinct as text'):
            self.write_and_read('collide', frame)

    def test_series_output_keeps_values_name_and_index(self):
        """
        A Series output skipped the column types, so dicts came back as JSON text.
        """
        series = pd.Series(
            [{'a': 1}, None, {'b': [decimal.Decimal('1.5')]}],
            index=pd.Index(['x', 'y', 'z'], name='key'),
        )

        back = self.write_and_read('series', series)

        assert_series_equal(back, series, check_exact=True)
        self.assertIsNone(back.name)

    def test_series_list_keeps_each_series(self):
        """
        Series were aligned on their index labels, so Series with different indexes
        mixed up rows, and shorter integer Series were padded to float.
        """
        series_list = [
            pd.Series([2**62 + 1, 3], index=['x', 'y'], name='big'),
            pd.Series([True, False, True], name='flag'),
            pd.Series(
                [1.5],
                index=pd.to_datetime(['2024-01-01']).tz_localize('UTC'),
                name='score',
            ),
            pd.Series(pd.Categorical([1, None]), name='codes'),
        ]

        back = self.write_and_read('series_list', series_list)

        self.assertEqual(len(back), len(series_list))
        for read, written in zip(back, series_list):
            assert_series_equal(read, written, check_exact=True)

    def test_a_block_can_switch_between_pandas_and_polars_outputs(self):
        """
        The variable type of a frame comes from the metadata of the previous output, so a
        Polars frame went to the pandas writer. A Polars output also read the column types
        left by a pandas output and cast 1.75 to 1.
        """
        pandas_frame = pd.DataFrame({'x': pd.array([1, 2], dtype='Int64')})
        polars_frame = pl.DataFrame({'x': [1.75, -2.5]})

        for frame in (pandas_frame, polars_frame, pandas_frame):
            with patch('mage_ai.data.models.manager.DataManager.writeable', return_value=False):
                with patch('mage_ai.data.models.manager.DataManager.readable', return_value=False):
                    path = os.path.join(self.repo_path, 'pipelines', 'dtypes')
                    Variable('switch', path, 'block1').write_data(frame)
                    back = Variable('switch', path, 'block1').read_data(raise_exception=True)

            if isinstance(frame, pl.DataFrame):
                assert_polars_frame_equal(back, frame)
            else:
                assert_frame_equal(back, frame)

    def test_outputs_written_by_earlier_versions_still_read(self):
        """
        Earlier versions stored Decimal columns as Arrow decimals and UUID columns as
        16 bytes, with the column types Decimal and UUID.
        """
        variable = self.variable('earlier', pd.DataFrame({'a': [1]}))
        os.makedirs(variable.variable_path, exist_ok=True)
        table = pa.table({
            'amount': pa.array([decimal.Decimal('1.10'), None], type=pa.decimal128(10, 2)),
            'key': pa.array([uuid.UUID(int=1).bytes, None], type=pa.binary(16)),
        })
        pq.write_table(table, os.path.join(variable.variable_path, DATAFRAME_PARQUET_FILE))
        with open(os.path.join(variable.variable_path, DATAFRAME_COLUMN_TYPES_FILE), 'w') as f:
            json.dump({'amount': 'Decimal', 'key': 'UUID'}, f)

        with patch('mage_ai.data.models.manager.DataManager.readable', return_value=False):
            back = variable.read_data(raise_exception=True)

        self.assertEqual(back['amount'].tolist(), [decimal.Decimal('1.10'), None])
        self.assertEqual(back['key'].tolist(), [uuid.UUID(int=1), None])

    def test_storage_reads_arrow_columns_with_filters(self):
        frame = pd.DataFrame({
            'id': range(5),
            'tags': pd.Series(
                [[i] for i in range(5)], dtype=pd.ArrowDtype(pa.list_(pa.int64())),
            ),
        })
        path = os.path.join(self.repo_path, 'arrow_filters.parquet')
        frame.to_parquet(path)

        back = LocalStorage().read_parquet(path, filters=[('id', '>=', 3)])

        self.assertEqual(back['id'].tolist(), [3, 4])
        self.assertEqual(back['tags'].tolist(), [[3], [4]])
        self.assertEqual(back['tags'].dtype, frame['tags'].dtype)
