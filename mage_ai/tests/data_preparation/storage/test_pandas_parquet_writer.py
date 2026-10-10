"""
Pandas outputs written by the Polars Parquet writer (parallel) read back exactly as the
ones pyarrow writes: the same Arrow schema with its pandas metadata, the same values and
the same pandas frame. Frames with other column types are written by pyarrow.
"""
import decimal
import io
import os
import tempfile
import unittest
from datetime import date
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from mage_ai.data_preparation.storage import base_storage
from mage_ai.data_preparation.storage.base_storage import (
    pandas_table,
    read_pandas_parquet,
    write_pandas_parquet,
)


def frames():
    rows = 50_000
    rng = np.random.default_rng(7)
    yield 'scalars', pd.DataFrame({
        'i64': np.arange(rows),
        'i8': (np.arange(rows) % 100).astype('int8'),
        'u64': np.full(rows, 2**63 + 5, dtype='uint64'),
        'f64': np.where(rng.random(rows) < 0.1, np.nan, rng.random(rows)),
        'f32': rng.random(rows).astype('float32'),
        'Int64': pd.array(np.where(rng.random(rows) < 0.2, None, np.arange(rows)), dtype='Int64'),
        'bool': rng.random(rows) < 0.5,
        'boolean': pd.array(np.where(rng.random(rows) < 0.1, None, rng.random(rows) < 0.5),
                            dtype='boolean'),
        'string': pd.Series(rng.choice(['alpha', 'beta', None, 'δέλτα'], rows), dtype='string'),
        'ts': pd.date_range('2020-01-01', periods=rows, freq='min'),
        'ts_tz': pd.date_range('2020-03-29', periods=rows, freq='min', tz='Europe/Madrid'),
        'ts_ns': pd.date_range('2020-01-01', periods=rows, freq='ns').astype('datetime64[ns]'),
    })
    yield 'named index', pd.DataFrame(
        {'value': [1.5, -0.0, np.inf]}, index=pd.Index(['a', 'b', 'c'], name='key'),
    )
    yield 'range index from 10', pd.DataFrame({'value': [1, 2, 3]}, index=pd.RangeIndex(10, 13))
    yield 'multi index', pd.DataFrame(
        {'value': [1, 2]}, index=pd.MultiIndex.from_tuples([(1, 'x'), (2, 'y')], names=['n', 's']),
    )
    yield 'dates and decimals', pd.DataFrame({
        'day': [date(2020, 1, 1), None, date(1900, 12, 31)],
        'amount': [decimal.Decimal('1.10'), None, decimal.Decimal('-99999.99')],
    })
    yield 'all null and empty strings', pd.DataFrame({
        'empty': pd.Series(['', '', ''], dtype='string'),
        'missing': pd.Series([None, None, None], dtype='string'),
    })
    yield 'no rows', pd.DataFrame({
        'a': pd.Series([], dtype='int64'), 'b': pd.Series([], dtype='string'),
    })
    with_attrs = pd.DataFrame({'a': [1, 2]})
    with_attrs.attrs = {'source': 'unit test'}
    yield 'attrs', with_attrs
    yield 'categorical (pyarrow)', pd.DataFrame({'c': pd.Categorical(['x', 'y', 'x'])})
    yield 'lists (pyarrow)', pd.DataFrame({'l': [[1, 2], None, []]})
    yield 'durations (pyarrow)', pd.DataFrame({'d': pd.to_timedelta([1, 2, 3], unit='s')})


class PandasParquetWriterTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp()

    def assert_same(self, name, frame, written):
        expected_path = os.path.join(self.directory, 'expected.parquet')
        pq.write_table(pandas_table(frame), expected_path)
        expected = pq.read_table(expected_path)
        actual = pq.read_table(written)
        self.assertTrue(
            actual.schema.equals(expected.schema, check_metadata=True),
            f'{name}: {actual.schema} != {expected.schema}',
        )
        self.assertTrue(actual.equals(expected), name)
        if hasattr(written, 'seek'):
            written.seek(0)
        pd.testing.assert_frame_equal(read_pandas_parquet(written), frame, obj=name)

    def test_every_frame_reads_back_as_pyarrow_writes_it(self):
        for name, frame in frames():
            with self.subTest(name):
                path = os.path.join(self.directory, 'actual.parquet')
                write_pandas_parquet(frame, path)
                self.assert_same(name, frame, path)

                buffer = io.BytesIO()
                write_pandas_parquet(frame, buffer)
                buffer.seek(0)
                self.assert_same(name, frame, buffer)

    def test_only_types_that_keep_go_through_polars(self):
        written = []
        original = base_storage.pl.DataFrame.write_parquet

        def spy(self, *args, **kwargs):
            written.append(True)
            return original(self, *args, **kwargs)

        path = os.path.join(self.directory, 'spy.parquet')
        with patch.object(base_storage.pl.DataFrame, 'write_parquet', spy):
            write_pandas_parquet(pd.DataFrame({'a': [1, 2]}), path)
            self.assertEqual(len(written), 1)
            write_pandas_parquet(pd.DataFrame({'c': pd.Categorical(['x'])}), path)
            self.assertEqual(len(written), 1)

    def test_a_schema_that_does_not_match_is_written_again_by_pyarrow(self):
        frame = pd.DataFrame({'a': [1, 2, 3]})
        other = pa.schema([pa.field('a', pa.int32())])
        for destination in (os.path.join(self.directory, 'guard.parquet'), io.BytesIO()):
            with self.subTest(type(destination).__name__):
                with patch.object(base_storage.pq, 'read_schema', return_value=other):
                    write_pandas_parquet(frame, destination)
                if hasattr(destination, 'seek'):
                    destination.seek(0)
                self.assert_same('guard', frame, destination)
                if hasattr(destination, 'seek'):
                    destination.seek(0)
                metadata = pq.read_metadata(destination)
                self.assertEqual(metadata.row_group(0).column(0).compression, 'SNAPPY')
