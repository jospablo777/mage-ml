import datetime as dt
import decimal
import json
import math
import os
import tempfile
import uuid

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.feather as feather

from mage_ai.data_preparation.models.block.r import exchange
from mage_ai.tests.base_test import TestCase


class ExchangeTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.job_dir = tempfile.mkdtemp()

    def write(self, value):
        entries, warnings = exchange.write_inputs([value], self.job_dir)
        entry = entries[0]
        path = os.path.join(self.job_dir, entry['path'])
        if entry['kind'] == 'frame':
            return entry, feather.read_table(path), warnings
        with open(path) as file:
            return entry, json.load(file), warnings

    def write_output(self, table=None, json_columns=(), value=None, kind='frame'):
        out = os.path.join(self.job_dir, 'output')
        os.makedirs(out, exist_ok=True)
        if kind == 'frame':
            feather.write_feather(table, os.path.join(out, 'data.arrow'))
        elif kind == 'json':
            with open(os.path.join(out, 'data.json'), 'w') as file:
                file.write(value)
        with open(os.path.join(out, 'manifest.json'), 'w') as file:
            json.dump(dict(kind=kind, json_columns=list(json_columns)), file)


class WriteInputsTest(ExchangeTestCase):
    def test_types_r_reads_exactly_are_kept(self):
        frame = pd.DataFrame({
            'big': pd.array([2**53 + 1, None], dtype='Int64'),
            'text': pd.Series(['ñ', None], dtype='str'),
            'day': [dt.date(2024, 2, 29), None],
            'at': pd.to_datetime(['2024-01-01 12:00:00.123456', None]),
            'ints': [[1, None], []],
        })

        entry, table, warnings = self.write(frame)

        self.assertEqual(entry['json_columns'], [])
        self.assertEqual(warnings, [])
        self.assertEqual(table.column('big').to_pylist(), [2**53 + 1, None])
        self.assertEqual(table.column('text').to_pylist(), ['ñ', None])
        self.assertEqual(table.column('day').type, pa.date32())
        self.assertEqual(table.column('ints').to_pylist(), [[1, None], []])

    def test_types_r_cannot_read_are_cast(self):
        table = pa.table({
            'view': pa.array(['a', None], pa.string_view()),
            'half': pa.array([1.5, None], pa.float16()),
            'small_u64': pa.array([5, None], pa.uint64()),
            'big_u64': pa.array([2**63 + 5, None], pa.uint64()),
            'views': pa.array([['a'], None], pa.list_(pa.string_view())),
            'fixed': pa.array([[1, 2], None], pa.list_(pa.int64(), 2)),
        })

        _, result, _ = self.write(table)

        self.assertEqual(result.schema.field('view').type, pa.large_string())
        self.assertEqual(result.schema.field('half').type, pa.float32())
        self.assertEqual(result.column('half').to_pylist(), [1.5, None])
        self.assertEqual(result.schema.field('small_u64').type, pa.int32())
        self.assertEqual(result.column('big_u64').to_pylist(), [str(2**63 + 5), None])
        self.assertEqual(result.schema.field('views').type, pa.list_(pa.large_string()))
        self.assertEqual(result.column('fixed').to_pylist(), [[1, 2], None])

    def test_integers_that_fit_become_r_integers(self):
        frame = pd.DataFrame({
            'small': pd.array([1, None, 2**31 - 1], dtype='Int64'),
            'r_na': pd.array([-(2**31), 0, 1], dtype='Int64'),
            'big': [2**53 + 1, 0, 1],
            'empty': pd.array([None, None, None], dtype='Int64'),
        })

        _, table, _ = self.write(frame)

        self.assertEqual(table.schema.field('small').type, pa.int32())
        # -2**31 is R's NA_integer_.
        self.assertEqual(table.schema.field('r_na').type, pa.int64())
        self.assertEqual(table.schema.field('big').type, pa.int64())
        self.assertEqual(table.schema.field('empty').type, pa.int32())

    def test_structs_and_maps_cross_as_json(self):
        table = pa.table({
            'record': pa.array([{'a': 1, 'b': 'x'}, None]),
            'records': pa.array([[{'a': 1}, None], None]),
            'map': pa.array([[('k', 1)], None], pa.map_(pa.string(), pa.int64())),
        })

        entry, result, _ = self.write(table)

        self.assertEqual(entry['json_columns'], ['record', 'records', 'map'])
        self.assertEqual(
            [None if v is None else json.loads(v) for v in result.column('record').to_pylist()],
            [{'a': 1, 'b': 'x'}, None],
        )
        self.assertEqual(json.loads(result.column('records')[0].as_py()), [{'a': 1}, None])

    def test_mixed_values_cross_as_json(self):
        frame = pd.DataFrame({'mixed': [1, 'a', {'b': [1, 2]}, None]})

        entry, result, _ = self.write(frame)

        self.assertEqual(entry['json_columns'], ['mixed'])
        self.assertEqual(result.column('mixed').to_pylist(), ['1', '"a"', '{"b": [1, 2]}', None])

    def test_warnings_for_values_r_reads_approximately(self):
        table = pa.table({
            'amount': pa.array([decimal.Decimal('1.5')], pa.decimal128(38, 9)),
            'small': pa.array([decimal.Decimal('1.5')], pa.decimal128(10, 2)),
            'n': pa.array([-(2**63)], pa.int64()),
        })

        _, _, warnings = self.write(table)

        self.assertEqual(len(warnings), 2)
        self.assertIn('amount is decimal128(38, 9)', warnings[0])
        self.assertIn('n holds -2**63', warnings[1])

    def test_uuids_cross_as_text(self):
        key = uuid.UUID('12345678-1234-5678-1234-567812345678')
        frame = pd.DataFrame({'key': [key, None], 'mixed': [key, 1]})

        entry, table, _ = self.write(frame)

        self.assertEqual(table.column('key').to_pylist(), [str(key), None])
        # encode_complex returns a UUID unchanged; it is written as its text.
        self.assertEqual(entry['json_columns'], ['mixed'])
        self.assertEqual(table.column('mixed').to_pylist(), [json.dumps(str(key)), '1'])

    def test_named_index_becomes_a_column(self):
        frame = pd.DataFrame({'v': [1, 2]}, index=pd.Index(['a', 'b'], name='key'))
        unnamed = pd.DataFrame({'v': [1, 2]}, index=[5, 7])

        self.assertEqual(self.write(frame)[1].column_names, ['key', 'v'])
        self.assertEqual(self.write(unnamed)[1].column_names, ['v'])

    def test_polars_frames(self):
        frame = pl.DataFrame({'text': ['a', None], 'n': [2**63 - 1, None]})

        _, table, _ = self.write(frame.lazy())

        self.assertEqual(table.schema.field('text').type, pa.large_string())
        self.assertEqual(table.column('n').to_pylist(), [2**63 - 1, None])

    def test_series_become_frames(self):
        self.assertEqual(self.write(pd.Series([1, 2], name='x'))[1].column_names, ['x'])
        self.assertEqual(self.write(pd.Series([1, 2]))[1].column_names, ['value'])

    def test_duplicate_column_names_raise(self):
        frame = pd.DataFrame([[1, 2]], columns=['a', 'a'])

        with self.assertRaisesRegex(ValueError, 'a appear more than once'):
            self.write(frame)

    def test_other_values_cross_as_json(self):
        entry, value, _ = self.write(
            {'a': np.int64(1), 'at': dt.datetime(2024, 1, 1), 'f': math.nan},
        )

        self.assertEqual(entry['kind'], 'json')
        self.assertEqual(value, {'a': 1, 'at': '2024-01-01T00:00:00', 'f': None})


class Int64MarkerTest(TestCase):
    def test_integers_a_double_cannot_hold_are_marked(self):
        value = {'small': 2**53, 'big': 2**53 + 1, 'negative': -(2**63),
                 'list': [1, 2**62, None], 'flag': True, 'f': 1.5}

        self.assertEqual(json.loads(exchange.json_for_r(value)), {
            'small': 2**53,
            'big': {'$int64': str(2**53 + 1)},
            'negative': {'$int64': str(-(2**63))},
            'list': [1, {'$int64': str(2**62)}, None],
            'flag': True,
            'f': 1.5,
        })

    def test_json_columns_and_values_are_marked(self):
        frame = pd.DataFrame({'doc': [{'n': 2**60}, None]})

        entry, table, _ = WriteInputsTest.write(self, frame)
        self.assertEqual(json.loads(table.column('doc')[0].as_py()), {'n': {'$int64': str(2**60)}})

        _, value, _ = WriteInputsTest.write(self, {'n': 2**60})
        self.assertEqual(value, {'n': {'$int64': str(2**60)}})

    def setUp(self):
        super().setUp()
        self.job_dir = tempfile.mkdtemp()


class GlobalsTest(TestCase):
    def test_globals(self):
        class Opaque:
            pass

        values, skipped = exchange.globals_for_r({
            'execution_date': dt.datetime(2024, 1, 1, 12),
            'quote': "it's \"quoted\"",
            'n': 2**53 + 1,
            'nested': {'a': [1, None]},
            'logger': object(),
            'circular': (lambda d: d.update(me=d) or d)({}),
        })

        self.assertEqual(values, {
            'execution_date': '2024-01-01T12:00:00',
            'quote': "it's \"quoted\"",
            'n': {'$int64': str(2**53 + 1)},
            'nested': {'a': [1, None]},
        })
        self.assertEqual(skipped, ['circular'])
        self.assertIsNotNone(Opaque)


class ReadOutputTest(ExchangeTestCase):
    def test_frames_get_nullable_types(self):
        self.write_output(pa.table({
            'i32': pa.array([1, None], pa.int32()),
            'i64': pa.array([2**53 + 1, None], pa.int64()),
            'flag': pa.array([True, None]),
            'day': pa.array([dt.date(2024, 1, 1), None]),
            'at': pa.array([dt.datetime(2024, 1, 1), None], pa.timestamp('us', tz='UTC')),
            'span': pa.array([dt.timedelta(seconds=1.5), None], pa.duration('us')),
            'time': pa.array([dt.time(12, 0, 0, 500000), None], pa.time64('us')),
            'tags': pa.array([['a'], []]),
            'factor': pa.array(['x', None]).dictionary_encode(),
            'text': pa.array(['ñ', None]),
        }))

        returned, frame = exchange.read_output(self.job_dir)

        self.assertTrue(returned)
        # R's integers come back as Int64.
        self.assertEqual(str(frame['i32'].dtype), 'Int64')
        self.assertEqual(frame['i64'].tolist(), [2**53 + 1, pd.NA])
        self.assertEqual(str(frame['flag'].dtype), 'boolean')
        self.assertEqual(frame['day'][0], dt.date(2024, 1, 1))
        self.assertEqual(str(frame['at'].dtype), 'datetime64[us, UTC]')
        self.assertEqual(frame['span'][0], pd.Timedelta(seconds=1.5))
        self.assertEqual(frame['time'][0], dt.time(12, 0, 0, 500000))
        self.assertEqual(frame['tags'].tolist(), [['a'], []])
        self.assertEqual(str(frame['factor'].dtype), 'category')
        self.assertEqual(frame['text'][0], 'ñ')

    def test_json_columns_are_decoded(self):
        self.write_output(
            pa.table({'id': [1, 2], 'doc': ['{"a": [1, null]}', None]}), json_columns=['doc'],
        )

        _, frame = exchange.read_output(self.job_dir)

        self.assertEqual(frame.columns.tolist(), ['id', 'doc'])
        self.assertEqual(frame['doc'].tolist(), [{'a': [1, None]}, None])

    def test_json_values(self):
        self.write_output(value='{"a": [1, 2], "b": null}', kind='json')

        self.assertEqual(exchange.read_output(self.job_dir), (True, {'a': [1, 2], 'b': None}))

    def test_no_value(self):
        self.assertEqual(exchange.read_output(self.job_dir), (False, None))
        self.write_output(kind='none')
        self.assertEqual(exchange.read_output(self.job_dir), (False, None))
