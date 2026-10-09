import datetime as dt
import io
import json
import unittest

import pandas as pd
import polars as pl

from mage_integrations.utils.frames import (
    batch_file_name,
    frame_from_records,
    nested_column_type,
    nested_values_as_json,
    read_file_frame,
    records_from_frame,
)


class FrameFromRecordsTest(unittest.TestCase):
    def test_integer_columns_with_nulls_stay_integers(self):
        records = [{'n': 2**53 + 1, 'm': 1}, {'n': None, 'm': None}]

        typed = frame_from_records(records, {'n': {'type': ['null', 'integer']}})
        inferred = frame_from_records(records)

        for frame in (typed, inferred):
            self.assertEqual(str(frame['n'].dtype), 'Int64')
            self.assertEqual(frame['n'][0], 2**53 + 1)
            self.assertTrue(pd.isna(frame['n'][1]))
        self.assertEqual(str(inferred['m'].dtype), 'Int64')

    def test_other_columns_keep_pandas_types(self):
        frame = frame_from_records([
            {'f': 1.5, 'b': True, 's': 'a', 'mixed': 1},
            {'f': None, 'b': None, 's': None, 'mixed': 'x'},
        ])

        self.assertEqual(frame['f'].dtype, 'float64')
        self.assertEqual(frame['b'].tolist(), [True, None])
        self.assertEqual(frame['mixed'].tolist(), [1, 'x'])

    def test_integers_out_of_int64_range_keep_their_values(self):
        frame = frame_from_records([{'n': 2**64 - 1}, {'n': None}])

        self.assertEqual(frame['n'][0], 2**64 - 1)

    def test_missing_keys_are_nulls(self):
        frame = frame_from_records([{'a': 1}, {'b': 'x'}])

        self.assertEqual(list(frame.columns), ['a', 'b'])
        self.assertEqual(len(frame), 2)


class RecordsFromFrameTest(unittest.TestCase):
    def test_arrow_lists_keep_integers_and_nulls(self):
        frame = pl.DataFrame({
            'ints': [[1, None], None],
            'f': [float('nan'), 1.5],
            'record': [{'a': 1}, None],
        }).to_pandas(use_pyarrow_extension_array=True)

        self.assertEqual(records_from_frame(frame), [
            {'ints': [1, None], 'f': None, 'record': {'a': 1}},
            {'ints': None, 'f': 1.5, 'record': None},
        ])

    def test_numpy_columns_use_none_for_missing_values(self):
        frame = pd.DataFrame({'s': pd.Series(['a', None], dtype='str'), 'f': [1.0, None]})

        self.assertEqual(records_from_frame(frame), [
            {'s': 'a', 'f': 1.0}, {'s': None, 'f': None},
        ])


class NestedValuesAsJsonTest(unittest.TestCase):
    def test_dicts_and_lists_become_json(self):
        frame = pd.DataFrame({'v': [{'a': None}, [1, 2], 'text', None]})

        values = nested_values_as_json(frame)['v'].tolist()

        self.assertEqual(json.loads(values[0]), {'a': None})
        self.assertEqual(values[1:3], ['[1, 2]', 'text'])
        # pandas 3 infers the str dtype, whose missing value is NaN.
        self.assertTrue(pd.isna(values[3]))


class BatchFileNameTest(unittest.TestCase):
    def test_names_in_the_same_second_differ(self):
        time = dt.datetime(2024, 1, 1, 12, 0, 0, 123456)

        first, second = batch_file_name(time, 'parquet'), batch_file_name(time, 'parquet')

        self.assertNotEqual(first, second)
        self.assertTrue(first.startswith('20240101-120000-123456-'))
        self.assertTrue(first.endswith('.parquet'))


class ReadFileFrameTest(unittest.TestCase):
    def test_csv_keeps_the_int64_minimum_and_null_rows(self):
        content = b'n\n1\n\n-9223372036854775808\n'

        frame = read_file_frame(content, 'csv')

        self.assertEqual(frame['n'].tolist(), [1, pd.NA, -(2**63)])

    def test_csv_in_another_encoding(self):
        content = 'name,n\nñandú,1\n'.encode('latin-1')

        frame = read_file_frame(content, 'csv', encoding='latin-1')

        self.assertEqual(frame['name'].tolist(), ['ñandú'])

    def test_parquet_with_fixed_size_lists_holding_nulls(self):
        buffer = io.BytesIO()
        pl.DataFrame(
            {'v': [[1.0, 2.0], None]}, schema={'v': pl.Array(pl.Float64, 2)},
        ).write_parquet(buffer)

        frame = read_file_frame(buffer.getvalue(), 'parquet')

        self.assertEqual(nested_column_type(frame['v'].dtype), 'array')
        self.assertEqual(records_from_frame(frame), [{'v': [1.0, 2.0]}, {'v': None}])
