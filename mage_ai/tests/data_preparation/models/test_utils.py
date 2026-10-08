import datetime
import decimal
import math
import unittest
import uuid
from unittest.mock import patch

import numpy as np
import pandas as pd
import polars as pl
import simplejson
from scipy.sparse import csr_matrix
from sklearn.linear_model import LinearRegression

from mage_ai.data_preparation.models.project.constants import FeatureUUID
from mage_ai.data_preparation.models.utils import (
    deserialize_columns,
    infer_variable_type,
    serialize_columns,
)
from mage_ai.data_preparation.models.variables.constants import VariableType
from mage_ai.tests.base_test import TestCase


class TestModelUtils(TestCase):
    def setUp(self):
        self.repo_path = self.repo_path
        self.pipeline_uuid = 'pipeline_uuid'
        self.block_uuid = 'block_uuid'
        self.variable_uuid = 'variable_uuid'
        self.partition = 'partition'
        self.storage = 'storage'
        self.clean_block_uuid = 'clean_block_uuid'

    def test_polars_dataframe(self):
        data = pl.DataFrame({'a': [1, 2, 3]})
        with patch(
            'mage_ai.data_preparation.models.project.Project.is_feature_enabled',
            lambda _, feature_uuid: FeatureUUID.POLARS == feature_uuid,
        ):
            variable_type_use, basic_iterable = infer_variable_type(data)
            self.assertEqual(variable_type_use, VariableType.POLARS_DATAFRAME)

    def test_pandas_dataframe(self):
        data = pd.DataFrame({'a': [1, 2, 3]})
        variable_type_use, basic_iterable = infer_variable_type(data)
        self.assertEqual(variable_type_use, VariableType.DATAFRAME)

    def test_sparse_matrix(self):
        data = csr_matrix([[0, 0, 1], [1, 0, 0], [0, 1, 0]])
        variable_type_use, basic_iterable = infer_variable_type(data)
        self.assertEqual(variable_type_use, VariableType.MATRIX_SPARSE)

    def test_pandas_series(self):
        data = pd.Series([1, 2, 3])
        variable_type_use, basic_iterable = infer_variable_type(data)
        self.assertEqual(variable_type_use, VariableType.SERIES_PANDAS)

    def test_polars_series(self):
        data = pl.Series([1, 2, 3])
        variable_type_use, basic_iterable = infer_variable_type(data)
        self.assertEqual(variable_type_use, VariableType.SERIES_POLARS)

    def test_sklearn_model(self):
        data = LinearRegression()
        variable_type_use, basic_iterable = infer_variable_type(data)
        self.assertEqual(variable_type_use, VariableType.MODEL_SKLEARN)

    def test_list_complex(self):
        data = [TestCase(), TestCase()]
        variable_type_use, basic_iterable = infer_variable_type(data)
        self.assertEqual(variable_type_use, VariableType.LIST_COMPLEX)

    def test_dictionary_complex(self):
        data = {'a': TestCase(), 'b': TestCase()}
        variable_type_use, basic_iterable = infer_variable_type(data)
        self.assertEqual(variable_type_use, VariableType.DICTIONARY_COMPLEX)

    def test_custom_object(self):
        class CustomObject:
            def __init__(self):
                self.a = 1

        data = CustomObject()
        variable_type_use, basic_iterable = infer_variable_type(data)
        self.assertEqual(variable_type_use, VariableType.CUSTOM_OBJECT)

    def test_iterable(self):
        data = [1, 2, 3]
        variable_type_use, basic_iterable = infer_variable_type(data)
        self.assertEqual(variable_type_use, VariableType.ITERABLE)


def deserialize_row(row: pd.Series, column_types: dict) -> pd.Series:
    # Reference rules: the per-row deserializer that ran through DataFrame.apply.
    for column, column_type in column_types.items():
        if column_type not in ('dict', 'list'):
            continue
        val = row[column]
        if isinstance(val, str):
            row[column] = simplejson.loads(val)
        elif isinstance(val, np.ndarray) and column_type == 'list':
            row[column] = list(val)
    return row


class ColumnSerializationTest(unittest.TestCase):
    def frame(self) -> pd.DataFrame:
        return pd.DataFrame({
            'ni': pd.array([1, None, 3], dtype='Int64'),
            'cat': pd.Categorical(['x', 'y', 'x']),
            'd': [{'a': 1}, None, {'b': [1, 2.5]}],
            'l': [[1, 2], None, []],
            'nan_d': [{'a': 1}, np.nan, None],
            'oid': [7, None, 'abc'],
        })

    def test_serialize_writes_json_and_strings(self):
        column_types = {'d': 'dict', 'l': 'list', 'nan_d': 'dict', 'oid': 'ObjectId'}

        actual = serialize_columns(self.frame(), column_types)

        self.assertEqual(actual['d'].tolist(), ['{"a": 1}', None, '{"b": [1, 2.5]}'])
        self.assertEqual(actual['l'].tolist(), ['[1, 2]', None, '[]'])
        # A NaN in place of the whole value is the missing marker and is stored as NULL.
        self.assertEqual(actual['nan_d'].tolist(), ['{"a": 1}', None, None])
        self.assertEqual(actual['oid'].tolist(), ['7', None, 'abc'])

    def test_json_columns_round_trip(self):
        column_types = {'d': 'dict', 'l': 'list', 'nan_d': 'dict'}
        original = self.frame()

        back = deserialize_columns(serialize_columns(self.frame(), column_types), column_types)

        self.assertEqual(back['d'].tolist(), original['d'].tolist())
        self.assertEqual(back['l'].tolist(), original['l'].tolist())
        self.assertEqual(back['nan_d'].tolist(), [{'a': 1}, None, None])

    def test_values_without_a_json_type_round_trip_inside_dicts_and_lists(self):
        values = [
            decimal.Decimal('123456789012345678901234567890.123456789'),
            datetime.datetime(2024, 1, 2, 3, 4, 5, 6),
            datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
            datetime.date(1, 1, 1),
            datetime.time(23, 59, 59, 999999),
            datetime.timedelta(days=-3, microseconds=1),
            b'\x00\xff\x00',
            uuid.UUID(int=7),
            float('nan'),
            float('inf'),
            None,
        ]
        df = pd.DataFrame({
            'l': pd.Series([values, None], dtype=object),
            'd': pd.Series([{'k': values}, None], dtype=object),
        })
        column_types = {'l': 'list', 'd': 'dict'}

        back = deserialize_columns(serialize_columns(df, column_types), column_types)

        restored = back['l'].tolist()[0]
        self.assertEqual(restored[:8], values[:8])
        self.assertTrue(math.isnan(restored[8]))
        self.assertEqual(restored[9:], values[9:])
        self.assertEqual(back['d'].tolist()[0]['k'][:8], values[:8])
        self.assertIsNone(back['l'].tolist()[1])

    def test_json_written_before_the_type_tags_reads_the_same(self):
        df = pd.DataFrame({'d': ['{"a": 1.5, "when": "2024-01-01"}', None]})

        back = deserialize_columns(df, {'d': 'dict'})

        self.assertEqual(back['d'].tolist(), [{'a': 1.5, 'when': '2024-01-01'}, None])

    def test_text_column_types_round_trip(self):
        offset = datetime.timezone(datetime.timedelta(hours=-6))
        df = pd.DataFrame({
            'dec': pd.Series(
                [decimal.Decimal('1E-50'), None, decimal.Decimal('NaN')], dtype=object,
            ),
            'f': pd.Series([1.5, float('nan'), None], dtype=object),
            'tz': pd.Series([datetime.time(12, 0, tzinfo=offset), None, None], dtype=object),
        })
        column_types = {'dec': 'Decimal', 'f': 'float_with_nan', 'tz': 'timetz'}

        back = deserialize_columns(serialize_columns(df.copy(), column_types), column_types)

        self.assertEqual(back['dec'].tolist()[:2], [decimal.Decimal('1E-50'), None])
        self.assertTrue(back['dec'].tolist()[2].is_nan())
        self.assertEqual(back['f'].tolist()[0], 1.5)
        self.assertTrue(math.isnan(back['f'].tolist()[1]))
        self.assertIsNone(back['f'].tolist()[2])
        self.assertEqual(back['tz'].tolist(), [datetime.time(12, 0, tzinfo=offset), None, None])

    def test_serialize_leaves_other_columns_untouched(self):
        df = serialize_columns(self.frame(), {'d': 'dict'})

        self.assertEqual(df['ni'].dtype, pd.Int64Dtype())
        self.assertIsInstance(df['cat'].dtype, pd.CategoricalDtype)

    def test_deserialize_matches_row_rules(self):
        df = pd.DataFrame({
            'd': ['{"a": 1}', None, '{"b": [1, 2]}'],
            'l': [np.array([1, 2]), None, '[3]'],
            'd_array': [np.array([1]), None, '{"c": 3}'],
        })
        column_types = {'d': 'dict', 'l': 'list', 'd_array': 'dict'}
        expected = df.copy().apply(lambda row: deserialize_row(row, column_types), axis=1)

        actual = deserialize_columns(df.copy(), column_types)

        for column in column_types:
            with self.subTest(column=column):
                self.assertEqual(
                    [None if v is None or v is np.nan else v for v in actual[column]][::2],
                    [None if v is None or v is np.nan else v for v in expected[column]][::2],
                )
        self.assertEqual(actual['l'].tolist()[0], [1, 2])
        self.assertIsInstance(actual['d_array'].tolist()[0], np.ndarray)

    def test_deserialize_keeps_other_column_dtypes(self):
        df = pd.DataFrame({
            'ni': pd.array([1, None], dtype='Int64'),
            'cat': pd.Categorical(['x', 'y']),
            'tz': pd.to_datetime(['2024-01-01', '2024-01-02']).tz_localize('UTC'),
            'd': ['{"a": 1}', None],
        })

        out = deserialize_columns(df, {'d': 'dict'})

        self.assertEqual(out['ni'].dtype, pd.Int64Dtype())
        self.assertIsInstance(out['cat'].dtype, pd.CategoricalDtype)
        self.assertEqual(str(out['tz'].dtype), 'datetime64[us, UTC]')
        self.assertEqual(out['d'].tolist()[0], {'a': 1})
        self.assertTrue(pd.isna(out['d'].tolist()[1]))

    def test_deserialize_skips_missing_columns(self):
        df = pd.DataFrame({'a': [1]})

        out = deserialize_columns(df, {'a': 'int64', 'gone': 'dict'})

        self.assertEqual(out.columns.tolist(), ['a'])
