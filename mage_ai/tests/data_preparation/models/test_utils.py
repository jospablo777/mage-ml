import unittest
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


def serialize_row(row: pd.Series, column_types: dict) -> pd.Series:
    # Reference rules: the per-row serializer that ran through DataFrame.apply.
    for column, column_type in column_types.items():
        val = row[column]
        if column_type in ('dict', 'list') and val is not None:
            row[column] = simplejson.dumps(val, ignore_nan=True, use_decimal=True)
        elif column_type == 'ObjectId' and val is not None:
            row[column] = str(val)
    return row


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

    def test_serialize_matches_row_rules(self):
        column_types = {'d': 'dict', 'l': 'list', 'nan_d': 'dict', 'oid': 'ObjectId'}
        expected = self.frame().apply(lambda row: serialize_row(row, column_types), axis=1)

        actual = serialize_columns(self.frame(), column_types)

        for column in column_types:
            with self.subTest(column=column):
                self.assertEqual(
                    [None if pd.isna(v) else v for v in actual[column]],
                    [None if pd.isna(v) else v for v in expected[column]],
                )
        self.assertEqual(actual['nan_d'].tolist()[1], 'null')

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
