import unittest

import numpy as np
import pandas as pd

from mage_ai.data_cleaner.column_types.column_type_detector import infer_column_types
from mage_ai.data_cleaner.column_types.constants import ColumnType
from mage_ai.shared.column_type_detector import (
    infer_column_types as infer_shared_column_types,
)
from mage_ai.shared.parsers import is_numpy_subdtype


class NullableDtypesTest(unittest.TestCase):
    def test_numeric_and_boolean_extension_columns_are_detected(self):
        for backend in ['numpy_nullable', 'pyarrow']:
            with self.subTest(backend=backend):
                frame = pd.DataFrame({
                    'count': [1, 2, 3, None],
                    'amount': [1.5, 2.5, 3.5, None],
                    'active': [True, False, True, None],
                }).convert_dtypes(dtype_backend=backend)

                self.assertEqual(infer_column_types(frame), {
                    'count': ColumnType.NUMBER,
                    'amount': ColumnType.NUMBER_WITH_DECIMALS,
                    'active': ColumnType.TRUE_OR_FALSE,
                })
                self.assertEqual(infer_shared_column_types(frame), {
                    'count': 'number', 'amount': 'number_with_decimals',
                    'active': 'true_or_false',
                })

    def test_scalar_types_are_recognized(self):
        self.assertTrue(is_numpy_subdtype(np.bool_, np.bool_))
        self.assertTrue(is_numpy_subdtype(int, np.integer))
        self.assertFalse(is_numpy_subdtype(None, np.floating))
