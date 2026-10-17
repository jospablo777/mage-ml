import unittest
import warnings

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.preprocessing import StandardScaler

from mage_ai.shared.complex import is_model_sklearn


class SklearnModelDetectionTest(unittest.TestCase):
    def test_values_are_rejected_without_estimator_tag_warnings(self):
        values = [None, False, 1, 'value', {}, [], np.array([1]), pd.DataFrame({'a': [1]})]
        with warnings.catch_warnings():
            warnings.simplefilter('error', DeprecationWarning)
            for value in values:
                with self.subTest(value=type(value)):
                    self.assertFalse(is_model_sklearn(value))

    def test_estimator_instances_are_recognized(self):
        for model in [BaseEstimator(), LogisticRegression(), LinearRegression(), StandardScaler()]:
            with self.subTest(model=type(model)):
                self.assertTrue(is_model_sklearn(model))
                self.assertFalse(is_model_sklearn(type(model)))

    def test_legacy_third_party_estimator_is_recognized(self):
        class LegacyClassifier:
            _estimator_type = 'classifier'

        self.assertTrue(is_model_sklearn(LegacyClassifier()))

    def test_third_party_estimator_tags_are_recognized(self):
        class TaggedClassifier:
            def __sklearn_tags__(self):
                return LogisticRegression().__sklearn_tags__()

        self.assertTrue(is_model_sklearn(TaggedClassifier()))


class SklearnCheckImportTest(unittest.TestCase):
    def test_checking_a_value_does_not_import_scikit_learn(self):
        # Serializing a pipeline imported scikit-learn on the server's event loop (0.7 s).
        import subprocess
        import sys

        script = (
            'import sys\n'
            'from mage_ai.shared.complex import is_model_sklearn\n'
            'from mage_ai.shared.parsers import encode_complex\n'
            'print(is_model_sklearn(object()), encode_complex({1, 2}) is not None,\n'
            "      'sklearn' in sys.modules)\n"
        )
        out = subprocess.run([sys.executable, '-c', script], capture_output=True, text=True,
                             check=True).stdout.split()
        self.assertEqual(out[0], 'False')
        self.assertEqual(out[-1], 'False')
