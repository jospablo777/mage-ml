"""
Contracts for the pandas 3 / numpy 2 / polars migration.

Each test here maps to something that broke during the upgrade. They exist so a
downgrade or a partial revert fails in CI instead of in a pipeline.
"""
import pathlib
import re
import unittest
from importlib.metadata import version

import numpy as np
import pandas as pd
import polars as pl
from packaging.version import Version

from mage_ai.shared.parsers import is_numpy_subdtype, is_string_dtype

FLOORS = {
    'pandas': '3.0',
    'numpy': '2.0',
    'polars': '1.44.2',
}

# Removed in numpy 2. The migration replaced every use.
REMOVED_NUMPY_ALIASES = [
    'NaN', 'float_', 'complex_', 'string_', 'unicode_', 'int0', 'uint0', 'bool8',
    'alltrue', 'sometrue', 'product', 'cumproduct', 'in1d', 'round_',
]

SOURCE_ROOT = pathlib.Path(__file__).resolve().parents[1]


class DataStackVersionTest(unittest.TestCase):
    def test_versions_meet_floors(self):
        for package, floor in sorted(FLOORS.items()):
            with self.subTest(package=package):
                installed = version(package)
                self.assertGreaterEqual(
                    Version(installed),
                    Version(floor),
                    '%s %s is below the %s floor' % (package, installed, floor),
                )


class NumpyAliasTest(unittest.TestCase):
    def test_source_uses_no_removed_aliases(self):
        pattern = re.compile(r'\bnp\.(%s)\b' % '|'.join(REMOVED_NUMPY_ALIASES))
        offenders = []

        for path in SOURCE_ROOT.rglob('*.py'):
            if 'frontend' in path.parts or 'templates' in path.parts:
                continue
            try:
                text = path.read_text(encoding='utf-8')
            except (UnicodeDecodeError, OSError):
                continue
            for match in pattern.finditer(text):
                offenders.append('%s: np.%s' % (path.relative_to(SOURCE_ROOT), match.group(1)))

        self.assertEqual(offenders, [], 'numpy 2 removed these names')

    def test_replacements_exist(self):
        for name in ['nan', 'float64', 'complex128', 'bytes_']:
            with self.subTest(name=name):
                self.assertTrue(hasattr(np, name))


class DtypeHelperTest(unittest.TestCase):
    """
    pandas 3 gives string columns a StringDtype. np.issubdtype raises on it, and
    every `dtype == 'object'` check stops matching.
    """

    def test_is_numpy_subdtype_on_string_column(self):
        dtype = pd.Series(['a', 'b']).dtype

        self.assertFalse(is_numpy_subdtype(dtype, np.integer))
        self.assertFalse(is_numpy_subdtype(dtype, np.floating))

    def test_is_numpy_subdtype_on_numeric_columns(self):
        self.assertTrue(is_numpy_subdtype(pd.Series([1, 2]).dtype, np.integer))
        self.assertTrue(is_numpy_subdtype(pd.Series([1.5]).dtype, np.floating))
        self.assertFalse(is_numpy_subdtype(pd.Series([1, 2]).dtype, np.floating))

    def test_is_numpy_subdtype_never_raises(self):
        for dtype in [pd.Series(['a']).dtype, pd.CategoricalDtype(), object, None, 'not a dtype']:
            with self.subTest(dtype=dtype):
                self.assertIsInstance(is_numpy_subdtype(dtype, np.integer), bool)

    def test_is_string_dtype_covers_both_representations(self):
        self.assertTrue(is_string_dtype(pd.Series(['a', 'b']).dtype))
        # Mixed content still falls back to object.
        self.assertTrue(is_string_dtype(pd.Series([{'a': 1}, {'b': 2}]).dtype))
        self.assertFalse(is_string_dtype(pd.Series([1, 2]).dtype))
        self.assertFalse(is_string_dtype(pd.Series([1.5]).dtype))


class PandasBehaviorTest(unittest.TestCase):
    def test_string_columns_are_not_object(self):
        # The assumption the old detector code was built on.
        self.assertNotEqual(pd.Series(['a', 'b']).dtype, object)

    def test_groupby_apply_drops_grouping_columns(self):
        # Why BaseAction.groupby reselects every column.
        df = pd.DataFrame({'a': ['x', 'x', 'y'], 'b': [1, 2, 3]})

        without = df.groupby(['a']).apply(lambda g: g)
        self.assertNotIn('a', without.columns)

        with_keys = df.groupby(['a'], group_keys=False)[df.columns.tolist()].apply(lambda g: g)
        self.assertEqual(list(with_keys.columns), ['a', 'b'])

    def test_loc_assignment_keeps_column_dtype(self):
        # Why remove_outliers assigns the column instead of using .loc.
        df = pd.DataFrame({'a': ['1', '2']})
        with self.assertRaises(TypeError):
            df.loc[:, 'a'] = df['a'].astype(float)

        df['a'] = df['a'].astype(float)
        self.assertTrue(is_numpy_subdtype(df['a'].dtype, np.floating))

    def test_mixed_timezones_need_utc(self):
        # Why clean_series passes utc=True.
        values = pd.Series(['2000-01-01', '2000-07-01 00:00:00+00:00'])
        with self.assertRaises(ValueError):
            pd.to_datetime(values, errors='coerce', format='mixed')

        parsed = pd.to_datetime(values, errors='coerce', format='mixed', utc=True)
        self.assertEqual(str(parsed.dt.tz), 'UTC')

    def test_applymap_is_gone(self):
        self.assertFalse(hasattr(pd.DataFrame({'a': [1]}), 'applymap'))
        self.assertTrue(hasattr(pd.DataFrame({'a': [1]}), 'map'))


class RegexEngineTest(unittest.TestCase):
    def test_currency_regex_runs_on_string_columns(self):
        # pandas 3 runs string ops through a stricter engine that rejects
        # backslashes before non-special characters.
        from mage_ai.data_cleaner.transformer_actions.constants import CURRENCY_SYMBOLS

        series = pd.Series(['$1', '€2', '¥3', 'Rs4', '5'])

        self.assertEqual(series.str.count(CURRENCY_SYMBOLS).sum(), 4)
        self.assertEqual(series.str.replace(CURRENCY_SYMBOLS, '', regex=True).tolist(),
                         ['1', '2', '3', '4', '5'])

    def test_reformat_currency_patterns_run(self):
        from mage_ai.data_cleaner.cleaning_rules.reformat_values import (
            ConvertCurrencySubRule,
        )

        series = pd.Series(['$1', '2€'])
        for pattern in [ConvertCurrencySubRule.CURR_PREFIX, ConvertCurrencySubRule.CURR_SUFFIX]:
            with self.subTest(pattern=pattern):
                self.assertNotIn('\\€', pattern)
                series.str.count(pattern)


class ColumnTypeDetectionTest(unittest.TestCase):
    """The detector returned None for every string column before the migration."""

    def test_detects_types_across_a_mixed_frame(self):
        from mage_ai.data_cleaner.column_types.column_type_detector import (
            infer_column_types,
        )
        from mage_ai.data_cleaner.column_types.constants import ColumnType

        df = pd.DataFrame({
            'counter': [1, 2, 3, 4, 5, 6],
            'amount': [1.5, 2.5, 3.5, 4.5, 5.5, 6.5],
            'email': ['a@b.co', 'c@d.co', 'e@f.co', 'g@h.co', 'i@j.co', 'k@l.co'],
            'joined': ['2020-01-01', '2020-02-01', '2020-03-01',
                       '2020-04-01', '2020-05-01', '2020-06-01'],
        })

        types = infer_column_types(df)

        self.assertEqual(types['counter'], ColumnType.NUMBER)
        self.assertEqual(types['amount'], ColumnType.NUMBER_WITH_DECIMALS)
        self.assertEqual(types['email'], ColumnType.EMAIL)
        self.assertEqual(types['joined'], ColumnType.DATETIME)

    def test_no_column_is_left_undetected(self):
        from mage_ai.data_cleaner.column_types.column_type_detector import (
            infer_column_types,
        )

        df = pd.DataFrame({
            'text': ['alpha', 'beta', 'gamma', 'delta'],
            'flag': [True, False, True, False],
            'number': [10, 20, 30, 40],
        })

        self.assertNotIn(None, infer_column_types(df).values())


class PolarsTest(unittest.TestCase):
    def test_round_trip_through_pandas(self):
        df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})

        back = pl.from_pandas(df).to_pandas()

        self.assertEqual(back['a'].tolist(), [1, 2, 3])
        self.assertEqual(back['b'].tolist(), ['x', 'y', 'z'])

    def test_surfaces_mage_uses(self):
        for attr in ['DataFrame', 'LazyFrame', 'Series', 'concat', 'from_pandas', 'read_parquet']:
            with self.subTest(attr=attr):
                self.assertTrue(hasattr(pl, attr))


if __name__ == '__main__':
    unittest.main()
