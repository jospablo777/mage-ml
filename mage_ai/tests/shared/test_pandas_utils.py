import sys
import types
import warnings
from unittest.mock import patch

import numpy as np
import pandas as pd

from mage_ai.shared.pandas_utils import (
    datetime_resolution,
    datetime_to_epoch_seconds,
    get_setting_with_copy_warning,
    ignore_setting_with_copy_warning,
    integer_bit_width,
    timedelta_to_nanoseconds,
)
from mage_ai.tests.base_test import TestCase


class FakeSettingWithCopyWarning(Warning):
    pass


def build_module(name: str, warning_class=None) -> types.ModuleType:
    module = types.ModuleType(name)
    if warning_class is not None:
        module.SettingWithCopyWarning = warning_class
    return module


class PandasUtilsTests(TestCase):
    def test_get_setting_with_copy_warning_from_pandas_errors(self):
        """pandas 1.5.0 through 2.x expose the class from pandas.errors."""
        modules = {
            'pandas.errors': build_module('pandas.errors', FakeSettingWithCopyWarning),
        }
        with patch.dict(sys.modules, modules):
            self.assertIs(get_setting_with_copy_warning(), FakeSettingWithCopyWarning)

    def test_get_setting_with_copy_warning_from_pandas_core_common(self):
        """pandas before 1.5.0 only exposes the class from pandas.core.common."""
        modules = {
            'pandas.errors': build_module('pandas.errors'),
            'pandas.core.common': build_module(
                'pandas.core.common', FakeSettingWithCopyWarning
            ),
        }
        with patch.dict(sys.modules, modules):
            self.assertIs(get_setting_with_copy_warning(), FakeSettingWithCopyWarning)

    def test_get_setting_with_copy_warning_removed_in_pandas_3(self):
        """pandas 3.0 dropped the class when copy-on-write became the default."""
        modules = {
            'pandas.errors': build_module('pandas.errors'),
            'pandas.core.common': build_module('pandas.core.common'),
        }
        with patch.dict(sys.modules, modules):
            self.assertIsNone(get_setting_with_copy_warning())

    def test_get_setting_with_copy_warning_without_pandas(self):
        modules = {
            'pandas': None,
            'pandas.errors': None,
            'pandas.core.common': None,
        }
        with patch.dict(sys.modules, modules):
            self.assertIsNone(get_setting_with_copy_warning())

    def test_get_setting_with_copy_warning_ignores_non_warning_attribute(self):
        modules = {
            'pandas.errors': build_module('pandas.errors', 'not a warning class'),
            'pandas.core.common': build_module('pandas.core.common'),
        }
        with patch.dict(sys.modules, modules):
            self.assertIsNone(get_setting_with_copy_warning())

    def test_ignore_setting_with_copy_warning_installs_filter(self):
        modules = {
            'pandas.errors': build_module('pandas.errors', FakeSettingWithCopyWarning),
        }
        with patch.dict(sys.modules, modules), warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            self.assertTrue(ignore_setting_with_copy_warning())
            warnings.warn('ignored', FakeSettingWithCopyWarning, stacklevel=2)

        self.assertEqual(caught, [])

    def test_ignore_setting_with_copy_warning_is_a_noop_in_pandas_3(self):
        modules = {
            'pandas.errors': build_module('pandas.errors'),
            'pandas.core.common': build_module('pandas.core.common'),
        }
        with patch.dict(sys.modules, modules):
            self.assertFalse(ignore_setting_with_copy_warning())

    def test_ignore_setting_with_copy_warning_with_installed_pandas(self):
        """Whatever pandas is installed, this must never raise an ImportError."""
        with warnings.catch_warnings():
            self.assertIsInstance(ignore_setting_with_copy_warning(), bool)


class DatetimeResolutionTests(TestCase):
    """pandas 3 keeps the source resolution instead of casting everything to ns."""

    def test_reads_the_unit_of_a_tz_aware_dtype(self):
        series = pd.Series(pd.to_datetime(['2026-07-20 18:15:34.258586+00:00'], utc=True))

        self.assertEqual(datetime_resolution(series.dtype), 'us')

    def test_reads_the_unit_of_a_naive_dtype(self):
        # A naive column carries a plain numpy dtype, which has no .unit attribute.
        series = pd.Series(pd.to_datetime(['2026-07-20 18:15:34']))

        self.assertEqual(datetime_resolution(series.dtype), 'us')

    def test_falls_back_to_nanoseconds(self):
        for dtype in (object, 'not a dtype', None):
            with self.subTest(dtype=dtype):
                self.assertEqual(datetime_resolution(dtype), 'ns')


class DatetimeToEpochSecondsTests(TestCase):
    def test_same_result_at_every_resolution(self):
        expected = 1784571334.258586

        for unit in ('us', 'ns'):
            with self.subTest(unit=unit):
                series = pd.Series(
                    pd.to_datetime(['2026-07-20 18:15:34.258586+00:00'], utc=True).as_unit(unit)
                )

                self.assertAlmostEqual(datetime_to_epoch_seconds(series).iloc[0], expected, 6)

    def test_second_resolution_is_not_divided_by_a_billion(self):
        series = pd.Series(pd.to_datetime(['2026-01-01'], utc=True).as_unit('s'))

        self.assertEqual(datetime_to_epoch_seconds(series).iloc[0], 1767225600.0)

    def test_missing_values_stay_missing(self):
        series = pd.Series(pd.to_datetime(['2026-01-01', None], utc=True))

        result = datetime_to_epoch_seconds(series)

        self.assertEqual(result.iloc[0], 1767225600.0)
        self.assertTrue(pd.isna(result.iloc[1]))

    def test_naive_and_aware_columns_agree(self):
        aware = pd.Series(pd.to_datetime(['2026-01-01 00:00:00+00:00'], utc=True))
        naive = pd.Series(pd.to_datetime(['2026-01-01 00:00:00']))

        self.assertEqual(
            datetime_to_epoch_seconds(aware).iloc[0],
            datetime_to_epoch_seconds(naive).iloc[0],
        )


class TimedeltaToNanosecondsTests(TestCase):
    def test_converts_from_the_native_resolution(self):
        series = pd.Series(pd.to_timedelta(['1 days', '0 days 00:00:01.5']))

        self.assertEqual(timedelta_to_nanoseconds(series).tolist(), [86400000000000, 1500000000])

    def test_result_is_an_integer_column(self):
        series = pd.Series(pd.to_timedelta(['1 days']))

        self.assertTrue(pd.api.types.is_integer_dtype(timedelta_to_nanoseconds(series)))


class IntegerBitWidthTests(TestCase):
    def test_picks_the_smallest_width(self):
        self.assertEqual(integer_bit_width(0, 1), 16)
        self.assertEqual(integer_bit_width(-32768, 32767), 16)
        self.assertEqual(integer_bit_width(0, 32768), 32)
        self.assertEqual(integer_bit_width(0, 2**31), 64)
        self.assertEqual(integer_bit_width(-(2**40), 2**40), 64)

    def test_accepts_python_ints_that_do_not_fit(self):
        """numpy 2 raises OverflowError when narrowing these, so no cast is used."""
        self.assertEqual(integer_bit_width(0, 345100), 32)

    def test_accepts_numpy_scalars(self):
        self.assertEqual(integer_bit_width(np.int64(0), np.int64(345100)), 32)

    def test_missing_bounds_fall_back_to_the_widest(self):
        for low, high in ((None, None), (np.nan, np.nan), ('a', 'b')):
            with self.subTest(bounds=(low, high)):
                self.assertEqual(integer_bit_width(low, high), 64)
