import warnings
from typing import Optional, Type

import numpy as np
import pandas as pd


def get_setting_with_copy_warning() -> Optional[Type[Warning]]:
    """
    Return pandas' SettingWithCopyWarning class, or None when pandas does not expose it.

    The class moved around between releases: it lived in pandas.core.common before 1.5.0,
    moved to pandas.errors in 1.5.0, and was removed in 3.0.0 when copy-on-write became
    the default. Probing for it is safer than comparing version strings, which also
    compares '1.10.0' as lower than '1.5.0'.
    """
    for module_name in ('pandas.errors', 'pandas.core.common'):
        try:
            module = __import__(module_name, fromlist=['SettingWithCopyWarning'])
            warning_class = module.SettingWithCopyWarning
        except (AttributeError, ImportError):
            continue

        if isinstance(warning_class, type) and issubclass(warning_class, Warning):
            return warning_class

    return None


def ignore_setting_with_copy_warning() -> bool:
    """
    Silence pandas' SettingWithCopyWarning when the installed pandas still raises it.

    Returns whether a filter was installed.
    """
    warning_class = get_setting_with_copy_warning()

    if warning_class is None:
        return False

    warnings.simplefilter(action='ignore', category=warning_class)

    return True


DATETIME_UNITS_PER_SECOND = {
    's': 1,
    'ms': 10**3,
    'us': 10**6,
    'ns': 10**9,
}

INT16_MIN, INT16_MAX = -(2**15), 2**15 - 1
INT32_MIN, INT32_MAX = -(2**31), 2**31 - 1


def datetime_resolution(dtype) -> str:
    """
    Return the resolution of a datetime64 or timedelta64 dtype.

    pandas 3 keeps whatever unit the source provides instead of always casting to
    nanoseconds, so 's', 'ms' and 'us' columns are now common.
    """
    unit = getattr(dtype, 'unit', None)

    if unit is None:
        try:
            unit = np.datetime_data(dtype)[0]
        except (TypeError, ValueError):
            unit = 'ns'

    return unit if unit in DATETIME_UNITS_PER_SECOND else 'ns'


def datetime_to_epoch_seconds(series: pd.Series) -> pd.Series:
    """
    Convert a datetime64 column to float epoch seconds, whatever its resolution.

    Series.view was removed in pandas 3, and dividing the integer representation by
    1e9 is only correct for nanosecond columns. Missing values stay missing instead of
    becoming the integer NaT sentinel.
    """
    divisor = DATETIME_UNITS_PER_SECOND[datetime_resolution(series.dtype)]

    return series.astype('int64').where(series.notna()) / divisor


def timedelta_to_nanoseconds(series: pd.Series) -> pd.Series:
    """Integer nanoseconds for a timedelta64 column of any resolution."""
    return series.astype('timedelta64[ns]').astype('int64')


def integer_bit_width(min_value, max_value) -> int:
    """
    Smallest of 16, 32 or 64 bits that holds the range, defaulting to 64.

    numpy 2 raises OverflowError when a Python int is narrowed to a type that cannot
    hold it, so range checks replace the narrowing casts the SQL exporters used.
    """
    try:
        low, high = int(min_value), int(max_value)
    except (OverflowError, TypeError, ValueError):
        return 64

    if INT16_MIN <= low and high <= INT16_MAX:
        return 16

    if INT32_MIN <= low and high <= INT32_MAX:
        return 32

    return 64
