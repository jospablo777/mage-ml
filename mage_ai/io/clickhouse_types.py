"""
ClickHouse column types for the tables ClickHouse.export creates, and the values it
inserts into them.

The exporter mapped only numpy int64 to an integer: nullable Int64, int32 and uint64
columns became String, and uint64 values failed to insert. Dates, decimals and UUIDs
became String and failed too, and datetimes became DateTime64(3), which dropped their
microseconds.
"""
import datetime
import decimal
import json
import math
import uuid
from typing import Any, List, Optional

import numpy as np
import pandas as pd

# ClickHouse Decimal holds up to 76 digits.
MAX_DECIMAL_PRECISION = 76
DATETIME_PRECISION = {'s': 0, 'ms': 3, 'us': 6, 'ns': 9}
INTEGER_TYPES = {
    'int8': 'Int8', 'int16': 'Int16', 'int32': 'Int32', 'int64': 'Int64',
    'uint8': 'UInt8', 'uint16': 'UInt16', 'uint32': 'UInt32', 'uint64': 'UInt64',
}


def quote(name: str) -> str:
    """A ClickHouse identifier. Names with spaces or keywords failed to parse."""
    return '`' + str(name).replace('\\', '\\\\').replace('`', '\\`') + '`'


def _is_missing(value: Any) -> bool:
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    return isinstance(value, float) and math.isnan(value)


def _integer_type(values: List[int]) -> str:
    smallest, largest = min(values), max(values)
    for bits in (64, 128, 256):
        if smallest >= 0 and largest < 2**bits and largest >= 2**(bits - 1):
            return f'UInt{bits}'
        if -(2**(bits - 1)) <= smallest and largest < 2**(bits - 1):
            return f'Int{bits}'
    return 'String'


def _decimal_type(values: List[decimal.Decimal]) -> str:
    finite = [v for v in values if v.is_finite()]
    if len(finite) != len(values) or not finite:
        # ClickHouse decimals have no NaN or infinity.
        return 'String'
    scale = max(0, max(-v.as_tuple().exponent for v in finite))
    integer_digits = max(
        max(len(v.as_tuple().digits) + v.as_tuple().exponent, 1) for v in finite
    )
    precision = max(integer_digits + scale, 1)
    if precision > MAX_DECIMAL_PRECISION:
        return 'String'
    return f'Decimal({precision}, {scale})'


def _object_type(values: List[Any]) -> str:
    present = [v for v in values if not _is_missing(v)]
    if not present:
        return 'String'
    if all(isinstance(v, (bool, np.bool_)) for v in present):
        return 'Bool'
    if all(isinstance(v, (int, np.integer)) and not isinstance(v, bool) for v in present):
        return _integer_type([int(v) for v in present])
    if all(isinstance(v, (float, np.floating)) for v in present):
        return 'Float64'
    if all(isinstance(v, decimal.Decimal) for v in present):
        return _decimal_type(present)
    if all(isinstance(v, datetime.datetime) for v in present):
        if all(v.tzinfo is not None for v in present):
            return "DateTime64(6, 'UTC')"
        if all(v.tzinfo is None for v in present):
            return 'DateTime64(6)'
        return 'String'
    if all(isinstance(v, datetime.date) and not isinstance(v, datetime.datetime)
           for v in present):
        return 'Date32'
    if all(isinstance(v, uuid.UUID) for v in present):
        return 'UUID'
    # Strings, bytes, and lists and dicts, which are written as JSON text: a ClickHouse
    # array cannot be NULL.
    return 'String'


def column_type(series: pd.Series) -> str:
    """The ClickHouse type of a column of a pandas frame, without Nullable."""
    dtype = series.dtype
    if isinstance(dtype, pd.CategoricalDtype):
        return 'String'
    if pd.api.types.is_bool_dtype(dtype):
        return 'Bool'
    name = str(dtype).lower()
    if name in INTEGER_TYPES:
        return INTEGER_TYPES[name]
    if name == 'float32':
        return 'Float32'
    if pd.api.types.is_float_dtype(dtype):
        return 'Float64'
    if isinstance(dtype, pd.DatetimeTZDtype):
        return f"DateTime64({DATETIME_PRECISION.get(dtype.unit, 6)}, 'UTC')"
    if pd.api.types.is_datetime64_dtype(dtype):
        unit = np.datetime_data(dtype)[0]
        return f'DateTime64({DATETIME_PRECISION.get(unit, 6)})'
    if pd.api.types.is_timedelta64_dtype(dtype):
        # ClickHouse has no duration type; durations are stored as microseconds.
        return 'Int64'
    if isinstance(dtype, pd.ArrowDtype):
        return _object_type(series.tolist())
    if pd.api.types.is_string_dtype(dtype) and not pd.api.types.is_object_dtype(dtype):
        return 'String'
    return _object_type(series.tolist())


def nullable(clickhouse_type: str) -> str:
    return f'Nullable({clickhouse_type})'


def _json_value(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, np.generic):
        return value.item()
    return str(value)


def values_for_insert(series: pd.Series, clickhouse_type: Optional[str]) -> pd.Series:
    """The values of a column as clickhouse-connect inserts them into clickhouse_type."""
    if clickhouse_type is None:
        return series
    if isinstance(series.dtype, pd.ArrowDtype):
        # clickhouse-connect cannot insert pyarrow-backed columns, such as the dates of a
        # Polars frame.
        series = pd.Series(
            [None if _is_missing(v) else v for v in series.tolist()],
            index=series.index,
            dtype=object,
        )
    if pd.api.types.is_timedelta64_dtype(series.dtype) and 'Int64' in clickhouse_type:
        return pd.Series(
            [None if _is_missing(v) else int(v / pd.Timedelta(microseconds=1)) for v in series],
            index=series.index,
            dtype=object,
        )
    if 'String' in clickhouse_type and series.dtype == object:
        # A list keeps None; Series.map made it NaN, which would be inserted as text.
        return pd.Series(
            [
                json.dumps(v, default=_json_value, ensure_ascii=False)
                if isinstance(v, (dict, list, tuple, np.ndarray)) else v
                for v in series
            ],
            index=series.index,
            dtype=object,
        )
    return series
