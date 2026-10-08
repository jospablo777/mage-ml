"""
Conversions between PostgreSQL column types and pandas or Polars frames.

Reading builds each column from the type PostgreSQL reports for it, so integers with
NULLs, numerics, bytes, dates and times keep their values. Writing chooses a PostgreSQL
type per column and renders every value in the text input format of that type. COPY and
INSERT both send that text, so both paths parse values the same way.
"""

import datetime
import decimal
import uuid
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import polars as pl
import simplejson

from mage_ai.shared.parsers import encode_complex

# Type OIDs from pg_type. Element OIDs of the built-in array types follow.
OID_BOOL = 16
OID_BYTEA = 17
OID_CHAR = 18
OID_NAME = 19
OID_INT8 = 20
OID_INT2 = 21
OID_INT4 = 23
OID_TEXT = 25
OID_OID = 26
OID_JSON = 114
OID_FLOAT4 = 700
OID_FLOAT8 = 701
OID_BPCHAR = 1042
OID_VARCHAR = 1043
OID_DATE = 1082
OID_TIME = 1083
OID_TIMESTAMP = 1114
OID_TIMESTAMPTZ = 1184
OID_INTERVAL = 1186
OID_TIMETZ = 1266
OID_NUMERIC = 1700
OID_UUID = 2950
OID_JSONB = 3802
OID_UUID_ARRAY = 2951

ARRAY_ELEMENT_OIDS = {
    1000: OID_BOOL,
    1001: OID_BYTEA,
    1002: OID_CHAR,
    1003: OID_NAME,
    1005: OID_INT2,
    1007: OID_INT4,
    1009: OID_TEXT,
    1014: OID_BPCHAR,
    1015: OID_VARCHAR,
    1016: OID_INT8,
    1021: OID_FLOAT4,
    1022: OID_FLOAT8,
    1028: OID_OID,
    1115: OID_TIMESTAMP,
    1182: OID_DATE,
    1183: OID_TIME,
    1185: OID_TIMESTAMPTZ,
    1187: OID_INTERVAL,
    1231: OID_NUMERIC,
    1270: OID_TIMETZ,
    199: OID_JSON,
    2951: OID_UUID,
    3807: OID_JSONB,
}

INTEGER_OIDS = {OID_INT2, OID_INT4, OID_INT8, OID_OID}
FLOAT_OIDS = {OID_FLOAT4, OID_FLOAT8}
TEXT_OIDS = {OID_TEXT, OID_VARCHAR, OID_BPCHAR, OID_NAME, OID_CHAR, OID_UUID}
JSON_OIDS = {OID_JSON, OID_JSONB}

POLARS_SCALAR_TYPES = {
    OID_BOOL: pl.Boolean,
    OID_BYTEA: pl.Binary,
    OID_CHAR: pl.String,
    OID_NAME: pl.String,
    OID_INT8: pl.Int64,
    OID_INT2: pl.Int16,
    OID_INT4: pl.Int32,
    OID_TEXT: pl.String,
    OID_OID: pl.UInt32,
    OID_JSON: pl.String,
    OID_FLOAT4: pl.Float32,
    OID_FLOAT8: pl.Float64,
    OID_BPCHAR: pl.String,
    OID_VARCHAR: pl.String,
    OID_DATE: pl.Date,
    OID_TIME: pl.Time,
    OID_TIMESTAMP: pl.Datetime('us'),
    OID_TIMESTAMPTZ: pl.Datetime('us', 'UTC'),
    OID_INTERVAL: pl.Duration('us'),
    OID_TIMETZ: pl.String,
    OID_UUID: pl.String,
    OID_JSONB: pl.String,
}

# PostgreSQL stores numeric values with up to 131072 digits before the point. Polars
# decimals hold 38 significant digits, so wider values stay text.
POLARS_MAX_DECIMAL_PRECISION = 38


def _is_missing(value: Any) -> bool:
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    if isinstance(value, (np.datetime64, np.timedelta64)):
        return bool(np.isnat(value))
    return False


# ---------------------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------------------


def _pandas_column(name: str, oid: int, values: List[Any]) -> pd.Series:
    if oid in INTEGER_OIDS:
        return pd.Series(pd.array(values, dtype='Int64'), name=name)
    if oid in FLOAT_OIDS:
        has_null = any(v is None for v in values)
        has_nan = any(v is not None and v != v for v in values)
        if not has_null:
            return pd.Series(values, name=name, dtype='float64')
        if not has_nan:
            return pd.Series(pd.array(values, dtype='Float64'), name=name)
        # pandas merges NaN into NA in nullable floats, so a column holding both keeps
        # Python objects.
        return pd.Series(values, name=name, dtype=object)
    if oid == OID_BOOL:
        return pd.Series(pd.array(values, dtype='boolean'), name=name)
    if oid in TEXT_OIDS:
        return pd.Series(values, name=name, dtype='str')
    if oid == OID_BYTEA:
        return pd.Series([None if v is None else bytes(v) for v in values], name=name, dtype=object)
    if oid == OID_TIMESTAMP:
        return pd.Series(pd.to_datetime(values), name=name).astype('datetime64[us]')
    if oid == OID_TIMESTAMPTZ:
        return pd.Series(pd.to_datetime(values, utc=True), name=name).astype(
            'datetime64[us, UTC]',
        )
    if oid == OID_INTERVAL:
        return pd.Series(pd.to_timedelta(values), name=name).astype('timedelta64[us]')
    if (
        oid not in (OID_NUMERIC, OID_DATE, OID_TIME, OID_TIMETZ)
        and oid not in JSON_OIDS
        and oid not in ARRAY_ELEMENT_OIDS
        and values
        and all(v is None or isinstance(v, str) for v in values)
    ):
        # Enums and other types psycopg2 returns as text.
        return pd.Series(values, name=name, dtype='str')
    # numeric, date, time, json and arrays keep the Python objects psycopg2 returns:
    # Decimal, date, time, dict or list. pandas has no exact dtype for them.
    return pd.Series(values, name=name, dtype=object)


def _decimal_scale(values: List[Any]) -> Optional[int]:
    scale = 0
    for value in values:
        if value is None:
            continue
        if not value.is_finite():
            return None
        exponent = value.as_tuple().exponent
        scale = max(scale, -exponent if exponent < 0 else 0)
    return scale


def _polars_decimal(name: str, values: List[Any], precision: Optional[int], scale: Optional[int]):
    if scale is None:
        scale = _decimal_scale(values)
    if scale is not None:
        digits = 0
        for value in values:
            if value is not None:
                integer_digits = max(value.adjusted() + 1, 1)
                digits = max(digits, integer_digits + scale)
        precision = precision or max(digits, 1)
        if precision <= POLARS_MAX_DECIMAL_PRECISION and digits <= POLARS_MAX_DECIMAL_PRECISION:
            return pl.Series(name, values, dtype=pl.Decimal(precision, scale))
    # NaN, infinity or more than 38 digits: keep the exact text.
    return pl.Series(name, [None if v is None else str(v) for v in values], dtype=pl.String)


def _polars_column(
    name: str,
    oid: int,
    values: List[Any],
    precision: Optional[int],
    scale: Optional[int],
) -> pl.Series:
    if oid == OID_NUMERIC:
        return _polars_decimal(name, values, precision, scale)
    if oid == OID_BYTEA:
        return pl.Series(name, [None if v is None else bytes(v) for v in values], dtype=pl.Binary)
    if oid == OID_TIMESTAMPTZ:
        utc = [None if v is None else v.astimezone(datetime.timezone.utc) for v in values]
        return pl.Series(name, utc, dtype=pl.Datetime('us', 'UTC'))
    if oid == OID_TIMETZ:
        return pl.Series(name, [None if v is None else v.isoformat() for v in values])
    if oid in POLARS_SCALAR_TYPES:
        return pl.Series(name, values, dtype=POLARS_SCALAR_TYPES[oid])
    element = ARRAY_ELEMENT_OIDS.get(oid)
    if element is not None:
        if element == OID_BYTEA:
            values = [
                None if v is None else [None if x is None else bytes(x) for x in v] for v in values
            ]
        inner = POLARS_SCALAR_TYPES.get(element)
        if element == OID_NUMERIC or inner is None:
            values = [
                None if v is None else [None if x is None else str(x) for x in v] for v in values
            ]
            inner = pl.String
        try:
            return pl.Series(name, values, dtype=pl.List(inner))
        except Exception:
            # Arrays with more than one dimension.
            return pl.Series(name, values, strict=False)
    # Enums and types without a fixed mapping arrive from psycopg2 as text.
    if all(v is None or isinstance(v, str) for v in values):
        return pl.Series(name, values, dtype=pl.String)
    return pl.Series(name, values, strict=False)


def frame_from_cursor(cursor, polars: bool = False):
    """
    Build a frame from an executed cursor, one column at a time from the reported types.
    """
    description = cursor.description or []
    rows = cursor.fetchall()
    columns = list(zip(*rows)) if rows else [()] * len(description)

    if polars:
        return pl.DataFrame(
            [
                _polars_column(d.name, d.type_code, list(values), d.precision, d.scale)
                for d, values in zip(description, columns)
            ]
        )

    series = [
        _pandas_column(d.name, d.type_code, list(values)) for d, values in zip(description, columns)
    ]
    if not series:
        return pd.DataFrame()
    return pd.concat(series, axis=1)


# ---------------------------------------------------------------------------------------
# Choosing column types for new tables
# ---------------------------------------------------------------------------------------


def _scalar_kind(value: Any) -> Optional[str]:
    if isinstance(value, (bool, np.bool_)):
        return 'boolean'
    if isinstance(value, (int, np.integer)):
        return 'bigint' if -(2**63) <= int(value) < 2**63 else 'numeric'
    if isinstance(value, (float, np.floating)):
        return 'double precision'
    if isinstance(value, decimal.Decimal):
        return 'numeric'
    if isinstance(value, str):
        return 'text'
    if isinstance(value, (bytes, bytearray, memoryview)):
        return 'bytea'
    if isinstance(value, datetime.datetime):
        return 'timestamptz' if value.tzinfo is not None else 'timestamp'
    if isinstance(value, datetime.date):
        return 'date'
    if isinstance(value, datetime.time):
        return 'timetz' if value.tzinfo is not None else 'time'
    if isinstance(value, (datetime.timedelta, np.timedelta64)):
        return 'interval'
    if isinstance(value, uuid.UUID):
        return 'uuid'
    return None


NUMERIC_WIDENING = ('bigint', 'numeric', 'double precision')


def _merge_kinds(kinds) -> Optional[str]:
    kinds = set(kinds)
    if len(kinds) == 1:
        return kinds.pop()
    if kinds and kinds <= set(NUMERIC_WIDENING):
        return 'double precision' if 'double precision' in kinds else 'numeric'
    if kinds == {'timestamp', 'timestamptz'}:
        return 'timestamptz'
    return None


def _array_type(values: List[Any]) -> str:
    kinds = []
    for value in values:
        items = value.tolist() if isinstance(value, np.ndarray) else value
        for item in items:
            if _is_missing(item):
                continue
            if isinstance(item, (list, tuple, dict, np.ndarray)):
                return 'jsonb'
            kind = _scalar_kind(item)
            if kind is None:
                return 'jsonb'
            kinds.append(kind)
    if not kinds:
        return 'jsonb'
    kind = _merge_kinds(kinds)
    return f'{kind}[]' if kind else 'jsonb'


def _object_type(values: List[Any]) -> str:
    present = [v for v in values if not _is_missing(v) and not (isinstance(v, float) and v != v)]
    if not present:
        return 'text'
    if all(isinstance(v, (list, tuple, np.ndarray)) for v in present):
        return _array_type(present)
    if any(isinstance(v, (dict, list, tuple, np.ndarray)) for v in present):
        return 'jsonb'
    kinds = [_scalar_kind(v) for v in present]
    if None in kinds:
        return 'text'
    return _merge_kinds(kinds) or 'text'


def pandas_column_type(series: pd.Series) -> str:
    dtype = series.dtype
    if isinstance(dtype, pd.CategoricalDtype):
        return 'text'
    if pd.api.types.is_bool_dtype(dtype):
        return 'boolean'
    if pd.api.types.is_integer_dtype(dtype):
        itemsize = np.dtype(dtype.numpy_dtype if hasattr(dtype, 'numpy_dtype') else dtype).itemsize
        unsigned = pd.api.types.is_unsigned_integer_dtype(dtype)
        if itemsize <= 2 and not (unsigned and itemsize == 2):
            return 'smallint'
        if itemsize <= 4 and not (unsigned and itemsize == 4):
            return 'integer'
        if unsigned and itemsize == 8:
            return 'numeric(20, 0)'
        return 'bigint'
    if pd.api.types.is_float_dtype(dtype):
        itemsize = np.dtype(dtype.numpy_dtype if hasattr(dtype, 'numpy_dtype') else dtype).itemsize
        return 'real' if itemsize == 4 else 'double precision'
    if isinstance(dtype, pd.DatetimeTZDtype):
        return 'timestamptz'
    if pd.api.types.is_datetime64_dtype(dtype):
        return 'timestamp'
    if pd.api.types.is_timedelta64_dtype(dtype):
        return 'interval'
    if pd.api.types.is_string_dtype(dtype) and not pd.api.types.is_object_dtype(dtype):
        return 'text'
    return _object_type(series.tolist())


def polars_column_type(dtype: pl.DataType) -> str:
    if dtype == pl.Boolean:
        return 'boolean'
    if dtype in (pl.Int8, pl.Int16, pl.UInt8):
        return 'smallint'
    if dtype in (pl.Int32, pl.UInt16):
        return 'integer'
    if dtype in (pl.Int64, pl.UInt32):
        return 'bigint'
    if dtype == pl.UInt64:
        return 'numeric(20, 0)'
    if dtype == pl.Float32:
        return 'real'
    if dtype == pl.Float64:
        return 'double precision'
    if isinstance(dtype, pl.Decimal):
        if dtype.precision is None:
            return 'numeric'
        return f'numeric({dtype.precision}, {dtype.scale})'
    if dtype in (pl.String, pl.Categorical) or isinstance(dtype, (pl.Enum, pl.Categorical)):
        return 'text'
    if dtype == pl.Binary:
        return 'bytea'
    if dtype == pl.Date:
        return 'date'
    if dtype == pl.Time:
        return 'time'
    if isinstance(dtype, pl.Datetime):
        return 'timestamptz' if dtype.time_zone else 'timestamp'
    if isinstance(dtype, pl.Duration):
        return 'interval'
    if isinstance(dtype, (pl.List, pl.Array)):
        inner = dtype.inner
        if isinstance(inner, (pl.List, pl.Array, pl.Struct)) or inner == pl.Object:
            return 'jsonb'
        inner_type = polars_column_type(inner)
        return 'jsonb' if inner_type == 'jsonb' else f'{inner_type}[]'
    if isinstance(dtype, pl.Struct) or dtype == pl.Object:
        return 'jsonb'
    return 'text'


# ---------------------------------------------------------------------------------------
# Rendering values in PostgreSQL text input format
# ---------------------------------------------------------------------------------------


def type_family(pg_type: str) -> str:
    """
    Group a PostgreSQL type name, as format_type or a table definition writes it.
    """
    name = pg_type.strip().lower()
    if name.endswith('[]') or name.startswith('_'):
        return 'array'
    name = name.split('(')[0].strip()
    if name in (
        'smallint',
        'integer',
        'bigint',
        'int',
        'int2',
        'int4',
        'int8',
        'oid',
        'smallserial',
        'serial',
        'bigserial',
    ):
        return 'integer'
    if name in ('numeric', 'decimal'):
        return 'numeric'
    if name in ('real', 'double precision', 'float4', 'float8', 'float'):
        return 'float'
    if name in ('boolean', 'bool'):
        return 'boolean'
    if name in ('json', 'jsonb'):
        return 'json'
    if name == 'bytea':
        return 'bytea'
    if name == 'date':
        return 'date'
    if name.startswith('timestamp'):
        return 'timestamp'
    if name.startswith('time'):
        return 'time'
    if name == 'interval':
        return 'interval'
    return 'text'


def array_element_type(pg_type: str) -> str:
    name = pg_type.strip()
    if name.endswith('[]'):
        return name[:-2]
    return name[1:]


def _json_text(value: Any) -> str:
    if isinstance(value, str):
        # A string in a JSON column is taken as JSON text when it parses, which is how
        # Polars frames carry JSON. Other strings are written as JSON strings.
        try:
            simplejson.loads(value)
            return value
        except ValueError:
            pass
    if isinstance(value, np.ndarray):
        value = value.tolist()
    return simplejson.dumps(
        value,
        default=encode_complex,
        ensure_ascii=False,
        ignore_nan=True,
        use_decimal=True,
    )


def _float_text(value: float) -> str:
    value = float(value)
    if value != value:
        return 'NaN'
    if value == float('inf'):
        return 'Infinity'
    if value == float('-inf'):
        return '-Infinity'
    return repr(value)


def _interval_text(value: Any) -> str:
    if isinstance(value, np.timedelta64):
        value = pd.Timedelta(value)
    if isinstance(value, pd.Timedelta):
        value = value.to_pytimedelta()
    if isinstance(value, datetime.timedelta):
        return f'{value.days} days {value.seconds} seconds {value.microseconds} microseconds'
    return str(value)


def _array_element_text(item: Any, element_type: str) -> str:
    if _is_missing(item):
        return 'NULL'
    if isinstance(item, (list, tuple, np.ndarray)):
        return _array_text(item, element_type)
    text = render_value(item, element_type, nan_is_null=False)
    if text is None:
        return 'NULL'
    return '"' + text.replace('\\', '\\\\').replace('"', '\\"') + '"'


def _array_text(value: Any, element_type: str) -> str:
    items = value.tolist() if isinstance(value, np.ndarray) else list(value)
    return '{' + ','.join(_array_element_text(item, element_type) for item in items) + '}'


def render_value(value: Any, pg_type: str, nan_is_null: bool = False) -> Optional[str]:
    """
    Render one value in the text input format of pg_type. None means SQL NULL.

    nan_is_null treats a float NaN as missing, which is how pandas stores NULL in a
    float64 column.
    """
    if _is_missing(value):
        return None
    if isinstance(value, (float, np.floating)) and value != value and nan_is_null:
        return None

    family = type_family(pg_type)

    if family == 'array':
        if isinstance(value, str):
            return value
        if isinstance(value, (list, tuple, np.ndarray)):
            return _array_text(value, array_element_type(pg_type))
        return str(value)
    if family == 'json':
        return _json_text(value)
    if family == 'bytea':
        if isinstance(value, memoryview):
            value = value.tobytes()
        if isinstance(value, (bytes, bytearray)):
            return '\\x' + bytes(value).hex()
        if isinstance(value, str):
            return '\\x' + value.encode('utf-8').hex()
        return str(value)
    if family == 'boolean':
        if isinstance(value, (bool, np.bool_)):
            return 'true' if value else 'false'
        return str(value)
    if family == 'integer':
        if isinstance(value, (bool, np.bool_)):
            return '1' if value else '0'
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        if isinstance(value, (float, np.floating)) and float(value).is_integer():
            return str(int(value))
        if isinstance(value, decimal.Decimal) and value == value.to_integral_value():
            return str(int(value))
        return str(value)
    if family in ('numeric', 'float'):
        if isinstance(value, (bool, np.bool_)):
            return '1' if value else '0'
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        if isinstance(value, (float, np.floating)):
            return _float_text(value)
        return str(value)
    if family == 'timestamp':
        if isinstance(value, np.datetime64):
            value = pd.Timestamp(value)
        if isinstance(value, datetime.datetime):
            return value.isoformat(sep=' ')
        if isinstance(value, datetime.date):
            return value.isoformat()
        return str(value)
    if family in ('date', 'time'):
        if isinstance(value, np.datetime64):
            value = pd.Timestamp(value)
        if isinstance(value, (datetime.date, datetime.time)):
            return value.isoformat()
        return str(value)
    if family == 'interval':
        return _interval_text(value)

    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list, tuple, np.ndarray)):
        return _json_text(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode('utf-8')
    if isinstance(value, (float, np.floating)):
        return _float_text(value)
    return str(value)


def _copy_escape(text: Optional[str]) -> str:
    if text is None:
        return '\\N'
    return text.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')


def column_values(frame, column: str) -> Tuple[List[Any], bool]:
    """
    Return the values of a column as Python objects, and whether NaN marks a NULL.
    """
    if isinstance(frame, pl.DataFrame):
        return frame.get_column(column).to_list(), False
    series = frame[column]
    # Outside object columns, NaN is how pandas marks a missing value: in float64 and in
    # the str dtype alike. Object columns can hold a float NaN as a value.
    nan_is_null = not pd.api.types.is_object_dtype(series.dtype)
    if isinstance(series.dtype, pd.DatetimeTZDtype) or pd.api.types.is_datetime64_dtype(
        series.dtype,
    ):
        values = [None if v is pd.NaT else v for v in series.tolist()]
        return values, nan_is_null
    return series.tolist(), nan_is_null


def render_columns(
    frame,
    columns: List[str],
    pg_types: Dict[str, str],
) -> List[List[Optional[str]]]:
    rendered = []
    for column in columns:
        values, nan_is_null = column_values(frame, column)
        pg_type = pg_types[column]
        rendered.append([render_value(v, pg_type, nan_is_null=nan_is_null) for v in values])
    return rendered


def copy_text(rendered_columns: List[List[Optional[str]]]) -> str:
    """
    Join rendered columns into COPY text format: tab separated, \\N for NULL.
    """
    escaped = [[_copy_escape(v) for v in column] for column in rendered_columns]
    if not escaped:
        return ''
    return ''.join('\t'.join(row) + '\n' for row in zip(*escaped))
