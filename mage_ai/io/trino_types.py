"""
Trino column types for the tables Trino.export creates, the SQL literals it inserts, and
Arrow columns for Trino.load with exact_types or polars.

The exporter made every column BIGINT, DOUBLE, BOOLEAN, TIMESTAMP or VARCHAR. TIMESTAMP
is timestamp(3) in Trino, which dropped microseconds; dates, decimals, UUIDs and bytes
became VARCHAR, and the Iceberg and Delta Lake connectors then rejected their values;
uint64 values failed. Rows were inserted one statement at a time.
"""
import datetime
import decimal
import math
import re
import uuid
from typing import Any, Dict, Iterator, List, Optional, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import simplejson

from mage_ai.shared.parsers import encode_complex

DELTA_LAKE = 'delta_lake'
# Trino's DECIMAL holds up to 38 digits.
MAX_DECIMAL_PRECISION = 38
DEFAULT_TIMESTAMP_PRECISION = 6
# The Delta Lake connector stores zoned timestamps in milliseconds.
DELTA_ZONED_PRECISION = 3
INTEGER_TYPES = {
    'int8': 'TINYINT', 'int16': 'SMALLINT', 'int32': 'INTEGER', 'int64': 'BIGINT',
    'uint8': 'SMALLINT', 'uint16': 'INTEGER', 'uint32': 'BIGINT',
    'uint64': 'DECIMAL(20, 0)',
}
TRINO_INTEGER_TYPES = {'tinyint', 'smallint', 'integer', 'bigint'}
ARROW_INTEGER_TYPES = {
    'tinyint': pa.int8(), 'smallint': pa.int16(), 'integer': pa.int32(), 'bigint': pa.int64(),
}


def quote(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def string_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _is_missing(value: Any) -> bool:
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    if isinstance(value, (float, np.floating)):
        return math.isnan(value)
    return isinstance(value, np.datetime64) and np.isnat(value)


def _timestamp_type(precision: int, zoned: bool, connector: Optional[str]) -> str:
    if not zoned:
        return f'TIMESTAMP({precision})'
    if connector == DELTA_LAKE:
        precision = min(precision, DELTA_ZONED_PRECISION)
    return f'TIMESTAMP({precision}) WITH TIME ZONE'


def _integer_type(values: List[int]) -> str:
    if all(-(2**63) <= v < 2**63 for v in values):
        return 'BIGINT'
    digits = max(len(str(abs(v))) for v in values)
    if digits <= MAX_DECIMAL_PRECISION:
        return f'DECIMAL({digits}, 0)'
    return 'VARCHAR'


def _decimal_type(values: List[decimal.Decimal]) -> str:
    if not all(v.is_finite() for v in values):
        # Trino decimals have no NaN or infinity.
        return 'VARCHAR'
    scale = max(0, max(-v.as_tuple().exponent for v in values))
    integer_digits = max(max(len(v.as_tuple().digits) + v.as_tuple().exponent, 1) for v in values)
    precision = integer_digits + scale
    if precision > MAX_DECIMAL_PRECISION:
        return 'VARCHAR'
    return f'DECIMAL({precision}, {scale})'


def _object_type(values: List[Any], connector: Optional[str], precision: int) -> str:
    present = [v for v in values if not _is_missing(v)]
    if not present:
        return 'VARCHAR'

    def every(types, exclude=()):
        return all(isinstance(v, types) and not isinstance(v, exclude) for v in present)

    if every((bool, np.bool_)):
        return 'BOOLEAN'
    if every((int, np.integer), exclude=(bool, np.bool_)):
        return _integer_type([int(v) for v in present])
    if every((float, np.floating)):
        return 'DOUBLE'
    if every(decimal.Decimal):
        return _decimal_type(present)
    if every(datetime.datetime):
        if all(v.tzinfo is not None for v in present):
            return _timestamp_type(precision, True, connector)
        if all(v.tzinfo is None for v in present):
            return _timestamp_type(precision, False, connector)
        return 'VARCHAR'
    if every(datetime.date, exclude=datetime.datetime):
        return 'DATE'
    if every(datetime.time):
        # The Delta Lake connector has no TIME type.
        return 'VARCHAR' if connector == DELTA_LAKE else 'TIME(6)'
    if every((datetime.timedelta, np.timedelta64)):
        return 'BIGINT'
    if every(uuid.UUID):
        # The Delta Lake connector has no UUID type.
        return 'VARCHAR' if connector == DELTA_LAKE else 'UUID'
    if every((bytes, bytearray, memoryview)):
        return 'VARBINARY'
    # Strings, and lists and dicts, which are written as JSON text: the Iceberg and Delta
    # Lake connectors have no JSON type.
    return 'VARCHAR'


def _arrow_type(data_type: pa.DataType, connector: Optional[str], precision: int) -> Optional[str]:
    if pa.types.is_boolean(data_type):
        return 'BOOLEAN'
    if pa.types.is_integer(data_type):
        return INTEGER_TYPES.get(str(data_type))
    if pa.types.is_floating(data_type):
        return 'DOUBLE' if pa.types.is_float64(data_type) else 'REAL'
    if pa.types.is_decimal(data_type):
        if data_type.precision <= MAX_DECIMAL_PRECISION:
            return f'DECIMAL({data_type.precision}, {data_type.scale})'
        return 'VARCHAR'
    if pa.types.is_date(data_type):
        return 'DATE'
    if pa.types.is_timestamp(data_type):
        return _timestamp_type(precision, data_type.tz is not None, connector)
    if pa.types.is_duration(data_type):
        return 'BIGINT'
    if pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type) \
            or pa.types.is_fixed_size_binary(data_type) or pa.types.is_binary_view(data_type):
        return 'VARBINARY'
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type) \
            or pa.types.is_string_view(data_type):
        return 'VARCHAR'
    return None


def column_type(
    series: pd.Series,
    connector: str = None,
    timestamp_precision: int = None,
) -> str:
    """
    The Trino type of a column of a pandas frame, for a catalog of connector, such as
    iceberg or delta_lake. Durations are stored in microseconds, and lists and dicts as
    JSON text.
    """
    precision = DEFAULT_TIMESTAMP_PRECISION if timestamp_precision is None \
        else int(timestamp_precision)
    dtype = series.dtype
    if isinstance(dtype, pd.CategoricalDtype):
        return 'VARCHAR'
    if isinstance(dtype, pd.ArrowDtype):
        arrow_type = _arrow_type(dtype.pyarrow_dtype, connector, precision)
        return arrow_type or _object_type(series.tolist(), connector, precision)
    if pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    name = str(dtype).lower()
    if name in INTEGER_TYPES:
        return INTEGER_TYPES[name]
    if name == 'float32':
        return 'REAL'
    if pd.api.types.is_float_dtype(dtype):
        return 'DOUBLE'
    if isinstance(dtype, pd.DatetimeTZDtype):
        return _timestamp_type(precision, True, connector)
    if pd.api.types.is_datetime64_dtype(dtype):
        return _timestamp_type(precision, False, connector)
    if pd.api.types.is_timedelta64_dtype(dtype):
        return 'BIGINT'
    if pd.api.types.is_string_dtype(dtype) and not pd.api.types.is_object_dtype(dtype):
        return 'VARCHAR'
    return _object_type(series.tolist(), connector, precision)


def _json_text(value: Any) -> str:
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, (set, frozenset)):
        value = sorted(value, key=str)
    return simplejson.dumps(value, default=encode_complex, ignore_nan=True, ensure_ascii=False)


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list, tuple, set, frozenset, np.ndarray)):
        return _json_text(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, datetime.datetime):
        return _timestamp_text(value, zoned=value.tzinfo is not None)
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return str(value.item())
    return str(value)


def _microseconds(value: Any) -> int:
    return int(pd.Timedelta(value) / pd.Timedelta(microseconds=1))


def _float_text(value: Any) -> str:
    value = float(value)
    if math.isinf(value):
        return 'Infinity' if value > 0 else '-Infinity'
    return repr(value)


def _decimal_text(value: Any) -> str:
    if isinstance(value, (int, np.integer)) and not isinstance(value, (bool, np.bool_)):
        return str(int(value))
    if isinstance(value, decimal.Decimal):
        return format(value, 'f')
    if isinstance(value, (float, np.floating)):
        return format(decimal.Decimal(repr(float(value))), 'f')
    return str(value)


def _offset_text(offset: datetime.timedelta) -> str:
    seconds = int(offset.total_seconds())
    sign = '-' if seconds < 0 else '+'
    hours, minutes = divmod(abs(seconds) // 60, 60)
    return f'{sign}{hours:02d}:{minutes:02d}'


def _timestamp_text(value: Any, zoned: bool) -> str:
    if isinstance(value, np.datetime64):
        value = pd.Timestamp(value)
    elif not isinstance(value, datetime.datetime) and isinstance(value, datetime.date):
        value = datetime.datetime(value.year, value.month, value.day)
    offset = value.utcoffset()
    if offset is not None and (not zoned or offset.total_seconds() % 60):
        # A naive column holds the instant in UTC. Trino offsets have no seconds.
        value = value.astimezone(datetime.timezone.utc)
        offset = datetime.timedelta(0)
    text = (
        f'{value.year:04d}-{value.month:02d}-{value.day:02d} '
        f'{value.hour:02d}:{value.minute:02d}:{value.second:02d}.{value.microsecond:06d}'
    )
    nanosecond = getattr(value, 'nanosecond', 0)
    if nanosecond:
        # Trino rounds to the column's precision.
        text += f'{nanosecond:03d}'
    if zoned:
        text += ' ' + ('UTC' if offset is None else _offset_text(offset))
    return text


def _base_type(trino_type: str) -> str:
    return re.split(r'[\s(]', trino_type.strip().lower(), maxsplit=1)[0]


def literal(value: Any, trino_type: str) -> str:
    """The SQL literal of value for a column of trino_type."""
    if not isinstance(value, (list, tuple, dict, np.ndarray)) and _is_missing(value):
        return 'NULL'
    lowered = trino_type.strip().lower()
    base = _base_type(lowered)
    if isinstance(value, str) and base not in ('varchar', 'char', 'json'):
        return f'CAST({string_literal(value)} AS {trino_type})'
    if base == 'boolean':
        return 'TRUE' if value else 'FALSE'
    if base in TRINO_INTEGER_TYPES:
        if isinstance(value, (datetime.timedelta, np.timedelta64)):
            return str(_microseconds(value))
        return str(int(value))
    if base in ('real', 'double'):
        return f"{base.upper()} '{_float_text(value)}'"
    if base == 'decimal':
        return f"DECIMAL '{_decimal_text(value)}'"
    if base in ('varchar', 'char'):
        return string_literal(_text(value))
    if base == 'varbinary':
        if isinstance(value, (bytes, bytearray, memoryview)):
            return f"X'{bytes(value).hex()}'"
        return f"X'{_text(value).encode('utf-8').hex()}'"
    if base == 'date':
        if isinstance(value, np.datetime64):
            value = pd.Timestamp(value)
        if isinstance(value, datetime.datetime):
            value = value.date()
        return f"DATE '{value.year:04d}-{value.month:02d}-{value.day:02d}'"
    if base == 'time':
        return f"TIME '{_text(value)}'"
    if base == 'timestamp':
        return f"TIMESTAMP '{_timestamp_text(value, 'with time zone' in lowered)}'"
    if base == 'uuid':
        return f"UUID '{value}'"
    if base == 'json':
        text = value if isinstance(value, str) else _json_text(value)
        return f'JSON {string_literal(text)}'
    if base in ('array', 'map', 'row'):
        return f'CAST(JSON {string_literal(_json_text(value))} AS {trino_type})'
    return f'CAST({string_literal(_text(value))} AS {trino_type})'


def insert_statements(
    full_table_name: str,
    columns: Sequence[str],
    types: Sequence[str],
    rows: Iterator[Sequence[Any]],
    max_length: int,
) -> Iterator[str]:
    """
    INSERT statements for rows, each with as many rows as fit in max_length characters.
    Trino rejects statements longer than its query.max-length, 1,000,000 by default.
    """
    prefix = f'INSERT INTO {full_table_name} ({", ".join(quote(c) for c in columns)}) VALUES '
    values = []
    length = len(prefix)
    for row in rows:
        text = '(' + ', '.join(literal(v, t) for v, t in zip(row, types)) + ')'
        if values and length + len(text) + 2 > max_length:
            yield prefix + ', '.join(values)
            values = []
            length = len(prefix)
        values.append(text)
        length += len(text) + 2
    if values:
        yield prefix + ', '.join(values)


def _arrow_values(values: List[Any], trino_type: str) -> pa.Array:
    lowered = trino_type.strip().lower()
    base = _base_type(lowered)
    if base == 'boolean':
        return pa.array(values, pa.bool_())
    if base in ARROW_INTEGER_TYPES:
        return pa.array(values, ARROW_INTEGER_TYPES[base])
    if base == 'real':
        return pa.array(values, pa.float32())
    if base == 'double':
        return pa.array(values, pa.float64())
    if base == 'decimal':
        precision, scale = (int(n) for n in re.findall(r'\d+', lowered)[:2])
        return pa.array(values, pa.decimal128(precision, scale))
    if base in ('varchar', 'char', 'json'):
        return pa.array(values, pa.string())
    if base == 'varbinary':
        return pa.array(values, pa.binary())
    if base == 'date':
        return pa.array(values, pa.date32())
    if base == 'time' and 'with time zone' not in lowered:
        return pa.array(values, pa.time64('us'))
    if base == 'timestamp':
        if 'with time zone' in lowered:
            utc = [
                None if v is None else v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                for v in values
            ]
            return pa.array(utc, pa.timestamp('us', tz='UTC'))
        return pa.array(values, pa.timestamp('us'))
    if base == 'uuid':
        return pa.array([None if v is None else str(v) for v in values], pa.string())
    if lowered == 'interval day to second':
        return pa.array(values, pa.duration('us'))
    if base in ('array', 'map', 'row'):
        try:
            return pa.array([None if v is None else _plain(v) for v in values])
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            pass
    return pa.array([None if v is None else _text(v) for v in values], pa.string())


def _plain(value: Any) -> Any:
    """Rows of the trino client, which are tuples with names, as dicts."""
    if isinstance(value, tuple) and hasattr(value, '_names'):
        return {name: _plain(v) for name, v in zip(value._names, value)}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    return value


def arrow_table(description: Sequence, rows: List[Sequence[Any]]) -> pa.Table:
    """An Arrow table of a query result, with columns of the result's Trino types."""
    columns = {}
    names = []
    for position, column in enumerate(description):
        name = column[0]
        while name in columns:
            name = f'{name}_{position}'
        names.append(name)
        columns[name] = _arrow_values([row[position] for row in rows], column[1])
    return pa.table(columns)


def frame_with_nullable_integers(description: Sequence, rows: List[Sequence[Any]]):
    """
    A pandas frame as pandas read_sql builds it, except that integer columns stay
    integers, as nullable Int64. read_sql turned integer columns with a NULL into float64,
    which rounds values above 2**53.
    """
    names = [column[0] for column in description]
    frame = pd.DataFrame.from_records(rows, columns=names, coerce_float=True)
    for position, column in enumerate(description):
        if _base_type(column[1]) in TRINO_INTEGER_TYPES:
            frame.isetitem(position, pd.array([row[position] for row in rows], dtype='Int64'))
    return frame


def table_types(rows: List[Sequence[Any]]) -> Dict[str, str]:
    """Column names and types from rows of information_schema.columns."""
    return {name: data_type for name, data_type in rows}
