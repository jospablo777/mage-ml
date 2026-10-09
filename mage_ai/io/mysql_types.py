"""
Arrow columns from the MySQL types of a query result, for MySQL.load with exact_types
or polars.

pandas read_sql builds columns from the Python values: integer columns with a NULL
become float64, which rounds values above 2**53, and DECIMAL becomes float.
"""
import decimal
from typing import Any, List, Optional

import pyarrow as pa
from mysql.connector.constants import FieldFlag, FieldType

BINARY_CHARSET = 63
MAX_DECIMAL128_PRECISION = 38
MAX_DECIMAL256_PRECISION = 76

INTEGER_TYPES = {
    FieldType.TINY,
    FieldType.SHORT,
    FieldType.INT24,
    FieldType.LONG,
    FieldType.LONGLONG,
}
TEXT_TYPES = {
    FieldType.VARCHAR,
    FieldType.VAR_STRING,
    FieldType.STRING,
    FieldType.TINY_BLOB,
    FieldType.MEDIUM_BLOB,
    FieldType.LONG_BLOB,
    FieldType.BLOB,
    FieldType.ENUM,
    FieldType.SET,
}


def _decimal_type(values: List[Any]) -> pa.DataType:
    """
    The narrowest decimal type that holds every value. The C extension of
    mysql-connector reports no precision or scale for DECIMAL columns.
    """
    scale = 0
    integer_digits = 1
    for value in values:
        if value is None or not value.is_finite():
            continue
        sign, digits, exponent = value.as_tuple()
        scale = max(scale, -exponent)
        integer_digits = max(integer_digits, len(digits) + exponent)
    precision = integer_digits + scale
    if precision <= MAX_DECIMAL128_PRECISION:
        return pa.decimal128(precision, scale)
    return pa.decimal256(min(precision, MAX_DECIMAL256_PRECISION), scale)


def arrow_type(description: tuple, values: List[Any]) -> pa.DataType:
    type_code, flags = description[1], description[7]
    charset = description[8] if len(description) > 8 else None
    unsigned = bool(flags & FieldFlag.UNSIGNED)

    if type_code in INTEGER_TYPES:
        if unsigned and type_code == FieldType.LONGLONG:
            return pa.uint64()
        return pa.int64()
    if type_code == FieldType.YEAR:
        return pa.int16()
    if type_code == FieldType.BIT:
        return pa.uint64()
    if type_code in (FieldType.FLOAT, FieldType.DOUBLE):
        return pa.float64()
    if type_code in (FieldType.DECIMAL, FieldType.NEWDECIMAL):
        return _decimal_type(values)
    if type_code in (FieldType.DATE, FieldType.NEWDATE):
        return pa.date32()
    if type_code == FieldType.DATETIME:
        return pa.timestamp('us')
    if type_code == FieldType.TIMESTAMP:
        # The query runs with the session time zone at UTC.
        return pa.timestamp('us', tz='UTC')
    if type_code == FieldType.TIME:
        return pa.duration('us')
    if type_code == FieldType.JSON:
        return pa.string()
    if type_code == FieldType.GEOMETRY:
        return pa.binary()
    if type_code == FieldType.NULL:
        return pa.null()
    if type_code in TEXT_TYPES:
        if charset == BINARY_CHARSET:
            return pa.binary()
        if any(isinstance(value, (set, frozenset)) for value in values):
            return pa.list_(pa.string())
        return pa.string()
    return pa.string()


def _prepared(values: List[Any], data_type: pa.DataType) -> List[Any]:
    if pa.types.is_list(data_type):
        # SET values arrive as Python sets, whose order is arbitrary.
        return [None if value is None else sorted(value) for value in values]
    if pa.types.is_string(data_type):
        return [
            value.decode() if isinstance(value, (bytes, bytearray)) else value
            for value in values
        ]
    if pa.types.is_decimal(data_type):
        return [
            None if value is None or not value.is_finite() else value for value in values
        ]
    return values


def arrow_table_from_cursor(cursor) -> pa.Table:
    rows = cursor.fetchall()
    descriptions = cursor.description or []
    columns = list(zip(*rows)) if rows else [() for _ in descriptions]
    names = [description[0] for description in descriptions]
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        raise ValueError(
            f'The query returns these columns more than once: {repeated}. Give each '
            'column its own name with AS.',
        )
    fields = []
    arrays = []
    for description, values in zip(descriptions, columns):
        values = list(values)
        data_type = arrow_type(description, values)
        arrays.append(pa.array(_prepared(values, data_type), type=data_type))
        fields.append(pa.field(description[0], data_type))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def decimal_column_type(values: List[Optional[decimal.Decimal]]) -> Optional[str]:
    """The MySQL DECIMAL type that holds every value, or None when none does."""
    data_type = _decimal_type(values)
    if data_type.precision > 65 or data_type.scale > 30:
        return None
    return f'DECIMAL({max(data_type.precision, 1)}, {data_type.scale})'
