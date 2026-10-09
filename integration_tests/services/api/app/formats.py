"""
Encoding and decoding of data frames in the exchange formats the test API supports.

JSON has no date, decimal or binary type, so values follow these conventions, which the
tests rely on:

- dates as 'YYYY-MM-DD', datetimes as ISO 8601 with the offset for zoned values;
- decimals as strings, so no digit is lost to float parsing;
- bytes as base64 strings;
- integers as JSON numbers, exact in the text even beyond 2**53.

CSV and Excel carry only the flat columns. In CSV, NULL is an empty field and an empty
string is "".
"""
import base64
import datetime as dt
import decimal
import io
import json
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl

MEDIA_TYPES = {
    'json': 'application/json',
    'json-columns': 'application/json',
    'json-split': 'application/json',
    'ndjson': 'application/x-ndjson',
    'csv': 'text/csv',
    'parquet': 'application/vnd.apache.parquet',
    'arrow': 'application/vnd.apache.arrow.file',
    'arrow-stream': 'application/vnd.apache.arrow.stream',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
}
TEXT_FORMATS = ('csv', 'xlsx')


class DecodeError(ValueError):
    """The body could not be read in the declared format, or does not match the schema."""

    def __init__(self, message: str, errors: Optional[List[Dict]] = None):
        super().__init__(message)
        self.errors = errors or []


def to_json_value(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode('ascii')
    if isinstance(value, list):
        return [to_json_value(v) for v in value]
    if isinstance(value, float) and value != value:
        return None
    return value


def from_json_value(value: Any, dtype: pl.DataType) -> Any:
    """Read a JSON value as the Python value of a Polars dtype. Raises ValueError."""
    if value is None:
        return None
    if dtype == pl.Datetime:
        parsed = dt.datetime.fromisoformat(value)
        if getattr(dtype, 'time_zone', None):
            if parsed.tzinfo is None:
                raise ValueError(f'{value!r} has no time zone offset')
            return parsed.astimezone(dt.timezone.utc)
        return parsed
    if dtype == pl.Date:
        return dt.date.fromisoformat(value)
    if dtype == pl.Decimal:
        return decimal.Decimal(str(value))
    if dtype == pl.Binary:
        return base64.b64decode(value, validate=True)
    if dtype == pl.List:
        if not isinstance(value, list):
            raise ValueError(f'{value!r} is not a list')
        return [from_json_value(v, dtype.inner) for v in value]
    if dtype == pl.Boolean:
        if not isinstance(value, bool):
            raise ValueError(f'{value!r} is not a boolean')
        return value
    if dtype.is_integer():
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f'{value!r} is not an integer')
        return value
    if dtype.is_float():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f'{value!r} is not a number')
        return float(value)
    if dtype == pl.String:
        if not isinstance(value, str):
            raise ValueError(f'{value!r} is not a string')
        return value
    return value


def rows_to_frame(rows: List[Dict], schema: Optional[Dict[str, pl.DataType]]) -> pl.DataFrame:
    if schema is None:
        return pl.DataFrame(rows, infer_schema_length=None)

    errors = []
    converted = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            errors.append(dict(row=index, column=None, message='row is not an object'))
            continue
        missing = [c for c in schema if c not in row]
        extra = [c for c in row if c not in schema]
        for column in missing:
            errors.append(dict(row=index, column=column, message='missing'))
        for column in extra:
            errors.append(dict(row=index, column=column, message='not in the schema'))
        values = {}
        for column, dtype in schema.items():
            try:
                values[column] = from_json_value(row.get(column), dtype)
            except (ValueError, TypeError, decimal.InvalidOperation, base64.binascii.Error) as err:
                errors.append(dict(row=index, column=column, message=str(err)))
        converted.append(values)
    if errors:
        raise DecodeError(f'{len(errors)} values do not match the schema', errors[:50])
    try:
        return pl.DataFrame(converted, schema=schema, strict=True)
    except Exception as err:
        raise DecodeError(f'Values do not fit the schema: {err}') from err


def _cast(frame: pl.DataFrame, schema: Optional[Dict[str, pl.DataType]]) -> pl.DataFrame:
    if schema is None:
        return frame
    missing = [c for c in schema if c not in frame.columns]
    extra = [c for c in frame.columns if c not in schema]
    errors = [dict(row=None, column=c, message='missing') for c in missing]
    errors += [dict(row=None, column=c, message='not in the schema') for c in extra]
    if errors:
        raise DecodeError('Columns do not match the schema', errors)
    try:
        return frame.select(
            pl.col(c).cast(schema[c], strict=True) for c in schema
        )
    except Exception as err:
        raise DecodeError(f'Values do not fit the schema: {err}') from err


def _csv_to_frame(body: bytes, schema: Optional[Dict[str, pl.DataType]]) -> pl.DataFrame:
    if schema is None:
        return pl.read_csv(io.BytesIO(body), infer_schema_length=None)
    text = pl.read_csv(io.BytesIO(body), infer_schema=False)
    rows = []
    for row in text.to_dicts():
        parsed = {}
        for column, value in row.items():
            dtype = schema.get(column)
            if value is None or dtype is None or dtype == pl.String:
                parsed[column] = value
            elif dtype == pl.Boolean:
                # Polars writes true and false, pandas writes True and False.
                parsed[column] = {'true': True, 'false': False}.get(value.lower(), value)
            elif dtype.is_integer():
                parsed[column] = int(value) if value.lstrip('-').isdigit() else value
            elif dtype.is_float():
                parsed[column] = float(value)
            elif dtype == pl.Datetime:
                # Polars writes the offset as +0000, which fromisoformat reads.
                parsed[column] = value
            else:
                parsed[column] = value
        rows.append(parsed)
    return rows_to_frame(rows, schema)


def decode(
    body: bytes,
    content_type: str,
    schema: Optional[Dict[str, pl.DataType]] = None,
) -> pl.DataFrame:
    """Read a request body. Raises DecodeError with the reason."""
    media = (content_type or '').split(';')[0].strip().lower()
    try:
        if media == 'application/json':
            data = json.loads(body)
            if isinstance(data, list):
                return rows_to_frame(data, schema)
            if isinstance(data, dict) and 'columns' in data and 'data' in data:
                rows = [dict(zip(data['columns'], values)) for values in data['data']]
                return rows_to_frame(rows, schema)
            if isinstance(data, dict) and all(isinstance(v, list) for v in data.values()):
                length = len(next(iter(data.values()), []))
                if any(len(v) != length for v in data.values()):
                    raise DecodeError('Columns have different lengths')
                rows = [{k: v[i] for k, v in data.items()} for i in range(length)]
                return rows_to_frame(rows, schema)
            raise DecodeError(
                'JSON must be a list of records, {"columns": [...], "data": [...]}, '
                'or an object of columns',
            )
        if media in ('application/x-ndjson', 'application/jsonl'):
            rows = [json.loads(line) for line in body.splitlines() if line.strip()]
            return rows_to_frame(rows, schema)
        if media == 'text/csv':
            return _csv_to_frame(body, schema)
        if media == 'application/vnd.apache.parquet':
            return _cast(pl.read_parquet(io.BytesIO(body)), schema)
        if media in ('application/vnd.apache.arrow.file', 'application/vnd.apache.arrow'):
            return _cast(pl.read_ipc(io.BytesIO(body)), schema)
        if media == 'application/vnd.apache.arrow.stream':
            return _cast(pl.read_ipc_stream(io.BytesIO(body)), schema)
    except DecodeError:
        raise
    except json.JSONDecodeError as err:
        raise DecodeError(f'Invalid JSON: {err}') from err
    except Exception as err:
        raise DecodeError(f'Cannot read the body as {media}: {err}') from err
    raise DecodeError(f'Unsupported content type {media!r}')


def _xlsx(frame: pl.DataFrame) -> bytes:
    from openpyxl import Workbook

    workbook = Workbook()
    sheet = workbook.active
    sheet.append(frame.columns)
    for row in frame.iter_rows():
        values = []
        for value in row:
            if isinstance(value, dt.datetime) and value.tzinfo is not None:
                # Excel has no time zones; zoned values are written in UTC.
                value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)
            elif isinstance(value, decimal.Decimal):
                value = str(value)
            values.append(value)
        sheet.append(values)
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def _json_rows(frame: pl.DataFrame) -> List[Dict]:
    return [{k: to_json_value(v) for k, v in row.items()} for row in frame.to_dicts()]


ENCODERS: Dict[str, Callable[[pl.DataFrame], bytes]] = {
    'json': lambda f: json.dumps(_json_rows(f), ensure_ascii=False).encode(),
    'json-columns': lambda f: json.dumps(
        {c: [to_json_value(v) for v in f[c].to_list()] for c in f.columns},
        ensure_ascii=False,
    ).encode(),
    'json-split': lambda f: json.dumps(
        dict(columns=f.columns, data=[list(r.values()) for r in _json_rows(f)]),
        ensure_ascii=False,
    ).encode(),
    'ndjson': lambda f: '\n'.join(
        json.dumps(r, ensure_ascii=False) for r in _json_rows(f)
    ).encode() + b'\n',
    'csv': lambda f: f.write_csv().encode(),
    'parquet': lambda f: _to_bytes(f.write_parquet),
    'arrow': lambda f: _to_bytes(f.write_ipc),
    'arrow-stream': lambda f: _to_bytes(f.write_ipc_stream),
    'xlsx': _xlsx,
}


def _to_bytes(writer: Callable) -> bytes:
    buffer = io.BytesIO()
    writer(buffer)
    return buffer.getvalue()


def encode(
    frame: pl.DataFrame,
    fmt: str,
    flat_columns: List[str],
    compat: str = 'newest',
) -> Tuple[bytes, str]:
    """
    Write a frame in a format. Formats without nested values get the flat columns.

    Polars writes Arrow strings as string_view by default, which pyarrow cannot convert
    to pandas inside lists; compat='oldest' writes large_string.
    """
    if fmt not in ENCODERS:
        raise KeyError(fmt)
    if fmt in TEXT_FORMATS:
        frame = frame.select([c for c in flat_columns if c in frame.columns])
    if fmt in ('arrow', 'arrow-stream') and compat == 'oldest':
        writer = frame.write_ipc if fmt == 'arrow' else frame.write_ipc_stream
        return (
            _to_bytes(lambda buffer: writer(buffer, compat_level=pl.CompatLevel.oldest())),
            MEDIA_TYPES[fmt],
        )
    return ENCODERS[fmt](frame), MEDIA_TYPES[fmt]


def column_checksums(frame: pl.DataFrame) -> Dict[str, str]:
    """A digest of each column's values, so a client can compare what it sent."""
    import hashlib

    checksums = {}
    for column in frame.columns:
        values = [to_json_value(v) for v in frame[column].to_list()]
        text = json.dumps(values, ensure_ascii=False, sort_keys=True)
        checksums[column] = hashlib.sha256(text.encode()).hexdigest()
    return checksums
