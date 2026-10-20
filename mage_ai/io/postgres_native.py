"""
Rows sent to PostgreSQL as binary COPY, encoded in Rust (column_atlas_native, built from the
Rust blocks' client), in the export's own transaction.

COPY text rendered every value in Python: 7 seconds for 1 million rows of seven columns,
against under a second encoded in Rust. The binary format must match each column's type
exactly, so rows go this way only when every column of the frame encodes to the type the
table has (checked against the table, after the export created or emptied it); otherwise,
and when the frame does not convert to Arrow, the export renders COPY text as before.
MAGE_POSTGRES_BINARY_COPY=off turns it off.
"""
import io
import os
import re
from typing import Dict, List, Optional, Union

import pandas as pd
import polars as pl

# Rows per COPY statement: bounds the encoded data held at once.
CHUNK_ROWS = 250_000

_SYNONYMS = {
    'int2': 'smallint',
    'int4': 'integer',
    'int': 'integer',
    'int8': 'bigint',
    'float4': 'real',
    'float8': 'double precision',
    'bool': 'boolean',
    'timestamp without time zone': 'timestamp',
    'timestamp with time zone': 'timestamptz',
    'time without time zone': 'time',
}


def enabled() -> bool:
    if (os.getenv('MAGE_POSTGRES_BINARY_COPY') or '').strip().lower() in ('off', '0', 'false'):
        return False
    try:
        import column_atlas_native
    except ImportError:
        return False
    return hasattr(column_atlas_native, 'pg_copy_binary')


def normalized_type(name: str) -> str:
    """A column type as format_type or CREATE TABLE writes it, in one spelling."""
    text = re.sub(r'\s+', ' ', name.strip().lower())
    text = re.sub(r'\s*,\s*', ', ', text)
    text = re.sub(r'\s*\(\s*', '(', text)
    text = re.sub(r'\s*\)', ')', text)
    array = text.endswith('[]')
    base = text[:-2] if array else text
    base = _SYNONYMS.get(base, base)
    return base + ('[]' if array else '')


def compatible(encoded: str, table: str) -> bool:
    """
    Whether values encoded as one type are read by a column of the other. The server reads
    binary numeric and text with the column's own precision, scale or length, as it reads
    COPY text: a numeric value goes into any numeric column, and text into varchar(n).
    """
    encoded, table = normalized_type(encoded), normalized_type(table)
    if encoded == table:
        return True
    if encoded.endswith('[]') != table.endswith('[]'):
        return False
    encoded, table = encoded.rstrip('[]'), table.rstrip('[]')
    if encoded.startswith('numeric') and table.startswith('numeric'):
        return True
    return encoded == 'text' and table.startswith('character varying')


def _to_polars(frame: Union[pd.DataFrame, pl.DataFrame], columns: List[str]) -> pl.DataFrame:
    if isinstance(frame, pl.DataFrame):
        chunk = frame.select(columns)
    else:
        chunk = pl.from_pandas(frame[columns])
    # PostgreSQL keeps microseconds and rounds the digits after them, half to even, as
    # pandas does; the encoder would drop them.
    rounded = [
        pl.col(name).dt.round('1us') for name, dtype in chunk.schema.items()
        if isinstance(dtype, (pl.Datetime, pl.Duration)) and dtype.time_unit == 'ns'
    ]
    return chunk.with_columns(rounded) if rounded else chunk


def _encode(chunk: pl.DataFrame):
    import column_atlas_native

    buffer = io.BytesIO()
    chunk.write_ipc_stream(buffer)
    return column_atlas_native.pg_copy_binary(buffer.getvalue())


def copy(
    cursor,
    full_table_name: str,
    frame: Union[pd.DataFrame, pl.DataFrame],
    columns: List[str],
    table_types: Dict[str, str],
) -> Optional[str]:
    """
    Sends the frame's rows through binary COPY on the cursor's connection. Returns None when
    they were sent, or why they were not (nothing sent then), for COPY text to send them.
    `table_types` are the table's column types, from the catalog.
    """
    if not len(frame):
        return 'no rows'
    try:
        converted = _to_polars(frame, columns)
    except Exception as error:
        return f'the frame does not convert to Arrow: {error}'
    try:
        types, data = _encode(converted.slice(0, CHUNK_ROWS))
    except Exception as error:
        return str(error)
    for column, encoded in zip(columns, types):
        expected = table_types.get(column)
        if expected is None or not compatible(encoded, expected):
            return (f'column {column} encodes as {encoded}, and the table has '
                    f'{expected or "no such column"}')
    insert_columns = ', '.join(f'"{column}"' for column in columns)
    command = f'COPY {full_table_name} ({insert_columns}) FROM STDIN (FORMAT binary)'
    cursor.copy_expert(command, io.BytesIO(data))
    for start in range(CHUNK_ROWS, converted.height, CHUNK_ROWS):
        _, data = _encode(converted.slice(start, CHUNK_ROWS))
        cursor.copy_expert(command, io.BytesIO(data))
    return None


# ---- loads ---------------------------------------------------------------------------

# The client settings a native connection reproduces; with any other libpq setting (client
# certificates, target_session_attrs, ...) loads stay on psycopg2.
_CONNECTION_KEYS = {
    'dbname', 'user', 'password', 'host', 'port', 'schema', 'connection_method', 'ssh_host',
    'ssh_port', 'ssh_username', 'ssh_password', 'ssh_pkey', 'verbose', 'connect_timeout',
    'sslmode', 'sslrootcert', 'keepalives', 'keepalives_idle', 'options',
}


def loads_enabled() -> bool:
    if (os.getenv('MAGE_POSTGRES_BINARY_LOAD') or '').strip().lower() in ('off', '0', 'false'):
        return False
    try:
        import column_atlas_native
    except ImportError:
        return False
    return hasattr(column_atlas_native, 'PgConnection')


def connection_settings(settings: Dict, host: str, port) -> Optional[Dict[str, str]]:
    """The client's settings as an io_config profile for the Rust client, or None when the
    client uses a setting the Rust client does not have."""
    if any(value is not None and key not in _CONNECTION_KEYS for key, value in settings.items()):
        return None
    profile = dict(
        POSTGRES_HOST=host,
        POSTGRES_PORT=port,
        POSTGRES_DBNAME=settings.get('dbname'),
        POSTGRES_USER=settings.get('user'),
        POSTGRES_PASSWORD=settings.get('password'),
        POSTGRES_CONNECT_TIMEOUT=settings.get('connect_timeout'),
        POSTGRES_SSLMODE=settings.get('sslmode'),
        POSTGRES_SSLROOTCERT=settings.get('sslrootcert'),
        POSTGRES_OPTIONS=settings.get('options'),
    )
    return {key: str(value) for key, value in profile.items() if value not in (None, '')}


def _rebuilt(cursor, name: str, native: pl.Series, oid: int, precision, scale) -> pl.Series:
    """
    The column as the psycopg2 load builds it (postgres_types._polars_column). Columns read
    as text become what psycopg2's typecaster for their type makes of the same text.
    """
    import psycopg2.extensions

    from mage_ai.io import postgres_types

    values = native.to_list()
    # The typecaster psycopg2 would use: the cursor's, the connection's, then the global
    # one (Mage registers uuid[] on each cursor).
    caster = ((getattr(cursor, 'string_types', None) or {}).get(oid)
              or (getattr(cursor.connection, 'string_types', None) or {}).get(oid)
              or psycopg2.extensions.string_types.get(oid))
    if caster is not None and native.dtype == pl.String:
        # The cursor, not None: array typecasters cast their elements through it, and
        # crashed (a segmentation fault in psycopg2) without one.
        values = [None if value is None else caster(value, cursor) for value in values]
    return postgres_types._polars_column(name, oid, values, precision, scale)


def _column(cursor, name: str, native: pl.Series, oid: int, precision, scale) -> pl.Series:
    """The native column where it is what the psycopg2 load returns, rebuilt otherwise."""
    from mage_ai.io import postgres_types

    if oid == postgres_types.OID_NUMERIC:
        fits = precision is not None and scale is not None and (
            precision <= postgres_types.POLARS_MAX_DECIMAL_PRECISION
        )
        if fits and isinstance(native.dtype, pl.Decimal):
            expected = pl.Decimal(precision, scale)
            return native if native.dtype == expected else native.cast(expected)
        return _rebuilt(cursor, name, native, oid, precision, scale)
    nested = isinstance(native.dtype, pl.List) and isinstance(native.dtype.inner, pl.List)
    expected = postgres_types._polars_column(name, oid, [], precision, scale).dtype
    if not nested and native.dtype == expected:
        return native
    return _rebuilt(cursor, name, native, oid, precision, scale)


def load_polars(cursor, connection, query: str) -> Optional[pl.DataFrame]:
    """
    The query's rows as the psycopg2 load returns them with polars=True, read through binary
    COPY on `connection` (column_atlas_native.PgConnection). Column types, precision and
    scale come from a LIMIT 0 run of the query on `cursor`, the client's psycopg2 cursor.
    Returns None when the query cannot run this way, for the psycopg2 load to run it.
    """
    sql = query.strip().rstrip(';')
    cursor.execute(f'SELECT * FROM ({sql}) AS mage_probe LIMIT 0')
    description = cursor.description or []
    native = pl.read_ipc_stream(io.BytesIO(connection.query(sql)))
    if native.columns != [d.name for d in description]:
        return None
    return pl.DataFrame([
        _column(cursor, d.name, native[d.name], d.type_code, d.precision, d.scale)
        for d in description
    ])
