"""
PostgreSQL SQL blocks in pipelines with Python blocks:

- pg_sql_pandas: a pandas loader, a SQL transformer and a SQL exporter.
- pg_sql_polars: a Polars loader and a SQL exporter.
- pg_sql_to_python: a SQL loader, a Python transformer and a SQL exporter.
- pg_sql_raw: a Python loader and a raw SQL exporter that inserts into a table.
- pg_sql_raw_to_python: a raw SQL loader and a Python transformer.

Python blocks hand frames to SQL blocks through a table that Mage creates from the
frame; SQL blocks hand their result to Python blocks as a frame. Each test compares what
the SQL exporter stored with the source.
"""
import datetime as dt
import decimal
import math
import uuid

import pandas as pd

from integration_tests.data import r_dataset, sql_dataset
from integration_tests.mage_runner import run_pipeline

ROWS = 200


def columns(pg, schema, table):
    with pg.cursor() as cursor:
        cursor.execute(
            'SELECT column_name, data_type FROM information_schema.columns '
            'WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position',
            (schema, table),
        )
        result = dict(cursor.fetchall())
    pg.rollback()
    return result


def stored(pg, schema, table):
    with pg.cursor() as cursor:
        cursor.execute(f'SELECT * FROM {schema}.{table} ORDER BY id')
        names = [d.name for d in cursor.description]
        rows = [dict(zip(names, row)) for row in cursor.fetchall()]
    pg.rollback()
    return names, rows


def normalized(value):
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, float) and math.isnan(value):
        return 'NaN'
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, dt.datetime) and value.tzinfo is not None:
        return value.astimezone(dt.timezone.utc)
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def mismatches(actual_rows, expected_rows, names):
    """Per column, the first row whose stored value differs from the expected one."""
    found = {}
    for name in names:
        for index, (actual, expected) in enumerate(zip(actual_rows, expected_rows)):
            a, e = normalized(actual[name]), normalized(expected[name])
            if a != e:
                found[name] = f'row {index}: {a!r}, expected {e!r}'
                break
    return found


def test_pandas_to_sql_transformer_to_sql_exporter(mage_project, pg, profile_schema):
    run_pipeline('pg_sql_pandas', rows=ROWS)

    source = r_dataset.source_frame(ROWS)
    expected = [
        {name: value for name, value in zip(source.columns, row)}
        for row in zip(*[r_dataset.values(source[name]) for name in source.columns])
    ]
    for row in expected:
        row['sql_int_times_two'] = None if row['c_int'] is None else row['c_int'] * 2
        row['sql_text_length'] = None if row['c_text'] is None else len(row['c_text'])
        row['sql_big_plus_one'] = (
            None if row['c_big'] is None else decimal.Decimal(row['c_big'] + 1)
        )
        row['sql_list_length'] = None if row['c_int_list'] is None else len(row['c_int_list'])
        row['sql_ts_plus_hour'] = (
            None if row['c_ts'] is None else row['c_ts'] + dt.timedelta(hours=1)
        )
        row['sql_struct_b'] = None if row['c_struct'] is None else row['c_struct']['b']

    names, rows = stored(pg, profile_schema, 'pg_sql_pandas_dst')

    assert len(rows) == len(expected)
    assert mismatches(rows, expected, names) == {}
    assert columns(pg, profile_schema, 'pg_sql_pandas_dst') == {
        'id': 'bigint', 'c_int': 'bigint', 'c_big': 'bigint', 'c_double': 'double precision',
        'c_bool': 'boolean', 'c_text': 'text', 'c_category': 'text', 'c_date': 'date',
        'c_ts': 'timestamp without time zone', 'c_tstz': 'timestamp with time zone',
        'c_duration': 'interval', 'c_time': 'time without time zone', 'c_decimal': 'numeric',
        'c_int_list': 'ARRAY', 'c_text_list': 'ARRAY', 'c_struct': 'jsonb', 'c_binary': 'bytea',
        'c_uuid': 'uuid', 'sql_int_times_two': 'bigint', 'sql_text_length': 'integer',
        'sql_big_plus_one': 'numeric', 'sql_list_length': 'integer',
        'sql_ts_plus_hour': 'timestamp without time zone', 'sql_struct_b': 'text',
    }


def test_polars_to_sql_exporter(mage_project, pg, profile_schema):
    """Polars types, among them unsigned, 128-bit and decimal(38, 10) columns."""
    run_pipeline('pg_sql_polars', rows=ROWS)

    expected = sql_dataset.expected_rows(ROWS)
    names, rows = stored(pg, profile_schema, 'pg_sql_polars_dst')

    assert len(rows) == len(expected)
    # Column names that are SQL keywords, such as text and day, are kept.
    assert names == list(expected[0])
    assert mismatches(rows, expected, names) == {}
    assert columns(pg, profile_schema, 'pg_sql_polars_dst') == {
        'id': 'bigint', 'i8': 'smallint', 'i16': 'smallint', 'i32': 'integer', 'i64': 'bigint',
        'u8': 'smallint', 'u16': 'integer', 'u32': 'bigint', 'u64': 'numeric',
        'i128': 'numeric', 'f32': 'real', 'f64': 'double precision', 'flag': 'boolean',
        'text': 'text', 'day': 'date', 'at': 'timestamp without time zone',
        'at_ns': 'timestamp without time zone', 'zoned': 'timestamp with time zone',
        'span': 'interval', 'clock': 'time without time zone', 'amount': 'numeric',
        'ints': 'ARRAY', 'words': 'ARRAY', 'pair': 'ARRAY', 'record': 'jsonb', 'raw': 'bytea',
        'mood': 'text',
    }


# How each source column compares with what the SQL exporter wrote after the SQL loader
# and a Python transformer. The SQL loader reads numeric and real as float, as pandas
# read_sql does, enums and uuid as text, and integer arrays as lists of ints, which are
# written as bigint[].
ROUND_TRIP = {
    'c_int_array': 'd.c_int_array = s.c_int_array::bigint[]',
    'c_numeric': 'd.c_numeric = s.c_numeric::double precision',
    'c_numeric_free': 'd.c_numeric_free = s.c_numeric_free::double precision',
    # psycopg2 reads real from its text, as 3.4028235e+38.
    'c_real': 'd.c_real = s.c_real::text::double precision',
    'c_char': 'd.c_char::char(5) = s.c_char',
    'c_uuid': 'd.c_uuid::uuid = s.c_uuid',
    'c_json': 'd.c_json::jsonb = s.c_json::jsonb',
    'c_enum': 'd.c_enum = s.c_enum::text',
    'c_uuid_array': 'd.c_uuid_array::uuid[] = s.c_uuid_array',
    # Nested lists are written as jsonb.
    'c_int_matrix': 'd.c_int_matrix = to_jsonb(s.c_int_matrix)',
}


def round_trip_mismatches(pg, schema, names):
    found = {}
    with pg.cursor() as cursor:
        for name in names:
            comparison = ROUND_TRIP.get(name, f'd.{name} = s.{name}')
            cursor.execute(f"""
                SELECT s.id, s.{name}::text, d.{name}::text
                FROM {schema}.src s JOIN {schema}.pg_sql_to_python_dst d USING (id)
                WHERE (s.{name} IS NULL) <> (d.{name} IS NULL)
                   OR NOT coalesce({comparison}, s.{name} IS NULL)
                ORDER BY s.id LIMIT 3
            """)
            rows = cursor.fetchall()
            if rows:
                found[name] = rows
    pg.rollback()
    return found


def test_sql_loader_to_python_to_sql_exporter(
    mage_project, pg, profile_schema, source_table, source_rows,
):
    """
    The SQL loader's frame kept integers: pandas read_sql turned integer columns with
    NULLs into float64, so 9223372036854775807 became 9.223372036854776e18.
    """
    from integration_tests.data import postgres_dataset

    run_pipeline('pg_sql_to_python', schema=profile_schema, expected_rows=len(source_rows))

    with pg.cursor() as cursor:
        cursor.execute(f'SELECT count(*) FROM {profile_schema}.pg_sql_to_python_dst')
        assert cursor.fetchone()[0] == len(source_rows)
    pg.rollback()
    assert round_trip_mismatches(pg, profile_schema, postgres_dataset.COLUMN_NAMES) == {}
    assert columns(pg, profile_schema, 'pg_sql_to_python_dst')['c_bigint'] == 'bigint'


def test_raw_sql_exporter_inserts_into_a_table(mage_project, pg, profile_schema):
    with pg.cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE {profile_schema}.pg_sql_raw_dst (
                id bigint, c_big bigint, c_text text, c_date date, c_tstz timestamptz,
                c_int_list bigint[], c_struct jsonb
            )
        """)
    pg.commit()

    run_pipeline('pg_sql_raw', schema=profile_schema, rows=ROWS)
    run_pipeline('pg_sql_raw', schema=profile_schema, rows=ROWS)

    source = r_dataset.source_frame(ROWS)
    names = ['id', 'c_big', 'c_text', 'c_date', 'c_tstz', 'c_int_list', 'c_struct']
    expected = [
        dict(zip(names, row)) for row in zip(*[r_dataset.values(source[n]) for n in names])
    ]
    _, rows = stored(pg, profile_schema, 'pg_sql_raw_dst')
    # Each run appends its rows.
    assert len(rows) == 2 * len(expected)
    assert mismatches(rows[::2], expected, names) == {}
    assert mismatches(rows[1::2], expected, names) == {}


def test_raw_sql_results_keep_integers(mage_project, profile_schema, source_table, source_rows):
    """
    A raw SQL block's result was read with pandas read_sql, which turned integer columns
    with NULLs into float64. The Python block checks the types and the extreme values.
    """
    run_pipeline(
        'pg_sql_raw_to_python', schema=profile_schema, expected_rows=len(source_rows),
    )
