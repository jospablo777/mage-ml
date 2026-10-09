"""
Mage pipelines with R blocks, run through Mage's trigger, scheduler and executor:

- r_types: a Python loader, an R transformer, a Python transformer that checks what R
  returned, and a SQL exporter that writes it to PostgreSQL.
- r_dbi: an R loader that reads PostgreSQL with DBI, an R transformer and an R exporter
  that writes the result back.
- r_sql: a SQL loader, an R transformer, a Polars transformer and a SQL exporter.
"""
import datetime as dt
import math

import pandas as pd

from integration_tests.data import r_dataset
from integration_tests.mage_runner import run_pipeline

MULTIPLIER = 2.5
PREFIX = "it's "


def columns(pg, schema, table):
    with pg.cursor() as cursor:
        cursor.execute(
            'SELECT column_name, data_type FROM information_schema.columns '
            'WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position',
            (schema, table),
        )
        return dict(cursor.fetchall())


def stored_rows(pg, schema, table):
    with pg.cursor() as cursor:
        cursor.execute(f'SELECT * FROM {schema}.{table} ORDER BY id')
        names = [d.name for d in cursor.description]
        return names, cursor.fetchall()


def normalized(value):
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, dt.datetime) and value.tzinfo is not None:
        return value.astimezone(dt.timezone.utc)
    return value


def test_r_types(mage_project, pg, profile_schema):
    """
    The Python check in the pipeline compares what R returned with the source. This
    compares what the SQL exporter stored.
    """
    rows = 200
    run_pipeline('r_types', multiplier=MULTIPLIER, prefix=PREFIX, rows=rows)

    source = r_dataset.source_frame(rows)
    expected = {name: r_dataset.values(source[name]) for name in source.columns}
    expected['c_decimal'] = [None if v is None else float(v) for v in expected['c_decimal']]
    expected['c_uuid'] = [None if v is None else str(v) for v in expected['c_uuid']]
    expected.update(r_dataset.derived_values(source, MULTIPLIER, PREFIX))

    names, stored = stored_rows(pg, profile_schema, 'r_types')
    assert sorted(names) == sorted(expected)
    mismatches = {}
    for position, name in enumerate(names):
        actual = [normalized(row[position]) for row in stored]
        wanted = [normalized(value) for value in expected[name]]
        if actual != wanted:
            index = next(i for i, (a, w) in enumerate(zip(actual, wanted)) if a != w)
            mismatches[name] = f'row {index}: {actual[index]!r}, expected {wanted[index]!r}'
    assert mismatches == {}

    assert columns(pg, profile_schema, 'r_types') == {
        'id': 'bigint', 'c_int': 'bigint', 'c_big': 'bigint', 'c_double': 'double precision',
        'c_bool': 'boolean', 'c_text': 'text', 'c_category': 'text', 'c_date': 'date',
        'c_ts': 'timestamp without time zone', 'c_tstz': 'timestamp with time zone',
        'c_duration': 'interval', 'c_time': 'time without time zone',
        'c_decimal': 'double precision', 'c_int_list': 'ARRAY', 'c_text_list': 'ARRAY',
        'c_struct': 'jsonb', 'c_binary': 'bytea', 'c_uuid': 'text',
        'r_big_plus_one': 'bigint', 'r_text_length': 'bigint', 'r_list_length': 'bigint',
        'r_date_year': 'double precision', 'r_ts_plus_hour': 'timestamp without time zone',
        'r_duration_seconds': 'double precision', 'r_struct_a': 'double precision',
        'r_scaled': 'double precision', 'r_label': 'text',
    }


# How each source column compares with what the R exporter wrote. RPostgres reads
# numeric as double, real through its text, char(n) padded, timestamp as a UTC
# date-time, which is written as timestamptz, and interval, JSON, arrays and enums as
# text, which it writes as text.
DBI_COMPARISONS = {
    'id': 'd.id = s.id',
    'c_smallint': 'd.c_smallint = s.c_smallint',
    'c_integer': 'd.c_integer = s.c_integer',
    'c_bigint': 'd.c_bigint = s.c_bigint',
    'c_numeric': 'd.c_numeric = s.c_numeric::double precision',
    'c_numeric_free': 'd.c_numeric_free = s.c_numeric_free::double precision',
    'c_real': 'd.c_real = s.c_real::text::double precision',
    'c_double': 'd.c_double = s.c_double',
    'c_bool': 'd.c_bool = s.c_bool',
    'c_text': 'd.c_text = s.c_text',
    'c_varchar': 'd.c_varchar = s.c_varchar',
    'c_char': 'd.c_char::char(5) = s.c_char',
    'c_bytea': 'd.c_bytea = s.c_bytea',
    'c_date': 'd.c_date = s.c_date',
    'c_time': 'd.c_time = s.c_time',
    'c_timestamp': "d.c_timestamp = s.c_timestamp AT TIME ZONE 'UTC'",
    'c_timestamptz': 'd.c_timestamptz = s.c_timestamptz',
    'c_interval': 'd.c_interval::interval = s.c_interval',
    'c_uuid': 'd.c_uuid::uuid = s.c_uuid',
    'c_json': 'd.c_json::jsonb = s.c_json::jsonb',
    'c_jsonb': 'd.c_jsonb::jsonb = s.c_jsonb',
    'c_int_array': 'd.c_int_array::integer[] = s.c_int_array',
    'c_bigint_array': 'd.c_bigint_array::bigint[] = s.c_bigint_array',
    'c_float_array': 'd.c_float_array::double precision[] = s.c_float_array',
    'c_bool_array': 'd.c_bool_array::boolean[] = s.c_bool_array',
    'c_text_array': 'd.c_text_array::text[] = s.c_text_array',
    'c_numeric_array': 'd.c_numeric_array::numeric[] = s.c_numeric_array',
    'c_date_array': 'd.c_date_array::date[] = s.c_date_array',
    'c_uuid_array': 'd.c_uuid_array::uuid[] = s.c_uuid_array',
    'c_int_matrix': 'd.c_int_matrix::integer[][] = s.c_int_matrix',
    'c_enum': 'd.c_enum = s.c_enum::text',
    'text_length': 'd.text_length = char_length(s.c_text)',
    'bigint_plus_one': 'd.bigint_plus_one = s.c_bigint + 1',
    'date_year': 'd.date_year = extract(year FROM s.c_date)',
}


# Values R cannot hold, which become NULL, by row id: R's integers use -2**31 as NA,
# bit64 uses -2**63, and a NaN becomes missing when the frame crosses pandas between
# blocks. Doubles hold microseconds only up to the year 2242.
DBI_LIMITS = {
    'c_integer': 'c_integer = -2147483648',
    'c_bigint': 'c_bigint = -9223372036854775808',
    # The transformer adds 1, which overflows the largest bigint; bit64 gives NA.
    'bigint_plus_one': 'c_bigint IN (-9223372036854775808, 9223372036854775807)',
    'c_numeric_free': "c_numeric_free = 'NaN'",
    'c_numeric': "c_numeric = 'NaN'",
    'c_real': "c_real = 'NaN'",
    'c_double': "c_double = 'NaN'",
    'c_timestamp': "c_timestamp >= '2242-01-01'",
    'c_timestamptz': "c_timestamptz >= '2242-01-01'",
}


def dbi_mismatches(pg, schema):
    """Per column, the ids of the rows whose values differ, null-safely."""
    mismatches = {}
    with pg.cursor() as cursor:
        for name, comparison in DBI_COMPARISONS.items():
            source = {
                'text_length': 's.c_text', 'bigint_plus_one': 's.c_bigint',
                'date_year': 's.c_date',
            }.get(name, f's.{name}')
            limit = f'AND NOT coalesce(s.{DBI_LIMITS[name]}, false)' if name in DBI_LIMITS else ''
            cursor.execute(f"""
                SELECT s.id FROM {schema}.src s JOIN {schema}.r_dbi_dst d USING (id)
                WHERE (({source} IS NULL) <> (d.{name} IS NULL)
                   OR NOT coalesce({comparison}, {source} IS NULL))
                   {limit}
                ORDER BY s.id LIMIT 5
            """)
            ids = [row[0] for row in cursor.fetchall()]
            if ids:
                mismatches[name] = ids
    pg.rollback()
    return mismatches


def limited_values(pg, schema):
    """What the rows with values R cannot hold hold after the round trip."""
    with pg.cursor() as cursor:
        result = {}
        for name, condition in DBI_LIMITS.items():
            cursor.execute(f"""
                SELECT count(*), count(d.{name}) FROM {schema}.src s
                JOIN {schema}.r_dbi_dst d USING (id) WHERE s.{condition}
            """)
            result[name] = cursor.fetchone()
    pg.rollback()
    return result


def test_r_dbi(mage_project, pg, profile_schema, source_table, source_rows):
    run_pipeline('r_dbi', max_id=10**9, expected_rows=len(source_rows))

    with pg.cursor() as cursor:
        cursor.execute(f'SELECT count(*) FROM {profile_schema}.r_dbi_dst')
        assert cursor.fetchone()[0] == len(source_rows)
    assert dbi_mismatches(pg, profile_schema) == {}
    limited = limited_values(pg, profile_schema)
    # Integers R cannot hold and NaN become NULL; the edge rows have each once.
    for name in ('c_integer', 'c_bigint', 'c_numeric_free'):
        assert limited[name] == (1, 0), (name, limited[name])
    assert limited['bigint_plus_one'] == (2, 0)


# How each source column compares with what the SQL exporter wrote after the SQL loader,
# the R transformer and the Polars transformer. The SQL loader reads numeric as float;
# the Polars block writes the JSON values as JSON text.
SQL_COMPARISONS = {
    'id': 'd.id = s.id',
    'c_smallint': 'd.c_smallint = s.c_smallint',
    'c_integer': 'd.c_integer = s.c_integer',
    'c_bigint': 'd.c_bigint = s.c_bigint',
    'c_numeric': 'd.c_numeric = s.c_numeric::double precision',
    'c_double': 'd.c_double = s.c_double',
    'c_bool': 'd.c_bool = s.c_bool',
    'c_text': 'd.c_text = s.c_text',
    'c_bytea': 'd.c_bytea = s.c_bytea',
    'c_date': 'd.c_date = s.c_date',
    'c_time': 'd.c_time = s.c_time',
    'c_timestamp': 'd.c_timestamp = s.c_timestamp',
    'c_timestamptz': 'd.c_timestamptz = s.c_timestamptz',
    'c_interval': 'd.c_interval = s.c_interval',
    'c_uuid': 'd.c_uuid::uuid = s.c_uuid',
    'c_jsonb': 'd.c_jsonb::jsonb = s.c_jsonb',
    'c_int_array': 'd.c_int_array = s.c_int_array::bigint[]',
    'c_text_array': 'd.c_text_array = s.c_text_array',
    'c_enum': 'd.c_enum = s.c_enum::text',
    'r_text_length': 'd.r_text_length = char_length(s.c_text)',
    'r_int_array_length': 'd.r_int_array_length = coalesce(cardinality(s.c_int_array), 0)',
    # R writes the years before 1000 without leading zeros: as.character() gives
    # "1-01-01" for 0001-01-01.
    'r_day': "d.r_day = regexp_replace(s.c_date::text, '^0+', '')",
    'polars_bigint_plus_one': 'd.polars_bigint_plus_one = s.c_bigint::numeric + 1',
}
SQL_SOURCES = {
    'r_text_length': 's.c_text',
    'r_int_array_length': '0',
    'r_day': 's.c_date',
    'polars_bigint_plus_one': 's.c_bigint',
}
# Values R cannot hold: bit64 uses -2**63 as NA, a NaN becomes missing when the frame
# crosses pandas, and doubles hold microseconds only up to the year 2242.
SQL_LIMITS = {
    'c_bigint': 'c_bigint = -9223372036854775808',
    'polars_bigint_plus_one': 'c_bigint = -9223372036854775808',
    'c_numeric': "c_numeric = 'NaN'",
    'c_double': "c_double = 'NaN'",
    'c_timestamp': "c_timestamp >= '2242-01-01'",
    'c_timestamptz': "c_timestamptz >= '2242-01-01'",
}


def sql_mismatches(pg, schema):
    mismatches = {}
    with pg.cursor() as cursor:
        for name, comparison in SQL_COMPARISONS.items():
            source = SQL_SOURCES.get(name, f's.{name}')
            limit = f'AND NOT coalesce(s.{SQL_LIMITS[name]}, false)' if name in SQL_LIMITS else ''
            cursor.execute(f"""
                SELECT s.id FROM {schema}.src s JOIN {schema}.r_sql_dst d USING (id)
                WHERE (({source} IS NULL) <> (d.{name} IS NULL)
                   OR NOT coalesce({comparison}, {source} IS NULL))
                   {limit}
                ORDER BY s.id LIMIT 5
            """)
            ids = [row[0] for row in cursor.fetchall()]
            if ids:
                cursor.execute(
                    f'SELECT s.id, ({source})::text, d.{name}::text FROM {schema}.src s '
                    f'JOIN {schema}.r_sql_dst d USING (id) WHERE s.id = ANY(%s) ORDER BY 1',
                    (ids,),
                )
                mismatches[name] = cursor.fetchall()
    pg.rollback()
    return mismatches


def test_r_sql(mage_project, pg, profile_schema, source_table, source_rows):
    run_pipeline('r_sql', schema=profile_schema)

    with pg.cursor() as cursor:
        cursor.execute(f'SELECT count(*) FROM {profile_schema}.r_sql_dst')
        assert cursor.fetchone()[0] == len(source_rows)
    pg.rollback()
    assert sql_mismatches(pg, profile_schema) == {}
    assert columns(pg, profile_schema, 'r_sql_dst')['polars_bigint_plus_one'] == 'numeric'
