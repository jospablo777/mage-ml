"""
Values written by Mage's PostgreSQL export, one case per way a value reaches it.

Each case writes through COPY and through INSERT ... ON CONFLICT and reads the result
with psycopg2.
"""

import datetime
import decimal
import uuid

import numpy as np
import pandas as pd
import polars as pl
import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

PATHS = {
    'copy': dict(),
    'upsert': dict(unique_constraints=['id'], unique_conflict_method='UPDATE'),
}


def run(pg, statement):
    with pg.cursor() as cursor:
        cursor.execute(statement)
    pg.commit()


def fetch(pg, schema, table='t', columns='*'):
    with pg.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(f'SELECT {columns} FROM {schema}.{table} ORDER BY id')
        result = [dict(row) for row in cursor.fetchall()]
    pg.rollback()
    return result


def export(client, schema, data, path='copy', table='t', **kwargs):
    kwargs.setdefault('if_exists', 'append')
    client.export(
        data, schema_name=schema, table_name=table, verbose=False, **PATHS[path], **kwargs
    )


def column(pg, schema, name, table='t'):
    return [row[name] for row in fetch(pg, schema, table)]


def nan(value):
    return isinstance(value, float) and value != value


# --- text ---------------------------------------------------------------------------------

TEXTS = [
    '',
    None,
    '\\N',
    'NULL',
    'None',
    'nan',
    'tab\there',
    'line\nbreak',
    'cr\rhere',
    'back\\slash',
    '"quoted"',
    "it's",
    '{"json": "text"}',
    'ñandú 🦊 中文 العربية',
]


@pytest.mark.parametrize('path', sorted(PATHS))
@pytest.mark.parametrize('kind', ['pandas_object', 'pandas_str', 'polars'])
def test_text_values(mage_postgres, pg, schema, path, kind):
    run(pg, f'CREATE TABLE {schema}.t (id integer PRIMARY KEY, v text)')
    ids = list(range(len(TEXTS)))
    if kind == 'polars':
        data = pl.DataFrame({'id': ids, 'v': TEXTS})
    else:
        data = pd.DataFrame(
            {'id': ids, 'v': pd.Series(TEXTS, dtype=object if kind == 'pandas_object' else 'str')}
        )

    export(mage_postgres, schema, data, path)

    assert column(pg, schema, 'v') == TEXTS


# --- floats -------------------------------------------------------------------------------

FLOATS = [
    1.5,
    float('nan'),
    None,
    float('inf'),
    float('-inf'),
    -0.0,
    5e-324,
    1.7976931348623157e308,
]


@pytest.mark.parametrize('path', sorted(PATHS))
@pytest.mark.parametrize('kind', ['pandas_float64', 'pandas_object', 'polars'])
def test_float_values(mage_postgres, pg, schema, path, kind):
    run(pg, f'CREATE TABLE {schema}.t (id integer PRIMARY KEY, v double precision)')
    ids = list(range(len(FLOATS)))
    if kind == 'polars':
        data = pl.DataFrame({'id': ids, 'v': FLOATS}, schema={'id': pl.Int64, 'v': pl.Float64})
    elif kind == 'pandas_object':
        data = pd.DataFrame({'id': ids, 'v': pd.Series(FLOATS, dtype=object)})
    else:
        data = pd.DataFrame({'id': ids, 'v': pd.Series(FLOATS, dtype='float64')})

    export(mage_postgres, schema, data, path)

    values = column(pg, schema, 'v')
    if kind == 'pandas_float64':
        # float64 has one missing marker, so NaN and NULL both arrive as NULL.
        assert values[1] is None and values[2] is None
    else:
        assert nan(values[1]) and values[2] is None
    assert values[0] == 1.5 and values[3:] == FLOATS[3:]


@pytest.mark.parametrize('kind', ['pandas', 'polars'])
def test_integral_floats_fill_integer_columns(mage_postgres, pg, schema, kind):
    run(pg, f'CREATE TABLE {schema}.t (id integer PRIMARY KEY, v bigint)')
    values = [1.0, None, -32768.0, 9007199254740992.0]
    data = (
        pl.DataFrame({'id': [0, 1, 2, 3], 'v': values})
        if kind == 'polars'
        else pd.DataFrame({'id': [0, 1, 2, 3], 'v': values})
    )

    export(mage_postgres, schema, data)

    assert column(pg, schema, 'v') == [1, None, -32768, 9007199254740992]


@pytest.mark.parametrize('path', sorted(PATHS))
def test_fractional_float_into_an_integer_column_fails(mage_postgres, pg, schema, path):
    run(pg, f'CREATE TABLE {schema}.t (id integer PRIMARY KEY, v integer)')

    with pytest.raises(psycopg2.errors.InvalidTextRepresentation):
        export(mage_postgres, schema, pd.DataFrame({'id': [1], 'v': [1.5]}), path)

    assert fetch(pg, schema) == []


# --- integers and booleans ----------------------------------------------------------------


@pytest.mark.parametrize('kind', ['pandas_int64', 'pandas_Int64', 'polars'])
def test_bigint_limits(mage_postgres, pg, schema, kind):
    values = [-(2**63), 2**63 - 1, 0]
    if kind == 'polars':
        data = pl.DataFrame({'id': [1, 2, 3], 'v': values}, schema={'id': pl.Int64, 'v': pl.Int64})
    else:
        data = pd.DataFrame({'id': [1, 2, 3], 'v': pd.Series(values, dtype=kind.split('_')[1])})

    export(mage_postgres, schema, data, if_exists='replace')

    assert column(pg, schema, 'v') == values


@pytest.mark.parametrize('kind', ['pandas', 'polars'])
def test_uint64_creates_a_numeric_column(mage_postgres, pg, schema, kind):
    values = [0, 2**64 - 1]
    data = (
        pl.DataFrame({'id': [1, 2], 'v': values}, schema={'id': pl.Int64, 'v': pl.UInt64})
        if kind == 'polars'
        else pd.DataFrame({'id': [1, 2], 'v': pd.Series(values, dtype='uint64')})
    )

    export(mage_postgres, schema, data, if_exists='replace')

    assert column(pg, schema, 'v') == [decimal.Decimal(v) for v in values]


@pytest.mark.parametrize('kind', ['numpy_bool', 'pandas_boolean', 'polars'])
def test_booleans(mage_postgres, pg, schema, kind):
    if kind == 'numpy_bool':
        data, expected = pd.DataFrame({'id': [1, 2], 'v': np.array([True, False])}), [True, False]
    elif kind == 'pandas_boolean':
        data = pd.DataFrame({'id': [1, 2, 3], 'v': pd.array([True, None, False], dtype='boolean')})
        expected = [True, None, False]
    else:
        data, expected = (
            pl.DataFrame({'id': [1, 2, 3], 'v': [True, None, False]}),
            [True, None, False],
        )

    export(mage_postgres, schema, data, if_exists='replace')

    assert column(pg, schema, 'v') == expected


# --- dates and times ----------------------------------------------------------------------


def test_naive_and_aware_timestamps_with_a_session_timezone(postgres_settings, pg, schema):
    """
    An aware timestamp keeps its instant. A naive timestamp written to a timestamptz
    column is read in the session time zone, as PostgreSQL does.
    """
    from mage_ai.io.postgres import Postgres

    client = Postgres(verbose=False, options='-c timezone=America/Costa_Rica', **postgres_settings)
    client.open()
    try:
        run(
            pg,
            f'CREATE TABLE {schema}.t (id integer PRIMARY KEY, aware timestamptz, '
            'naive_tz timestamptz, naive timestamp)',
        )
        instant = datetime.datetime(2024, 6, 1, 12, 0, 0, 123456, tzinfo=datetime.timezone.utc)
        naive = datetime.datetime(2024, 6, 1, 12, 0, 0, 123456)
        data = pd.DataFrame({'id': [1], 'aware': [instant], 'naive_tz': [naive], 'naive': [naive]})

        client.export(data, schema_name=schema, table_name='t', if_exists='append', verbose=False)
        loaded = client.load(f'SELECT * FROM {schema}.t', verbose=False, exact_types=True)
    finally:
        client.close()

    row = fetch(pg, schema)[0]
    assert row['aware'] == instant
    assert row['naive'] == naive
    assert row['naive_tz'] == naive.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=-6)))
    # Loads return timestamptz in UTC whatever the session time zone.
    assert loaded['aware'].iloc[0] == pd.Timestamp(instant)
    assert str(loaded['aware'].dtype) == 'datetime64[us, UTC]'


def test_nanoseconds_round_to_microseconds(mage_postgres, pg, schema):
    stamps = pd.to_datetime(['2024-01-01 00:00:00.000001499', '2024-01-01 00:00:00.000001501'])
    data = pd.DataFrame({'id': [1, 2], 'v': stamps})

    export(mage_postgres, schema, data, if_exists='replace')

    assert column(pg, schema, 'v') == [
        datetime.datetime(2024, 1, 1, 0, 0, 0, 1),
        datetime.datetime(2024, 1, 1, 0, 0, 0, 2),
    ]


def test_polars_nanoseconds_round_to_microseconds(mage_postgres, pg, schema):
    """Polars columns were truncated to microseconds; pandas columns are rounded."""
    stamps = pl.Series(
        [1704067200000001499, 1704067200000001501], dtype=pl.Int64,
    ).cast(pl.Datetime('ns'))
    data = pl.DataFrame({'id': [1, 2], 'v': stamps})

    export(mage_postgres, schema, data, if_exists='replace')

    assert column(pg, schema, 'v') == [
        datetime.datetime(2024, 1, 1, 0, 0, 0, 1),
        datetime.datetime(2024, 1, 1, 0, 0, 0, 2),
    ]


def test_polars_dates_after_the_year_9999(mage_postgres, pg, schema):
    """
    Python datetimes end at the year 9999, and the export of a Polars column with a later
    value raised a Rust panic. PostgreSQL holds years up to 294276.
    """
    data = pl.DataFrame({'id': [1, 2, 3]}).with_columns(
        at=pl.Series([
            datetime.datetime(9999, 12, 31, 23, 59, 59, 999999), None,
            datetime.datetime(2024, 1, 1),
        ], dtype=pl.Datetime('us')),
        zoned=pl.Series([
            datetime.datetime(2024, 7, 1, 12, tzinfo=datetime.timezone.utc), None, None,
        ]).dt.convert_time_zone('America/New_York'),
        day=pl.Series([datetime.date(1, 1, 1), None, datetime.date(2024, 2, 29)]),
    ).with_columns(
        at=pl.when(pl.col('id') == 3).then(pl.datetime(10000, 1, 1, 12)).otherwise('at'),
        day=pl.when(pl.col('id') == 3).then(pl.date(12345, 6, 7)).otherwise('day'),
    )

    export(mage_postgres, schema, data, if_exists='replace')

    columns = 'id, _at::text AS at, zoned::text AS zoned, _day::text AS day'
    assert fetch(pg, schema, columns=columns) == [
        dict(id=1, at='9999-12-31 23:59:59.999999', zoned='2024-07-01 12:00:00+00',
             day='0001-01-01'),
        dict(id=2, at=None, zoned=None, day=None),
        dict(id=3, at='10000-01-01 12:00:00', zoned=None, day='12345-06-07'),
    ]


@pytest.mark.parametrize('kind', ['pandas', 'polars'])
def test_dates_times_and_intervals(mage_postgres, pg, schema, kind):
    dates = [datetime.date(1, 1, 1), None, datetime.date(9999, 12, 31)]
    times = [datetime.time(0, 0), None, datetime.time(23, 59, 59, 999999)]
    intervals = [
        datetime.timedelta(days=-90, microseconds=1),
        None,
        datetime.timedelta(days=36500, seconds=86399),
    ]
    if kind == 'polars':
        data = pl.DataFrame({'id': [1, 2, 3], 'd': dates, 'tm': times, 'i': intervals})
    else:
        data = pd.DataFrame(
            {'id': [1, 2, 3], 'd': dates, 'tm': times, 'i': pd.Series(pd.to_timedelta(intervals))}
        )

    export(mage_postgres, schema, data, if_exists='replace')

    rows = fetch(pg, schema)
    assert [r['d'] for r in rows] == dates
    assert [r['tm'] for r in rows] == times
    assert [r['i'] for r in rows] == intervals


def test_datetime_values_into_a_date_column(mage_postgres, pg, schema):
    run(pg, f'CREATE TABLE {schema}.t (id integer PRIMARY KEY, d date)')
    data = pd.DataFrame({'id': [1, 2], 'd': pd.to_datetime(['2024-02-29', None])})

    export(mage_postgres, schema, data)

    assert column(pg, schema, 'd') == [datetime.date(2024, 2, 29), None]


# --- JSON ---------------------------------------------------------------------------------


@pytest.mark.parametrize('path', sorted(PATHS))
def test_json_values_from_pandas(mage_postgres, pg, schema, path):
    run(pg, f'CREATE TABLE {schema}.t (id integer PRIMARY KEY, j jsonb)')
    values = [
        {'a': 1, 'nested': {'b': [1, 2, None]}},
        [1, 'two', None],
        {
            'nan': float('nan'),
            'decimal': decimal.Decimal('1.25'),
            'when': datetime.datetime(2024, 1, 1, 12, 0),
        },
        '{"already": "json text"}',
        'plain text',
        None,
        {'unicode': 'ñandú 🦊', 'escapes': 'tab\t"q"\\'},
    ]
    data = pd.DataFrame({'id': range(len(values)), 'j': pd.Series(values, dtype=object)})

    export(mage_postgres, schema, data, path)

    assert column(pg, schema, 'j') == [
        {'a': 1, 'nested': {'b': [1, 2, None]}},
        [1, 'two', None],
        {'nan': None, 'decimal': 1.25, 'when': '2024-01-01T12:00:00'},
        {'already': 'json text'},
        'plain text',
        None,
        {'unicode': 'ñandú 🦊', 'escapes': 'tab\t"q"\\'},
    ]


def test_polars_struct_and_json_text(mage_postgres, pg, schema):
    data = pl.DataFrame(
        {
            'id': [1, 2],
            's': [{'a': 1, 'b': 'x'}, {'a': None, 'b': 'y'}],
            'j': ['{"k": [1, 2]}', None],
        }
    )

    export(mage_postgres, schema, data, if_exists='replace', overwrite_types={'j': 'jsonb'})

    rows = fetch(pg, schema)
    assert [r['s'] for r in rows] == [{'a': 1, 'b': 'x'}, {'a': None, 'b': 'y'}]
    assert [r['j'] for r in rows] == [{'k': [1, 2]}, None]


# --- arrays -------------------------------------------------------------------------------


@pytest.mark.parametrize('path', sorted(PATHS))
@pytest.mark.parametrize('kind', ['pandas_list', 'pandas_ndarray', 'polars'])
def test_array_values(mage_postgres, pg, schema, path, kind):
    run(
        pg,
        f'CREATE TABLE {schema}.t (id integer PRIMARY KEY, a text[], n integer[], m integer[][])',
    )
    texts = [['x', 'NULL', None, '', 'with "q"', 'with,comma', '{brace}', 'back\\slash'], [], None]
    numbers = [[1, None, -3], [], None]
    matrix = [[[1, 2], [3, 4]], [], None]
    if kind == 'polars':
        data = pl.DataFrame({'id': [1, 2, 3], 'a': texts, 'n': numbers, 'm': matrix})
    else:

        def wrap(value):
            return (
                np.array(value, dtype=object)
                if kind == 'pandas_ndarray' and value is not None
                else value
            )

        data = pd.DataFrame(
            {
                'id': [1, 2, 3],
                'a': pd.Series([wrap(v) for v in texts], dtype=object),
                'n': pd.Series([wrap(v) for v in numbers], dtype=object),
                'm': pd.Series([wrap(v) for v in matrix], dtype=object),
            }
        )

    export(mage_postgres, schema, data, path)

    rows = fetch(pg, schema)
    assert [r['a'] for r in rows] == texts
    assert [r['n'] for r in rows] == numbers
    assert [r['m'] for r in rows] == matrix


# --- bytes, decimals, categories, uuids ------------------------------------------------------


@pytest.mark.parametrize('path', sorted(PATHS))
def test_bytes_values(mage_postgres, pg, schema, path):
    run(pg, f'CREATE TABLE {schema}.t (id integer PRIMARY KEY, b bytea)')
    values = [b'\x00\xff', bytearray(b'\x01'), memoryview(b'\x02\x03'), b'', None]
    data = pd.DataFrame({'id': range(5), 'b': pd.Series(values, dtype=object)})

    export(mage_postgres, schema, data, path)

    assert [None if v is None else bytes(v) for v in column(pg, schema, 'b')] == [
        b'\x00\xff',
        b'\x01',
        b'\x02\x03',
        b'',
        None,
    ]


def test_polars_binary(mage_postgres, pg, schema):
    data = pl.DataFrame({'id': [1, 2, 3], 'b': [b'\x00', b'', None]})

    export(mage_postgres, schema, data, if_exists='replace')

    assert [None if v is None else bytes(v) for v in column(pg, schema, 'b')] == [
        b'\x00',
        b'',
        None,
    ]


def test_decimals_beyond_the_column_scale_are_rounded_by_postgres(mage_postgres, pg, schema):
    run(pg, f'CREATE TABLE {schema}.t (id integer PRIMARY KEY, v numeric(10, 2))')
    data = pd.DataFrame({'id': [1, 2], 'v': [decimal.Decimal('1.005'), decimal.Decimal('NaN')]})

    export(mage_postgres, schema, data)

    values = column(pg, schema, 'v')
    assert values[0] == decimal.Decimal('1.01')
    assert values[1].is_nan()


@pytest.mark.parametrize('kind', ['pandas_category', 'polars_categorical', 'polars_enum'])
def test_categories_become_text(mage_postgres, pg, schema, kind):
    if kind == 'pandas_category':
        data = pd.DataFrame({'id': [1, 2], 'label': pd.Categorical(['a', None])})
    elif kind == 'polars_categorical':
        data = pl.DataFrame({'id': [1, 2], 'label': pl.Series(['a', None], dtype=pl.Categorical)})
    else:
        data = pl.DataFrame(
            {'id': [1, 2], 'label': pl.Series(['a', None], dtype=pl.Enum(['a', 'b']))}
        )

    export(mage_postgres, schema, data, if_exists='replace')

    assert column(pg, schema, 'label') == ['a', None]
    with pg.cursor() as cursor:
        cursor.execute(
            'SELECT format_type(atttypid, atttypmod) FROM pg_attribute '
            "WHERE attrelid = to_regclass(%s) AND attname = 'label'",
            (f'{schema}.t',),
        )
        assert cursor.fetchone()[0] == 'text'


def test_uuid_objects_create_a_uuid_column(mage_postgres, pg, schema):
    ids = [uuid.UUID(int=1), None]
    data = pd.DataFrame({'id': [1, 2], 'u': pd.Series(ids, dtype=object)})

    export(mage_postgres, schema, data, if_exists='replace')

    assert column(pg, schema, 'u') == ['00000000-0000-0000-0000-000000000001', None]
    with pg.cursor() as cursor:
        cursor.execute(
            'SELECT format_type(atttypid, atttypmod) FROM pg_attribute '
            "WHERE attrelid = to_regclass(%s) AND attname = 'u'",
            (f'{schema}.t',),
        )
        assert cursor.fetchone()[0] == 'uuid'


# --- frame shapes and inputs ------------------------------------------------------------------


@pytest.mark.parametrize('kind', ['pandas', 'polars'])
def test_empty_frame_creates_a_typed_table(mage_postgres, pg, schema, kind):
    if kind == 'polars':
        data = pl.DataFrame(schema={'id': pl.Int32, 'v': pl.Float64, 's': pl.String})
    else:
        data = pd.DataFrame(
            {
                'id': pd.Series([], dtype='int32'),
                'v': pd.Series([], dtype='float64'),
                's': pd.Series([], dtype='str'),
            }
        )

    export(mage_postgres, schema, data, if_exists='replace')

    assert fetch(pg, schema) == []
    with pg.cursor() as cursor:
        cursor.execute(
            'SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute '
            'WHERE attrelid = to_regclass(%s) AND attnum > 0 ORDER BY attnum',
            (f'{schema}.t',),
        )
        assert cursor.fetchall() == [('id', 'integer'), ('v', 'double precision'), ('s', 'text')]


@pytest.mark.parametrize('kind', ['pandas', 'polars'])
def test_all_null_column(mage_postgres, pg, schema, kind):
    data = (
        pl.DataFrame({'id': [1, 2], 'v': [None, None]})
        if kind == 'polars'
        else pd.DataFrame({'id': [1, 2], 'v': [None, None]})
    )

    export(mage_postgres, schema, data, if_exists='replace')

    assert column(pg, schema, 'v') == [None, None]


def test_dict_list_and_lazy_inputs(mage_postgres, pg, schema):
    export(mage_postgres, schema, {'id': 1, 'v': 'dict'}, table='a', if_exists='replace')
    export(
        mage_postgres,
        schema,
        [{'id': 1, 'v': 'list'}, {'id': 2, 'v': None}],
        table='b',
        if_exists='replace',
    )
    export(
        mage_postgres,
        schema,
        pl.LazyFrame({'id': [1], 'v': ['lazy']}),
        table='c',
        if_exists='replace',
    )

    assert column(pg, schema, 'v', 'a') == ['dict']
    assert column(pg, schema, 'v', 'b') == ['list', None]
    assert column(pg, schema, 'v', 'c') == ['lazy']


def test_index_is_exported_when_asked(mage_postgres, pg, schema):
    data = pd.DataFrame({'v': ['a', 'b']}, index=pd.Index([10, 20], name='id'))

    export(mage_postgres, schema, data, if_exists='replace', index=True)

    assert fetch(pg, schema) == [{'id': 10, 'v': 'a'}, {'id': 20, 'v': 'b'}]
