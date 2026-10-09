"""
Loading DuckDB query results with Mage's client, compared with DuckDB's own values.

The default load goes through pandas.read_sql. exact_types=True and polars=True read
DuckDB's Arrow result.
"""
import datetime as dt
import decimal
import uuid

import duckdb
import pandas as pd
import polars as pl
import pytest

from integration_tests.api.canonical import canonical, columns, same
from integration_tests.data import duckdb_dataset

QUERY = 'SELECT * FROM src ORDER BY id'
MODES = {
    'default': {},
    'exact_types': dict(exact_types=True),
    'polars': dict(polars=True),
}
# Columns whose values differ from DuckDB's Python values, by load. Nanoseconds are left
# out: Python datetimes hold microseconds, and the round trip tests compare them in SQL.
DIFFERENCES = {
    # read_sql: integer columns with a NULL become float, decimals become float.
    'default': {
        'c_tinyint', 'c_smallint', 'c_integer', 'c_bigint', 'c_utinyint', 'c_usmallint',
        'c_uinteger', 'c_ubigint', 'c_decimal_small', 'c_decimal', 'c_decimal_wide',
    },
    # Different Python representations of equal values: UUID as text, MAP as key and
    # value pairs, INTERVAL as Arrow's month, day and nanosecond value, HUGEINT as an
    # integral Decimal.
    'exact_types': {'c_uuid', 'c_map', 'c_interval', 'c_interval_months', 'c_hugeint'},
    # UUID as text, intervals with months as a struct of months, days and nanoseconds.
    'polars': {'c_uuid', 'c_interval_months'},
}


def python_values(frame):
    values = columns(frame)
    for name, column in values.items():
        values[name] = [
            v.to_pytimedelta() if isinstance(v, pd.Timedelta) else v for v in column
        ]
    return values


@pytest.mark.parametrize('mode', sorted(MODES))
def test_values_match_duckdb(duckdb_client, mode):
    native = duckdb_client.conn.execute(QUERY).fetchall()
    frame = duckdb_client.load(QUERY, verbose=False, **MODES[mode])

    got = python_values(frame)
    differing = set()
    for index, column in enumerate(duckdb_dataset.COLUMN_NAMES):
        if column == 'c_timestamp_ns':
            continue
        expected = [canonical(row[index]) for row in native]
        if not all(same(a, b) for a, b in zip(got[column], expected)):
            differing.add(column)

    assert differing == DIFFERENCES[mode]


def test_representations_of_equal_values(duckdb_client):
    exact = duckdb_client.load(QUERY, verbose=False, exact_types=True)
    polars = duckdb_client.load(QUERY, verbose=False, polars=True)

    assert exact['c_uuid'].tolist()[1] == 'ffffffff-ffff-ffff-ffff-ffffffffffff'
    assert exact['c_map'].tolist()[1] == [('k', 1), ('ñ', None)]
    assert exact['c_hugeint'].tolist()[2] == decimal.Decimal(18446744073709551616)
    assert polars['c_interval_months'].to_list()[1] == dict(months=14, days=3, nanoseconds=0)
    assert polars['c_interval'].to_list()[1] == dt.timedelta(days=1, seconds=3723,
                                                             microseconds=456789)


@pytest.mark.parametrize('mode', ['exact_types', 'polars'])
def test_nanoseconds_are_kept(duckdb_client, mode):
    """DuckDB's Python conversion rounds them: .999999999 becomes the next second."""
    frame = duckdb_client.load(
        'SELECT c_timestamp_ns FROM src WHERE id = 4', verbose=False, **MODES[mode],
    )
    native = duckdb_client.conn.execute('SELECT c_timestamp_ns FROM src WHERE id = 4').fetchone()

    value = frame['c_timestamp_ns'][0]
    nanoseconds = value.value if isinstance(value, pd.Timestamp) else None
    if mode == 'polars':
        nanoseconds = frame['c_timestamp_ns'].cast(pl.Int64)[0]
    assert nanoseconds == -1
    assert native[0] == dt.datetime(1970, 1, 1)


def test_the_default_load_rounds_nanoseconds_to_the_next_second(duckdb_client):
    """read_sql gets DuckDB's Python values, so 23:59:59.999999999 becomes midnight."""
    frame = duckdb_client.load('SELECT c_timestamp_ns FROM src WHERE id = 4', verbose=False)

    assert frame['c_timestamp_ns'][0] == pd.Timestamp('1970-01-01 00:00:00')


def test_128_bit_integers_are_exact(duckdb_client):
    """
    DuckDB exports UHUGEINT to Arrow as decimal128, which wraps values above 2**127:
    the largest arrives as -1. Polars rounds 39-digit HUGEINT values to 38 digits.
    """
    query = (
        'SELECT h AS c_hugeint, u AS c_uhuge FROM (VALUES '
        '(-170141183460469231731687303715884105727::HUGEINT - 1, '
        '18446744073709551615::UHUGEINT), '
        '(170141183460469231731687303715884105727::HUGEINT, '
        '340282366920938463463374607431768211455::UHUGEINT), '
        '(NULL::HUGEINT, NULL::UHUGEINT)) v(h, u)'
    )
    arrow = duckdb_client.conn.sql(
        'SELECT 340282366920938463463374607431768211455::UHUGEINT AS v',
    ).to_arrow_table()

    polars = duckdb_client.load(query, verbose=False, polars=True)
    exact = duckdb_client.load(query, verbose=False, exact_types=True)

    assert arrow['v'].to_pylist() == [decimal.Decimal(-1)]
    hugeints = [-(2**127), 2**127 - 1, None]
    assert polars['c_hugeint'].to_list() == hugeints
    assert polars.schema['c_hugeint'] == pl.Int128
    assert polars['c_uhuge'].to_list() == [2**64 - 1, 2**128 - 1, None]
    assert exact['c_uhuge'].tolist() == [2**64 - 1, 2**128 - 1, None]


@pytest.mark.parametrize('mode', ['exact_types', 'polars'])
def test_zoned_timestamps_are_in_utc(duckdb_client, mode):
    """DuckDB exports TIMESTAMPTZ in the session time zone."""
    duckdb_client.conn.execute("SET TimeZone = 'Asia/Kathmandu'")

    frame = duckdb_client.load(
        'SELECT c_timestamptz FROM src WHERE id = 3', verbose=False, **MODES[mode],
    )

    value = canonical(frame['c_timestamptz'][0])
    assert value == dt.datetime(2024, 2, 29, 17, 34, 56, 789012, tzinfo=dt.timezone.utc)
    assert value.utcoffset() == dt.timedelta(0)


def test_uuid_and_json_text(duckdb_client):
    frame = duckdb_client.load(
        'SELECT c_uuid, c_json FROM src WHERE id = 2', verbose=False, polars=True,
    )

    assert uuid.UUID(frame['c_uuid'][0]) == uuid.UUID(int=2**128 - 1)
    assert frame['c_json'][0] == '{"a": [1, null, {"b": "ñ"}], "n": 1.5}'


@pytest.mark.parametrize('mode', sorted(MODES))
def test_empty_results_keep_their_columns(duckdb_client, mode):
    frame = duckdb_client.load(
        'SELECT * FROM src WHERE id < 0', verbose=False, **MODES[mode],
    )

    assert list(frame.columns) == duckdb_dataset.COLUMN_NAMES
    assert len(frame) == 0


def test_parameters_and_limit(duckdb_client):
    frame = duckdb_client.load(
        'SELECT id FROM src WHERE id > ? ORDER BY id', verbose=False, polars=True,
        params=[1000], limit=3,
    )

    assert frame['id'].to_list() == [1001, 1002, 1003]


def test_query_errors_raise(duckdb_client):
    with pytest.raises(duckdb.CatalogException):
        duckdb_client.load('SELECT * FROM missing', verbose=False, polars=True)
