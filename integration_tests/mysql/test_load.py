"""
MySQL.load against MySQL 8.4: the default pandas load, exact_types and polars.

The server's time zone is -06:00, so values that go through the session time zone show
it.
"""
import datetime as dt
import decimal

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from integration_tests.data import mysql_dataset

QUERY = 'SELECT * FROM src ORDER BY id'


def as_rows(frame) -> dict:
    if isinstance(frame, pl.DataFrame):
        records = frame.to_dicts()
    else:
        records = pa.Table.from_pandas(frame, preserve_index=False).to_pylist()
    return {row['id']: row for row in records}


def test_exact_types_values_match_mysql(mage_mysql, my, mysql_source):
    expected = mysql_dataset.rows(my.cursor(), 'src')

    frame = mage_mysql.load(QUERY, verbose=False, exact_types=True)

    assert all(isinstance(dtype, pd.ArrowDtype) for dtype in frame.dtypes)
    assert mysql_dataset.mismatches(expected, as_rows(frame)) == {}


def test_polars_values_match_mysql(mage_mysql, my, mysql_source):
    expected = mysql_dataset.rows(my.cursor(), 'src')

    frame = mage_mysql.load(QUERY, verbose=False, polars=True)

    assert frame.schema['c_ubigint'] == pl.UInt64
    assert frame.schema['c_decimal'] == pl.Decimal(18, 4)
    assert frame.schema['c_timestamp'] == pl.Datetime('us', 'UTC')
    assert frame.schema['c_time'] == pl.Duration('us')
    rows = as_rows(frame)
    # DECIMAL(65,30) arrives as its exact text.
    for row in rows.values():
        if row['c_decimal_wide'] is not None:
            row['c_decimal_wide'] = decimal.Decimal(row['c_decimal_wide'])
    assert mysql_dataset.mismatches(expected, rows) == {}


def test_exact_types(mage_mysql, mysql_source):
    frame = mage_mysql.load(QUERY, verbose=False, exact_types=True)
    row = frame.set_index('id').loc[2]

    assert row['c_ubigint'] == 2**64 - 1
    assert row['c_decimal_wide'] == decimal.Decimal(
        '99999999999999999999999999999999999.999999999999999999999999999999',
    )
    assert len(row['c_text']) == 70000
    assert len(row['c_blob']) == 80000
    assert row['c_timestamp'] == pd.Timestamp('2038-01-19 03:14:07.999999', tz='UTC')
    assert row['c_set'] == ['a', 'b', 'c']


def test_wide_decimals_reach_polars_as_exact_text(mage_mysql, mysql_source):
    """
    DECIMAL(65,30) holds 65 digits. Polars decimals hold 38, and Polars panicked on the
    wider Arrow decimal.
    """
    frame = mage_mysql.load(
        'SELECT id, c_decimal, c_decimal_wide FROM src WHERE id = 2', verbose=False,
        polars=True,
    )

    assert frame.schema['c_decimal'] == pl.Decimal(18, 4)
    assert frame['c_decimal_wide'].to_list() == [
        '99999999999999999999999999999999999.999999999999999999999999999999',
    ]


def test_timestamps_are_in_utc_whatever_the_session_time_zone(mage_mysql, mysql_source):
    """
    TIMESTAMP values are returned in the session time zone. The default load gives them
    naive, in the server's -06:00; exact_types gives them in UTC.
    """
    query = 'SELECT id, c_timestamp FROM src WHERE id = 3'
    time_zone = 'SELECT @@session.time_zone AS tz'
    before = mage_mysql.load(time_zone, verbose=False)['tz'][0]

    default = mage_mysql.load(query, verbose=False)
    exact = mage_mysql.load(query, verbose=False, exact_types=True)

    assert before == '-06:00'
    assert default['c_timestamp'][0] == pd.Timestamp('2024-02-29 11:34:56.789012')
    assert exact['c_timestamp'][0] == pd.Timestamp('2024-02-29 17:34:56.789012', tz='UTC')
    # The session keeps its time zone after the load.
    assert mage_mysql.load(time_zone, verbose=False)['tz'][0] == before


def test_default_load_turns_integers_with_nulls_into_floats(mage_mysql, mysql_source):
    """pandas read_sql: integer columns with a NULL become float64, DECIMAL becomes float."""
    frame = mage_mysql.load(QUERY, verbose=False)

    assert frame['c_bigint'].dtype == 'float64'
    assert frame.loc[frame['id'] == 3, 'c_bigint'].item() == 2**53
    assert frame['c_decimal'].dtype == 'float64'


def test_params(mage_mysql, mysql_source):
    frame = mage_mysql.load(
        'SELECT id FROM src WHERE id IN (%s, %s) ORDER BY id',
        verbose=False, exact_types=True, params=(3, 2),
    )

    assert frame['id'].tolist() == [2, 3]


def test_repeated_column_names_raise(mage_mysql, mysql_source):
    """The limit wraps the query in a derived table, whose columns MySQL requires unique."""
    with pytest.raises(Exception, match='Duplicate column'):
        mage_mysql.load('SELECT id, id FROM src', verbose=False, exact_types=True)


def test_empty_results_keep_their_types(mage_mysql, mysql_source):
    frame = mage_mysql.load('SELECT * FROM src WHERE id < 0', verbose=False, polars=True)

    assert frame.height == 0
    assert frame.schema['c_bigint'] == pl.Int64
    assert frame.schema['c_date'] == pl.Date


def test_limit(mage_mysql, mysql_source):
    frame = mage_mysql.load(QUERY, limit=7, verbose=False, exact_types=True)

    assert len(frame) == 7


def test_time_values(mage_mysql, mysql_source):
    frame = mage_mysql.load('SELECT id, c_time FROM src WHERE id IN (1, 4) ORDER BY id',
                            verbose=False, polars=True)

    assert frame['c_time'].to_list() == [
        -dt.timedelta(hours=838, minutes=59, seconds=59),
        -dt.timedelta(microseconds=1),
    ]


def test_each_load_sees_committed_rows(mage_mysql, my, mysql_source):
    """
    A load left its transaction open, so the next load on the client read the same
    snapshot and missed rows other sessions committed.
    """
    query = 'SELECT COUNT(*) AS n FROM src'
    before = mage_mysql.load(query, verbose=False)['n'][0]
    with my.cursor() as cursor:
        cursor.execute('INSERT INTO src (id) VALUES (-1)')

    after = mage_mysql.load(query, verbose=False, exact_types=True)['n'][0]

    assert after == before + 1


def test_a_load_does_not_block_other_sessions(mage_mysql, my, mysql_source):
    """An open transaction held a metadata lock that made other sessions' DDL wait."""
    mage_mysql.load('SELECT id FROM src LIMIT 1', verbose=False)

    with my.cursor() as cursor:
        cursor.execute('SET SESSION lock_wait_timeout = 5')
        cursor.execute('ALTER TABLE src ADD COLUMN added INT')
        cursor.execute('SELECT COUNT(*) FROM information_schema.columns '
                       "WHERE table_schema = DATABASE() AND column_name = 'added'")
        assert cursor.fetchone() == (1,)
