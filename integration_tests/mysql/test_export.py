"""
MySQL.export against MySQL 8.4.
"""
import datetime as dt
import decimal

import numpy as np
import pandas as pd
import polars as pl
import pytest

from integration_tests.data import mysql_dataset

# Columns whose type the export changes, with the values kept: ENUM and SET become text
# and JSON, BINARY(4) becomes LONGBLOB.
ROUND_TRIP_COLUMNS = [c for c in mysql_dataset.COLUMN_NAMES if c != 'c_set']


def column_types(my, table):
    with my.cursor() as cursor:
        cursor.execute(
            'SELECT column_name, column_type FROM information_schema.columns '
            'WHERE table_schema = DATABASE() AND table_name = %s ORDER BY ordinal_position',
            (table,),
        )
        return dict(cursor.fetchall())


def stored(my, table, columns=None):
    return mysql_dataset.rows(my.cursor(), table, columns)


@pytest.mark.parametrize('mode', ['exact_types', 'polars'])
def test_round_trip(mage_mysql, my, mysql_source, mode):
    frame = mage_mysql.load('SELECT * FROM src', verbose=False, **{mode: True})
    if mode == 'polars':
        # DECIMAL(65,30) reaches Polars as text; the copy stores the text.
        frame = frame.drop('c_decimal_wide')

    mage_mysql.export(frame, None, 'copy', verbose=False, allow_reserved_words=True)

    columns = [c for c in ROUND_TRIP_COLUMNS if c in frame.columns]
    assert mysql_dataset.mismatches(
        stored(my, 'src', columns), stored(my, 'copy', columns), columns,
    ) == {}
    # SET values are stored as a JSON array of their members.
    assert stored(my, 'copy', ['id', 'c_set'])[2]['c_set'] == '["a", "b", "c"]'


def test_created_types(mage_mysql, my):
    """
    FLOAT and DECIMAL columns were created as DECIMAL, which MySQL reads as DECIMAL(10,0),
    so 0.25 was stored as 0. Booleans were CHAR(52), bytes VARBINARY(255), text TEXT,
    and timestamps TIMESTAMP without fractional seconds.
    """
    frame = pd.DataFrame({
        'id': [1, 2],
        'n': [1, 2],
        'f': [0.25, 1e300],
        'b': [True, False],
        's': ['a', 'b'],
        'raw': [b'\x00', b'\xff'],
        'at': pd.to_datetime(['2024-01-01 00:00:00.123456', '2100-01-01'], format='ISO8601'),
        'day': [dt.date(2024, 1, 1), dt.date(1000, 1, 1)],
        'amount': [decimal.Decimal('12345.6789'), decimal.Decimal('-0.01')],
        'doc': [{'a': [1, None]}, [1, 2]],
    })

    mage_mysql.export(frame, None, 'created', verbose=False, allow_reserved_words=True)

    assert column_types(my, 'created') == {
        'id': 'bigint', 'n': 'bigint', 'f': 'double', 'b': 'tinyint(1)', 's': 'longtext',
        'raw': 'longblob', 'at': 'datetime(6)', 'day': 'date', 'amount': 'decimal(9,4)',
        'doc': 'json',
    }
    rows = stored(my, 'created', list(frame.columns))
    assert rows[1]['f'] == 0.25 and rows[2]['f'] == 1e300
    assert rows[1]['at'] == dt.datetime(2024, 1, 1, 0, 0, 0, 123456)
    assert rows[2]['amount'] == decimal.Decimal('-0.0100')


def test_numpy_backed_frames(mage_mysql, my, mysql_source):
    """The default pandas load, with floats for integers with NULLs, exports its values."""
    frame = mage_mysql.load(
        'SELECT id, c_int, c_double, c_varchar, c_date, c_datetime FROM src', verbose=False,
    )

    mage_mysql.export(frame, None, 'copy', verbose=False)

    columns = list(frame.columns)
    expected = stored(my, 'src', columns)
    assert mysql_dataset.mismatches(expected, stored(my, 'copy', columns), columns) == {}


def test_text_and_bytes_keep_their_values(mage_mysql, my):
    """
    Text values lost their surrounding double quotes: '"quoted"' was stored as quoted.
    TEXT holds 64 KB and VARBINARY(255) 255 bytes.
    """
    values = ['"quoted"', '"', 'ñandú 中文 🐍', 'a' * 100_000, '', None]
    blobs = [b'"x"', b'\x00' * 300, b'\xff', b'', b'\x00', None]
    frame = pd.DataFrame({'id': range(6), 'text': values, 'raw': blobs})

    mage_mysql.export(frame, None, 'texts', verbose=False, allow_reserved_words=True)

    rows = stored(my, 'texts', ['id', 'text', 'raw'])
    assert [rows[i]['text'] for i in range(6)] == values
    assert [rows[i]['raw'] for i in range(6)] == blobs


def test_zoned_timestamps_are_stored_in_utc(mage_mysql, my):
    """
    Zoned values were sent as text with an offset, which MySQL converts to the session
    time zone, -06:00 here. They are stored in UTC.
    """
    frame = pd.DataFrame({
        'id': [1, 2, 3],
        'at': pd.to_datetime(
            ['2024-07-01 12:00:00.000001', '2024-12-01 12:00', None], format='ISO8601',
        ).tz_localize('America/New_York'),
    })

    mage_mysql.export(frame, None, 'zoned', verbose=False, allow_reserved_words=True)

    rows = stored(my, 'zoned', ['id', 'at'])
    assert [rows[i]['at'] for i in (1, 2, 3)] == [
        dt.datetime(2024, 7, 1, 16, 0, 0, 1), dt.datetime(2024, 12, 1, 17, 0), None,
    ]


def test_durations(mage_mysql, my):
    """
    Durations within TIME's range are TIME(6). Longer ones are integer nanoseconds, and
    NaT was written as -9223372036854775808 nanoseconds.
    """
    short = pd.DataFrame({'id': [1, 2, 3], 'span': pd.to_timedelta(['1 s', None, '-838 h'])})
    long = pd.DataFrame({'id': [1, 2], 'span': pd.to_timedelta(['1000 h', None])})

    mage_mysql.export(short, None, 'short', verbose=False)
    mage_mysql.export(long, None, 'long', verbose=False)

    assert column_types(my, 'short')['span'] == 'time(6)'
    assert [r['span'] for r in stored(my, 'short', ['id', 'span']).values()] == [
        dt.timedelta(seconds=1), None, -dt.timedelta(hours=838),
    ]
    assert column_types(my, 'long')['span'] == 'bigint'
    assert [r['span'] for r in stored(my, 'long', ['id', 'span']).values()] == [
        3_600_000_000_000_000, None,
    ]


def test_upsert_on_a_text_key_with_reserved_column_names(mage_mysql, my):
    """
    A UNIQUE key on a TEXT column fails in MySQL, and the update clause did not quote
    column names, so reserved words such as order failed.
    """
    first = pd.DataFrame({'key': ['a', 'b'], 'order': [1, 2], 'note': ['x', 'y']})
    second = pd.DataFrame({'key': ['b', 'c'], 'order': [20, 3], 'note': [None, 'z']})
    options = dict(
        verbose=False, allow_reserved_words=True, unique_constraints=['key'],
        unique_conflict_method='UPDATE', if_exists='append',
    )

    mage_mysql.export(first, None, 'upserts', **options)
    mage_mysql.export(second, None, 'upserts', **options)

    with my.cursor() as cursor:
        cursor.execute('SELECT `key`, `order`, note FROM upserts ORDER BY `key`')
        assert cursor.fetchall() == [('a', 1, 'x'), ('b', 20, None), ('c', 3, 'z')]
    assert column_types(my, 'upserts')['key'] == 'varchar(255)'


def test_append_matches_columns_by_name(mage_mysql, my):
    with my.cursor() as cursor:
        cursor.execute('CREATE TABLE existing (a BIGINT, b VARCHAR(10), c DOUBLE)')
    my.commit()

    mage_mysql.export(
        pd.DataFrame({'c': [1.5], 'a': [7], 'b': ['x']}), None, 'existing',
        verbose=False, if_exists='append',
    )

    with my.cursor() as cursor:
        cursor.execute('SELECT a, b, c FROM existing')
        assert cursor.fetchall() == [(7, 'x', 1.5)]


def test_a_failed_replace_keeps_the_rows(mage_mysql, my):
    with my.cursor() as cursor:
        cursor.execute('CREATE TABLE kept (id BIGINT, code VARCHAR(3))')
        cursor.execute("INSERT INTO kept VALUES (1, 'abc')")
    my.commit()

    with pytest.raises(Exception, match='Data too long'):
        mage_mysql.export(
            pd.DataFrame({'id': [2], 'code': ['too long']}), None, 'kept', verbose=False,
        )

    with my.cursor() as cursor:
        cursor.execute('SELECT * FROM kept')
        assert cursor.fetchall() == [(1, 'abc')]


def test_polars_frames(mage_mysql, my):
    frame = pl.DataFrame({
        'id': [1, 2],
        'big': [2**63 - 1, None],
        'u': pl.Series([2**64 - 1, None], dtype=pl.UInt64),
        'f': [0.1, None],
        'day': [dt.date(2024, 2, 29), None],
        'tags': [['a', 'b'], None],
    })

    mage_mysql.export(frame, None, 'polars', verbose=False, allow_reserved_words=True)

    assert column_types(my, 'polars')['u'] == 'bigint unsigned'
    rows = stored(my, 'polars', frame.columns)
    assert rows[1] == {'id': 1, 'big': 2**63 - 1, 'u': 2**64 - 1, 'f': 0.1,
                       'day': dt.date(2024, 2, 29), 'tags': '["a", "b"]'}
    assert rows[2] == {'id': 2, 'big': None, 'u': None, 'f': None, 'day': None,
                       'tags': None}


def test_a_large_frame(mage_mysql, my):
    count = 200_000
    frame = pd.DataFrame({
        'id': np.arange(count),
        'text': [f'row {i} ñ' for i in range(count)],
        'f': np.arange(count) / 7,
    })

    mage_mysql.export(frame, None, 'large', verbose=False, allow_reserved_words=True)

    with my.cursor() as cursor:
        cursor.execute('SELECT COUNT(*), SUM(id), MAX(text) FROM large')
        assert cursor.fetchone() == (count, decimal.Decimal(count * (count - 1) // 2),
                                     'row 99999 ñ')


@pytest.mark.parametrize('table', ['order', 'with-dash', 'with space'])
def test_table_names_are_quoted(mage_mysql, my, table):
    """Table names were not quoted, so reserved words and names with a dash failed."""
    frame = pd.DataFrame({'id': [1, 2]})

    mage_mysql.export(frame, None, table, verbose=False)
    mage_mysql.export(frame, None, table, verbose=False, if_exists='append')

    with my.cursor() as cursor:
        cursor.execute(f'SELECT COUNT(*) FROM `{table}`')
        assert cursor.fetchone() == (4,)
