"""
Mage's ClickHouse client (mage_ai.io.clickhouse) against ClickHouse 25.8.
"""
import datetime as dt
import decimal
import uuid

import pandas as pd
import polars as pl
import pytest


def column_types(ch, table):
    return dict(ch.query(
        'SELECT name, type FROM system.columns WHERE database = currentDatabase() '
        'AND table = {table:String} ORDER BY position',
        parameters=dict(table=table),
    ).result_rows)


def rows(ch, table):
    return ch.query(f'SELECT * FROM `{table}` ORDER BY id').result_rows


def test_column_types_hold_every_value(mage_clickhouse, ch):
    """
    Nullable Int64, int32 and uint64 columns became String, uint64 values failed to
    insert, dates, decimals and UUIDs failed, and datetimes lost their microseconds.
    """
    key = uuid.UUID('12345678-1234-5678-1234-567812345678')
    frame = pd.DataFrame({
        'id': [1, 2],
        'big': pd.array([2**53 + 1, None], dtype='Int64'),
        'i32': pd.Series([-(2**31), 2**31 - 1], dtype='int32'),
        'u64': pd.Series([2**64 - 1, 0], dtype='uint64'),
        'f32': pd.Series([1.5, -0.25], dtype='float32'),
        'f': [1.5, None],
        'flag': pd.array([True, None], dtype='boolean'),
        'text': pd.Series(['ñ "q" \'s\'', None], dtype='str'),
        'day': [dt.date(1900, 1, 1), None],
        'at': pd.to_datetime(['2024-01-01 12:00:00.123456', None]),
        'zoned': pd.to_datetime(['2024-07-01 12:00:00.000001', None]).tz_localize(
            'America/New_York',
        ),
        'amount': [decimal.Decimal('12345678901234567890.123456789'), None],
        'huge': pd.Series([2**100, None], dtype=object),
        'key': [key, None],
        'span': pd.to_timedelta(['1.5 s', None]),
        'doc': [{'a': [1, None], 'b': 'x'}, None],
        'tags': [['a', 'b'], None],
    })

    mage_clickhouse.export(frame, 't', if_exists='replace', verbose=False)

    assert column_types(ch, 't') == {
        'id': 'Nullable(Int64)', 'big': 'Nullable(Int64)', 'i32': 'Nullable(Int32)',
        'u64': 'Nullable(UInt64)', 'f32': 'Nullable(Float32)', 'f': 'Nullable(Float64)',
        'flag': 'Nullable(Bool)', 'text': 'Nullable(String)', 'day': 'Nullable(Date32)',
        'at': 'Nullable(DateTime64(6))', 'zoned': "Nullable(DateTime64(6, 'UTC'))",
        'amount': 'Nullable(Decimal(29, 9))', 'huge': 'Nullable(Int128)',
        'key': 'Nullable(UUID)', 'span': 'Nullable(Int64)', 'doc': 'Nullable(String)',
        'tags': 'Nullable(String)',
    }
    first, second = rows(ch, 't')
    assert first == (
        1, 2**53 + 1, -(2**31), 2**64 - 1, 1.5, 1.5, True, 'ñ "q" \'s\'',
        dt.date(1900, 1, 1), dt.datetime(2024, 1, 1, 12, 0, 0, 123456),
        # The instant in UTC, which clickhouse-connect returns naive.
        dt.datetime(2024, 7, 1, 16, 0, 0, 1),
        decimal.Decimal('12345678901234567890.123456789'), 2**100, key,
        # ClickHouse has no duration type; durations are stored in microseconds.
        1_500_000,
        '{"a": [1, null], "b": "x"}', '["a", "b"]',
    )
    assert second[:2] == (2, None) and set(second[2:]) <= {None, 2**31 - 1, 0, -0.25}


def test_tables_keep_their_data_on_disk(mage_clickhouse, ch):
    """The Memory engine kept tables in RAM only, so a restart of ClickHouse emptied them."""
    mage_clickhouse.export(pd.DataFrame({'id': [1]}), 't', verbose=False)

    engine = ch.query(
        "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't'",
    ).result_rows[0][0]
    assert engine == 'MergeTree'


def test_column_names_with_spaces_and_keywords(mage_clickhouse, ch):
    frame = pd.DataFrame({'id': [1], 'my col': ['a'], 'select': [2], 'back`tick': [3]})

    mage_clickhouse.export(frame, 'odd table', verbose=False)

    assert list(column_types(ch, 'odd table')) == ['id', 'my col', 'select', 'back`tick']
    assert rows(ch, 'odd table') == [(1, 'a', 2, 3)]


def test_write_policies(mage_clickhouse, ch):
    first = pd.DataFrame({'id': [1, 2]})
    second = pd.DataFrame({'id': [3]})

    mage_clickhouse.export(first, 't', verbose=False)
    mage_clickhouse.export(second, 't', if_exists='append', verbose=False)
    assert rows(ch, 't') == [(1,), (2,), (3,)]

    mage_clickhouse.export(second, 't', if_exists='replace', verbose=False)
    assert rows(ch, 't') == [(3,)]

    with pytest.raises(ValueError, match='already exists'):
        mage_clickhouse.export(second, 't', if_exists='fail', verbose=False)


def test_appends_convert_values_for_the_existing_columns(mage_clickhouse, ch):
    ch.command(
        'CREATE TABLE t (id Int64, doc Nullable(String), span Nullable(Int64)) '
        'ENGINE = MergeTree ORDER BY id',
    )
    frame = pd.DataFrame({'id': [1], 'doc': [{'a': 1}], 'span': pd.to_timedelta(['2 ms'])})

    mage_clickhouse.export(frame, 't', if_exists='append', verbose=False)

    assert rows(ch, 't') == [(1, '{"a": 1}', 2000)]


def test_polars_frames(mage_clickhouse, ch):
    frame = pl.DataFrame({
        'id': [1, 2],
        'big': pl.Series([2**63 - 1, None], dtype=pl.Int64),
        'huge': pl.Series([2**100, None], dtype=pl.Int128),
        'amount': pl.Series([decimal.Decimal('1.10'), None], dtype=pl.Decimal(10, 2)),
        'at': pl.Series([dt.datetime(2024, 1, 1, 0, 0, 0, 1), None], dtype=pl.Datetime('us')),
        'day': pl.Series([dt.date(2024, 2, 29), None]),
    })

    mage_clickhouse.export(frame.lazy(), 't', verbose=False)

    assert rows(ch, 't') == [
        (1, 2**63 - 1, 2**100, decimal.Decimal('1.10'), dt.datetime(2024, 1, 1, 0, 0, 0, 1),
         dt.date(2024, 2, 29)),
        (2, None, None, None, None, None),
    ]


def test_load(mage_clickhouse, ch):
    ch.command(
        'CREATE TABLE t (id Int64, n Nullable(Int64), at DateTime64(6)) '
        'ENGINE = MergeTree ORDER BY id',
    )
    ch.command("INSERT INTO t VALUES (1, 9007199254740993, '2024-01-01 00:00:00.000001'), "
               "(2, NULL, '2024-01-02 00:00:00')")

    frame = mage_clickhouse.load(f'SELECT * FROM {ch.database}.t ORDER BY id', verbose=False)

    assert frame['n'].tolist() == [2**53 + 1, pd.NA]
    assert frame['at'][0] == pd.Timestamp('2024-01-01 00:00:00.000001')
