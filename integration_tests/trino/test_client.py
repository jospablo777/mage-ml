"""
Mage's Trino client (mage_ai.io.trino) against Trino 483, in each catalog: memory, Iceberg
with tables in MinIO, and Delta Lake.
"""
import datetime as dt
import decimal
import uuid

import numpy as np
import pandas as pd
import polars as pl
import pytest

KEY = uuid.UUID('12345678-1234-5678-1234-567812345678')


def column_types(tr, schema, table):
    return dict(tr(
        'SELECT column_name, data_type FROM information_schema.columns '
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
        'ORDER BY ordinal_position'
    ))


def every_type_frame():
    return pd.DataFrame({
        'id': [1, 2],
        'tiny': pd.Series([-128, 127], dtype='int8'),
        'small': pd.Series([-(2**15), 2**15 - 1], dtype='int16'),
        'i32': pd.Series([-(2**31), 2**31 - 1], dtype='int32'),
        'big': pd.array([2**53 + 1, None], dtype='Int64'),
        'u8': pd.Series([255, 0], dtype='uint8'),
        'u32': pd.Series([2**32 - 1, 0], dtype='uint32'),
        'u64': pd.Series([2**64 - 1, 0], dtype='uint64'),
        'f32': pd.Series([1.5, -0.25], dtype='float32'),
        'f': [float('inf'), np.nan],
        'flag': pd.array([True, None], dtype='boolean'),
        'text': pd.Series(['ñ "q" \'s\' \\ 😀', None], dtype='str'),
        'day': [dt.date(1, 1, 1), None],
        'at': pd.to_datetime(['2024-01-01 12:00:00.123456', None]),
        'zoned': pd.to_datetime(['2024-07-01 12:00:00.000001', None]).tz_localize(
            'America/New_York',
        ),
        'amount': [decimal.Decimal('12345678901234567890.123456789'), None],
        'huge': pd.Series([2**100, None], dtype=object),
        'key': [KEY, None],
        'raw': [b'\x00\xff', None],
        'clock': [dt.time(23, 59, 59, 999999), None],
        'span': pd.to_timedelta(['1.5 s', None]),
        'doc': [{'a': [1, None], 'b': 'x'}, None],
        'tags': [['a', 'b'], None],
        'cat': pd.Categorical(['x', None]),
    })


TYPES = {
    'id': 'bigint', 'tiny': 'tinyint', 'small': 'smallint', 'i32': 'integer',
    'big': 'bigint', 'u8': 'smallint', 'u32': 'bigint', 'u64': 'decimal(20,0)',
    'f32': 'real', 'f': 'double', 'flag': 'boolean', 'text': 'varchar', 'day': 'date',
    'at': 'timestamp(6)', 'zoned': 'timestamp(6) with time zone',
    'amount': 'decimal(29,9)', 'huge': 'decimal(31,0)', 'key': 'uuid', 'raw': 'varbinary',
    'clock': 'time(6)', 'span': 'bigint', 'doc': 'varchar', 'tags': 'varchar',
    'cat': 'varchar',
}
# Iceberg stores 8- and 16-bit integers as integer. The Delta Lake connector has no uuid or
# time type and stores zoned timestamps in milliseconds.
CATALOG_TYPES = {
    'memory': {},
    'iceberg': {'tiny': 'integer', 'small': 'integer', 'u8': 'integer'},
    'delta': {
        'key': 'varchar', 'clock': 'varchar', 'zoned': 'timestamp(3) with time zone',
    },
}


def test_column_types_hold_every_value(mage_trino, tr, trino_catalog, trino_schema):
    """
    Columns were BIGINT, DOUBLE, BOOLEAN, TIMESTAMP(3) or VARCHAR: microseconds were lost,
    uint64 values failed, and Iceberg and Delta Lake rejected dates, decimals and UUIDs.
    """
    mage_trino.export(every_type_frame(), trino_schema, 't', verbose=False)

    assert column_types(tr, trino_schema, 't') == {**TYPES, **CATALOG_TYPES[trino_catalog]}
    first, second = tr('SELECT * FROM t ORDER BY id')
    delta = trino_catalog == 'delta'
    assert first == [
        1, -128, -(2**15), -(2**31), 2**53 + 1, 255, 2**32 - 1,
        decimal.Decimal(2**64 - 1), 1.5, float('inf'), True, 'ñ "q" \'s\' \\ 😀',
        dt.date(1, 1, 1), dt.datetime(2024, 1, 1, 12, 0, 0, 123456),
        dt.datetime(2024, 7, 1, 16, 0, 0, 0 if delta else 1, tzinfo=dt.timezone.utc)
        if trino_catalog != 'memory' else first[14],
        decimal.Decimal('12345678901234567890.123456789'), decimal.Decimal(2**100),
        str(KEY) if delta else KEY, b'\x00\xff',
        '23:59:59.999999' if delta else dt.time(23, 59, 59, 999999),
        # Durations are stored in microseconds.
        1_500_000,
        '{"a": [1, null], "b": "x"}', '["a", "b"]', 'x',
    ]
    if trino_catalog == 'memory':
        # The memory connector keeps the time zone offset.
        assert first[14] == dt.datetime(
            2024, 7, 1, 12, 0, 0, 1, tzinfo=dt.timezone(dt.timedelta(hours=-4)),
        )
    assert second[0] == 2 and second[4] is None and second[9] is None
    assert all(value is None for value in second[10:])


def test_special_values(mage_trino, tr, trino_schema):
    frame = pd.DataFrame({
        'id': [1, 2, 3],
        'n': pd.Series([-(2**63), 2**63 - 1, 0], dtype='int64'),
        'f': [float('-inf'), -0.0, 1e300],
        'text': pd.Series(['', 'nul\x00 tab\t nl\n', "x" * 100_000], dtype='str'),
        'at': pd.to_datetime(['1677-09-22', '2262-04-11 23:47:16.854775', None], format='ISO8601'),
        'empty': pd.Series([None, None, None], dtype=object),
    })

    mage_trino.export(frame, trino_schema, 't', verbose=False)

    rows = tr('SELECT n, f, text, CAST(at AS varchar), empty FROM t ORDER BY id')
    assert [row[:2] for row in rows] == [[-(2**63), float('-inf')], [2**63 - 1, -0.0],
                                         [0, 1e300]]
    # Empty text is not NULL.
    assert [row[2] for row in rows] == ['', 'nul\x00 tab\t nl\n', 'x' * 100_000]
    assert [row[3] for row in rows] == [
        '1677-09-22 00:00:00.000000', '2262-04-11 23:47:16.854775', None,
    ]
    assert [row[4] for row in rows] == [None, None, None]


def test_nanoseconds_round_to_microseconds(mage_trino, tr, trino_schema):
    frame = pd.DataFrame({'at': pd.to_datetime(['2024-01-01 00:00:00.000000500'])})

    mage_trino.export(frame, trino_schema, 't', verbose=False)

    assert tr('SELECT CAST(at AS varchar) FROM t') == [['2024-01-01 00:00:00.000001']]


def test_write_policies(mage_trino, tr, trino_schema):
    """A replace into the memory connector failed: it cannot DELETE rows."""
    mage_trino.export(pd.DataFrame({'id': [1, 2]}), trino_schema, 't', verbose=False)

    mage_trino.export(pd.DataFrame({'id': [3]}), trino_schema, 't', verbose=False)
    assert tr('SELECT id FROM t') == [[3]]

    mage_trino.export(pd.DataFrame({'id': [4]}), trino_schema, 't', if_exists='append',
                      verbose=False)
    assert tr('SELECT id FROM t ORDER BY id') == [[3], [4]]

    with pytest.raises(ValueError, match='already exists'):
        mage_trino.export(pd.DataFrame({'id': [5]}), trino_schema, 't', if_exists='fail',
                          verbose=False)

    mage_trino.export(pd.DataFrame({'name': ['a']}), trino_schema, 't',
                      drop_table_on_replace=True, verbose=False)
    assert tr('SELECT * FROM t') == [['a']]


def test_appends_match_columns_by_name(mage_trino, tr, trino_schema):
    """Rows were inserted by position, so a frame in another column order mixed values."""
    tr('CREATE TABLE t (id bigint, label varchar, day varchar, amount double)')
    frame = pd.DataFrame({
        'amount': [decimal.Decimal('1.25')],
        'day': [dt.date(2024, 1, 31)],
        'Label': ['x'],
        'id': [7],
    })

    mage_trino.export(frame, trino_schema, 't', if_exists='append', verbose=False)

    # Values are written as the table's column types.
    assert tr('SELECT * FROM t') == [[7, 'x', '2024-01-31', 1.25]]


def test_rows_go_in_few_statements(mage_trino, tr, trino_schema, monkeypatch):
    """Rows were inserted one statement each: an Iceberg commit per row."""
    from mage_ai.io import trino_types

    statements = []
    insert_statements = trino_types.insert_statements

    def counted(*args, **kwargs):
        for statement in insert_statements(*args, **kwargs):
            statements.append(len(statement))
            yield statement

    monkeypatch.setattr(trino_types, 'insert_statements', counted)
    rows = 20_000
    frame = pd.DataFrame({
        'id': range(rows),
        'text': [f'row {i} ñ' for i in range(rows)],
        'at': pd.date_range('2024-01-01', periods=rows, freq='s'),
    })

    mage_trino.export(frame, trino_schema, 't', verbose=False)

    assert len(statements) <= 5
    assert max(statements) <= mage_trino.QUERY_MAX_LENGTH
    assert tr('SELECT count(*), sum(id), count(DISTINCT text) FROM t') == [
        [rows, rows * (rows - 1) // 2, rows],
    ]


def test_query_string_into_an_existing_table(mage_trino, tr, trino_schema):
    """A replace without dropping the table ran CREATE TABLE AS, which failed."""
    for _ in range(2):
        mage_trino.export(None, trino_schema, 't', query_string='SELECT 1 AS x',
                          verbose=False)
    assert tr('SELECT x FROM t') == [[1]]

    mage_trino.export(None, trino_schema, 't', query_string='SELECT 2 AS x',
                      if_exists='append', verbose=False)
    assert tr('SELECT x FROM t ORDER BY x') == [[1], [2]]


def test_table_exists_matches_exact_names(mage_trino, tr, trino_schema):
    """SHOW TABLES LIKE 'axb' matched a table named a_b."""
    tr('CREATE TABLE a_b (x bigint)')

    assert mage_trino.table_exists(trino_schema, 'a_b')
    assert not mage_trino.table_exists(trino_schema, 'axb')
    assert not mage_trino.table_exists(trino_schema.replace('_', 'x'), 'a_b')


def test_column_names(mage_trino, tr, trino_schema):
    frame = pd.DataFrame({'Order ID': [1], 'select': ['s'], 'a-b': [2.5]})

    mage_trino.export(frame, trino_schema, 't', verbose=False)

    assert list(column_types(tr, trino_schema, 't')) == ['order_id', 'select', 'a_b']
    assert tr('SELECT * FROM t') == [[1, 's', 2.5]]


def test_load_modes(mage_trino, tr, trino_catalog, trino_schema):
    zoned = 'timestamp(3) with time zone' if trino_catalog == 'delta' \
        else 'timestamp(6) with time zone'
    tr(f'CREATE TABLE t (id bigint, big bigint, amount decimal(38, 9), at timestamp(6), '
       f'zoned {zoned}, name varchar)')
    tr("INSERT INTO t VALUES (1, 9223372036854775807, DECIMAL '12345678901234567890.123456789', "
       "TIMESTAMP '2024-01-01 00:00:00.000001', TIMESTAMP '2024-07-01 12:00:00 -04:00', 'a'), "
       '(2, NULL, NULL, NULL, NULL, NULL)')
    query = 'SELECT * FROM t ORDER BY id'

    plain = mage_trino.load(query, verbose=False)
    # read_sql, as before: an integer column with a NULL becomes float.
    assert plain['big'].dtype == 'float64'

    nullable = mage_trino.load(query, verbose=False, nullable_integers=True)
    assert str(nullable['big'].dtype) == 'Int64'
    assert nullable['big'].tolist() == [2**63 - 1, pd.NA]

    exact = mage_trino.load(query, verbose=False, exact_types=True)
    assert exact['big'].tolist() == [2**63 - 1, None] or exact['big'][0] == 2**63 - 1
    assert str(exact['amount'].dtype) == 'decimal128(38, 9)[pyarrow]'
    assert exact['amount'][0] == decimal.Decimal('12345678901234567890.123456789')
    assert exact['at'][0] == pd.Timestamp('2024-01-01 00:00:00.000001')
    assert exact['zoned'][0] == pd.Timestamp('2024-07-01 16:00:00', tz='UTC')

    frame = mage_trino.load(query, verbose=False, polars=True)
    assert frame.schema['big'] == pl.Int64
    assert frame.schema['amount'] == pl.Decimal(38, 9)
    assert frame['at'][0] == dt.datetime(2024, 1, 1, 0, 0, 0, 1)


def test_loads_keep_the_order_and_the_limit(mage_trino, tr):
    """
    Loads ran SELECT * FROM (query) LIMIT n, and Trino drops an ORDER BY in a subquery,
    so the rows came in any order.
    """
    tr('CREATE TABLE t (id bigint)')
    for start in range(0, 60, 10):
        # Each INSERT writes its own file in Iceberg and Delta Lake.
        tr(f'INSERT INTO t SELECT * FROM UNNEST(sequence({start}, {start + 9}))')
    query = 'SELECT id FROM t ORDER BY id DESC'
    expected = list(range(59, 49, -1))

    for options in [{}, dict(nullable_integers=True), dict(exact_types=True)]:
        frame = mage_trino.load(query, limit=10, verbose=False, **options)
        assert frame['id'].tolist() == expected, options
    assert mage_trino.load(query, limit=10, verbose=False, polars=True)['id'].to_list() == \
        expected


def test_failed_queries_raise(mage_trino):
    """A failed query was printed, run twice more, and load returned None."""
    from trino.exceptions import TrinoUserError

    with pytest.raises(TrinoUserError, match='SYNTAX_ERROR'):
        mage_trino.load('SELEC 1', verbose=False)
    with pytest.raises(TrinoUserError, match='TABLE_NOT_FOUND'):
        mage_trino.execute('SELECT * FROM missing_table')
