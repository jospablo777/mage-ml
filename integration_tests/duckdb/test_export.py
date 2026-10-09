"""
Exporting frames to DuckDB with Mage's client.

DuckDB reads the frame through Arrow: new tables get the frame's types, rows are
inserted by column name, and each export is one transaction. Tables are compared with
the source in SQL, column by column.
"""
import datetime as dt
import decimal
import time
import uuid

import duckdb
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from integration_tests.data import duckdb_dataset

QUERY = 'SELECT * FROM src ORDER BY id'
LOADS = {'exact_types': dict(exact_types=True), 'polars': dict(polars=True)}

# Types of the tables an export creates from each load. Polars has no seconds unit, so
# TIMESTAMP_S arrives as milliseconds, and no UUID type.
CREATED = {
    'exact_types': {
        **duckdb_dataset.COLUMNS,
        'c_timestamptz': 'TIMESTAMP WITH TIME ZONE',
        'c_hugeint': 'DECIMAL(38,0)',
        'c_uuid': 'VARCHAR',
        'c_json': 'VARCHAR',
        'c_enum': 'VARCHAR',
    },
    'polars': {
        **duckdb_dataset.COLUMNS,
        'c_timestamptz': 'TIMESTAMP WITH TIME ZONE',
        'c_timestamp_s': 'TIMESTAMP_MS',
        'c_time': 'TIME_NS',
        'c_uuid': 'VARCHAR',
        'c_json': 'VARCHAR',
        'c_enum': 'VARCHAR',
    },
}


def column_types(client, table):
    return dict(client.conn.execute(
        'SELECT column_name, data_type FROM information_schema.columns '
        'WHERE table_name = ? ORDER BY ordinal_position', [table],
    ).fetchall())


def rows(client, query):
    return client.conn.execute(query).fetchall()


def export(client, frame, table='dst', **kwargs):
    kwargs.setdefault('if_exists', 'append')
    kwargs.setdefault('allow_reserved_words', True)
    client.export(frame, table_name=table, verbose=False, **kwargs)


# Round trips ------------------------------------------------------------------------------

@pytest.mark.parametrize('target', ['new', 'existing'])
@pytest.mark.parametrize('mode', sorted(LOADS))
def test_round_trip(duckdb_client, mode, target):
    frame = duckdb_client.load(QUERY, verbose=False, **LOADS[mode])
    if target == 'existing':
        duckdb_client.conn.execute('CREATE TABLE dst AS SELECT * FROM src LIMIT 0')

    export(duckdb_client, frame)

    assert duckdb_dataset.mismatches(duckdb_client.conn, 'src', 'dst') == {}


@pytest.mark.parametrize('mode', sorted(LOADS))
def test_created_types(duckdb_client, mode):
    export(duckdb_client, duckdb_client.load(QUERY, verbose=False, **LOADS[mode]))

    assert column_types(duckdb_client, 'dst') == CREATED[mode]


def test_default_load_round_trip_losses(duckdb_client):
    """
    The default load changes these values before the export sees them: integers with
    NULL and decimals become float, NaN becomes the missing marker, nanoseconds are
    rounded, and structs, maps and lists of structs become dicts, stored as JSON. Row 2
    holds the largest BIGINT, which as a float no longer fits.
    """
    frame = duckdb_client.load('SELECT * FROM src WHERE id <> 2 ORDER BY id', verbose=False)

    export(duckdb_client, frame)

    problems = duckdb_dataset.mismatches(duckdb_client.conn, 'src', 'dst', where='id <> 2')
    assert set(problems) == {
        'c_bigint', 'c_decimal', 'c_decimal_wide', 'c_float', 'c_double', 'c_timestamp_ns',
        'c_struct', 'c_map', 'c_nested',
    }


def test_upsert_twice_changes_nothing(duckdb_client):
    frame = duckdb_client.load(QUERY, verbose=False, polars=True)
    duckdb_client.conn.execute('CREATE TABLE dst AS SELECT * FROM src LIMIT 0')
    duckdb_client.conn.execute('ALTER TABLE dst ADD PRIMARY KEY (id)')

    for _ in range(2):
        export(duckdb_client, frame, unique_constraints=['id'], unique_conflict_method='UPDATE')

    assert duckdb_dataset.mismatches(duckdb_client.conn, 'src', 'dst') == {}


# Write policies ---------------------------------------------------------------------------

def test_replace_deletes_rows_and_keeps_the_table(duckdb_client):
    duckdb_client.conn.execute('CREATE TABLE dst (id BIGINT, extra VARCHAR DEFAULT \'d\')')
    duckdb_client.conn.execute("INSERT INTO dst VALUES (100, 'old')")

    export(duckdb_client, pd.DataFrame({'id': [1, 2]}), if_exists='replace')

    assert rows(duckdb_client, 'SELECT * FROM dst ORDER BY id') == [(1, 'd'), (2, 'd')]


def test_replace_with_drop_recreates_the_table(duckdb_client):
    duckdb_client.conn.execute('CREATE TABLE dst (id VARCHAR)')

    export(duckdb_client, pd.DataFrame({'id': [1]}), if_exists='replace',
           drop_table_on_replace=True)

    assert column_types(duckdb_client, 'dst') == {'id': 'BIGINT'}


def test_fail_policy(duckdb_client):
    export(duckdb_client, pd.DataFrame({'id': [1]}))

    with pytest.raises(ValueError, match='already exists'):
        export(duckdb_client, pd.DataFrame({'id': [2]}), if_exists='fail')
    assert rows(duckdb_client, 'SELECT id FROM dst') == [(1,)]


def test_new_schema(duckdb_client):
    export(duckdb_client, pd.DataFrame({'id': [1]}), table='t', schema_name='analytics')

    assert rows(duckdb_client, 'SELECT id FROM analytics.t') == [(1,)]


# Conflicts --------------------------------------------------------------------------------

def keyed_table(client, unique='UNIQUE (k)'):
    client.conn.execute(f'CREATE TABLE dst (k BIGINT, v VARCHAR, {unique})')
    client.conn.execute("INSERT INTO dst VALUES (1, 'old'), (2, 'keep')")


@pytest.mark.parametrize('engine', ['pandas', 'polars'])
def test_update_overwrites_and_inserts(duckdb_client, engine):
    keyed_table(duckdb_client)
    data = {'k': [1, 3], 'v': ['new', 'added']}
    frame = pd.DataFrame(data) if engine == 'pandas' else pl.DataFrame(data)

    export(duckdb_client, frame, unique_constraints=['k'], unique_conflict_method='update')

    assert rows(duckdb_client, 'SELECT * FROM dst ORDER BY k') == [
        (1, 'new'), (2, 'keep'), (3, 'added'),
    ]


def test_ignore_keeps_stored_rows(duckdb_client):
    keyed_table(duckdb_client)

    export(duckdb_client, pd.DataFrame({'k': [1, 3], 'v': ['new', 'added']}),
           unique_constraints=['k'], unique_conflict_method='IGNORE')

    assert rows(duckdb_client, 'SELECT * FROM dst ORDER BY k') == [
        (1, 'old'), (2, 'keep'), (3, 'added'),
    ]


def test_update_with_duplicate_keys_raises_and_writes_nothing(duckdb_client):
    """DuckDB would keep the first of the duplicates without an error."""
    keyed_table(duckdb_client)

    with pytest.raises(ValueError, match='more than once'):
        export(duckdb_client, pd.DataFrame({'k': [5, 5], 'v': ['a', 'b']}),
               unique_constraints=['k'], unique_conflict_method='UPDATE')
    assert rows(duckdb_client, 'SELECT count(*) FROM dst') == [(2,)]


def test_ignore_with_duplicate_keys_keeps_the_first(duckdb_client):
    keyed_table(duckdb_client)

    export(duckdb_client, pd.DataFrame({'k': [5, 5], 'v': ['first', 'second']}),
           unique_constraints=['k'], unique_conflict_method='IGNORE')

    assert rows(duckdb_client, 'SELECT v FROM dst WHERE k = 5') == [('first',)]


def test_null_keys_are_not_duplicates(duckdb_client):
    """DuckDB's ON CONFLICT DO UPDATE kept one of several rows with a NULL key."""
    keyed_table(duckdb_client)

    export(duckdb_client, pd.DataFrame({'k': pd.array([None, None], dtype='Int64'),
                                        'v': ['a', 'b']}),
           unique_constraints=['k'], unique_conflict_method='UPDATE')

    assert rows(duckdb_client, 'SELECT count(*) FROM dst WHERE k IS NULL') == [(2,)]


def test_composite_keys(duckdb_client):
    duckdb_client.conn.execute('CREATE TABLE dst (a BIGINT, b VARCHAR, v BIGINT, UNIQUE (a, b))')
    duckdb_client.conn.execute("INSERT INTO dst VALUES (1, 'x', 0)")

    export(duckdb_client, pl.DataFrame({'a': [1, 1], 'b': ['x', 'y'], 'v': [5, 6]}),
           unique_constraints=['a', 'b'], unique_conflict_method='UPDATE')

    assert rows(duckdb_client, 'SELECT * FROM dst ORDER BY b') == [(1, 'x', 5), (1, 'y', 6)]


def test_new_tables_get_the_unique_constraint(duckdb_client):
    export(duckdb_client, pd.DataFrame({'k': [1], 'v': ['a']}), unique_constraints=['k'],
           unique_conflict_method='UPDATE')
    export(duckdb_client, pd.DataFrame({'k': [1], 'v': ['b']}), unique_constraints=['k'],
           unique_conflict_method='UPDATE')

    assert rows(duckdb_client, 'SELECT * FROM dst') == [(1, 'b')]


def test_unknown_conflict_method(duckdb_client):
    with pytest.raises(ValueError, match='unique_conflict_method'):
        export(duckdb_client, pd.DataFrame({'k': [1]}), unique_constraints=['k'],
               unique_conflict_method='MERGE')


def test_a_failed_export_leaves_the_table_unchanged(duckdb_client):
    """The export runs in one transaction; the DELETE of replace is undone too."""
    keyed_table(duckdb_client, unique='PRIMARY KEY (k)')

    with pytest.raises(duckdb.ConstraintException):
        export(duckdb_client, pd.DataFrame({'k': [7, 1], 'v': ['x', 'dup']}))
    with pytest.raises(duckdb.ConversionException):
        export(duckdb_client, pd.DataFrame({'k': ['not a number'], 'v': ['x']}),
               if_exists='replace')

    assert rows(duckdb_client, 'SELECT * FROM dst ORDER BY k') == [(1, 'old'), (2, 'keep')]


# Names ------------------------------------------------------------------------------------

def test_reserved_names_are_prefixed_in_new_tables(duckdb_client):
    duckdb_client.export(pd.DataFrame({'name': ['a'], 'date': ['b'], 'id': [1]}),
                         table_name='dst', verbose=False)

    assert list(column_types(duckdb_client, 'dst')) == ['_name', '_date', 'id']


def test_reserved_names_match_plain_columns_of_existing_tables(duckdb_client):
    duckdb_client.conn.execute('CREATE TABLE dst (id BIGINT, name VARCHAR, "Status" VARCHAR)')

    duckdb_client.export(pd.DataFrame({'Status': ['ok'], 'name': ['a'], 'id': [1]}),
                         table_name='dst', if_exists='append', verbose=False)

    assert rows(duckdb_client, 'SELECT id, name, "Status" FROM dst') == [(1, 'a', 'ok')]


def test_columns_are_matched_by_name_not_position(duckdb_client):
    duckdb_client.conn.execute('CREATE TABLE dst (a BIGINT, b VARCHAR, c DOUBLE)')

    export(duckdb_client, pd.DataFrame({'c': [1.5], 'b': ['x'], 'a': [7]}))

    assert rows(duckdb_client, 'SELECT * FROM dst') == [(7, 'x', 1.5)]


def test_table_columns_missing_from_the_frame_get_their_default(duckdb_client):
    duckdb_client.conn.execute("CREATE TABLE dst (a BIGINT, note VARCHAR DEFAULT 'n')")

    export(duckdb_client, pd.DataFrame({'a': [1]}))

    assert rows(duckdb_client, 'SELECT * FROM dst') == [(1, 'n')]


def test_a_frame_column_missing_from_the_table_is_named(duckdb_client):
    duckdb_client.conn.execute('CREATE TABLE dst (a BIGINT)')

    with pytest.raises(ValueError, match="'unexpected'"):
        export(duckdb_client, pd.DataFrame({'a': [1], 'unexpected': [2]}))


def test_names_that_clean_to_the_same_name_raise(duckdb_client):
    with pytest.raises(ValueError, match='same name'):
        duckdb_client.export(pd.DataFrame({'Total Sales': [1], 'total_sales': [2]}),
                             table_name='dst', verbose=False)


def test_unicode_and_case(duckdb_client):
    frame = pd.DataFrame({'Ñandú Count': [1], 'MixedCase': [2]})

    duckdb_client.export(frame, table_name='dst', verbose=False, case_sensitive=True)

    assert list(column_types(duckdb_client, 'dst')) == ['ñandú_count', 'MixedCase'] or \
        list(column_types(duckdb_client, 'dst')) == ['Ñandú_Count', 'MixedCase']


# Inputs and values ------------------------------------------------------------------------

def test_dict_list_lazy_and_arrow_inputs(duckdb_client):
    export(duckdb_client, {'a': 1, 'b': 'x'})
    export(duckdb_client, [{'a': 2, 'b': 'y'}])
    export(duckdb_client, pl.LazyFrame({'a': [3], 'b': ['z']}))
    export(duckdb_client, pa.table({'a': [4], 'b': ['w']}))

    assert rows(duckdb_client, 'SELECT * FROM dst ORDER BY a') == [
        (1, 'x'), (2, 'y'), (3, 'z'), (4, 'w'),
    ]


def test_index_is_exported_when_asked(duckdb_client):
    frame = pd.DataFrame({'v': [1, 2]}, index=pd.Index(['a', 'b'], name='key'))

    export(duckdb_client, frame, index=True)

    assert rows(duckdb_client, 'SELECT * FROM dst ORDER BY key') == [('a', 1), ('b', 2)]


def test_empty_frame_creates_a_typed_table(duckdb_client):
    frame = pd.DataFrame({'a': pd.Series([], dtype='int16'), 'b': pd.Series([], dtype='str')})

    export(duckdb_client, frame)

    assert column_types(duckdb_client, 'dst') == {'a': 'SMALLINT', 'b': 'VARCHAR'}


def test_numpy_backed_pandas_frames(duckdb_client):
    """NaN is pandas' missing marker in float64 columns and becomes NULL."""
    frame = pd.DataFrame({
        'i': np.array([1, 2], dtype='int32'),
        'f': [0.1234567, np.nan],
        'b': [True, False],
        's': pd.Series(['ñ', None], dtype='str'),
        'tz': pd.to_datetime(['2024-01-01 12:00', None]).tz_localize('America/New_York'),
        'td': pd.to_timedelta(['1 days 00:00:01', None]),
        'cat': pd.Categorical(['x', None]),
    })

    export(duckdb_client, frame)

    assert column_types(duckdb_client, 'dst') == dict(
        i='INTEGER', f='DOUBLE', b='BOOLEAN', s='VARCHAR', tz='TIMESTAMP WITH TIME ZONE',
        td='INTERVAL', cat='VARCHAR',
    )
    assert rows(duckdb_client, "SELECT i, f, b, s, tz AT TIME ZONE 'UTC', td, cat FROM dst") == [
        (1, 0.1234567, True, 'ñ', dt.datetime(2024, 1, 1, 17), dt.timedelta(days=1, seconds=1),
         'x'),
        (2, None, False, None, None, None, None),
    ]


def test_object_columns(duckdb_client):
    frame = pd.DataFrame({
        'doc': pd.Series([{'a': [1, {'b': 'ñ'}]}, None], dtype=object),
        'nested': pd.Series([[{'x': 1}], None], dtype=object),
        'items': pd.Series([[1, 2], None], dtype=object),
        'key': pd.Series([uuid.UUID(int=7), None], dtype=object),
        'amount': pd.Series([decimal.Decimal('12345678901234.5678'), None], dtype=object),
        'raw': pd.Series([b'\x00\xff', None], dtype=object),
        'day': pd.Series([dt.date(2024, 2, 29), None], dtype=object),
        'mixed': pd.Series([1, 'a'], dtype=object),
    })

    export(duckdb_client, frame)

    assert column_types(duckdb_client, 'dst') == dict(
        doc='JSON', nested='JSON', items='BIGINT[]', key='UUID', amount='DECIMAL(18,4)',
        raw='BLOB', day='DATE', mixed='VARCHAR',
    )
    assert rows(duckdb_client, 'SELECT doc, nested, items, key, amount, raw, day, mixed '
                               'FROM dst ORDER BY mixed') == [
        ('{"a": [1, {"b": "ñ"}]}', '[{"x": 1}]', [1, 2], uuid.UUID(int=7),
         decimal.Decimal('12345678901234.5678'), b'\x00\xff', dt.date(2024, 2, 29), '1'),
        (None, None, None, None, None, None, None, 'a'),
    ]


def test_the_caller_frame_is_not_changed(duckdb_client):
    frame = pd.DataFrame({'Total Sales': [1.5], 'name': ['a']})
    original = frame.copy()

    duckdb_client.export(frame, table_name='dst', verbose=False)

    pd.testing.assert_frame_equal(frame, original)


def test_overwrite_types(duckdb_client):
    duckdb_client.export(pd.DataFrame({'n': [1], 'day_text': ['2024-01-01']}),
                         table_name='dst', verbose=False, overwrite_types={'day_text': 'DATE'})

    assert column_types(duckdb_client, 'dst') == {'n': 'BIGINT', 'day_text': 'DATE'}


def test_polars_object_columns_are_rejected(duckdb_client):
    frame = pl.DataFrame({'o': pl.Series([object()], dtype=pl.Object)})

    with pytest.raises(ValueError, match='Object'):
        export(duckdb_client, frame)


def test_a_million_rows(duckdb_client):
    frame = pl.DataFrame({
        'id': range(1_000_000),
        'value': [i * 0.5 for i in range(1_000_000)],
        'label': [f'row {i}' for i in range(1_000_000)],
    })

    started = time.perf_counter()
    export(duckdb_client, frame)
    elapsed = time.perf_counter() - started

    assert rows(duckdb_client, 'SELECT count(*), sum(id), max(label) FROM dst') == [
        (1_000_000, 499_999_500_000, 'row 999999'),
    ]
    assert elapsed < 10, elapsed
