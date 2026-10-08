"""
Duplicate handling, batching and failure behavior of Mage's PostgreSQL export.

Every case runs with a pandas and a Polars frame.
"""

import pandas as pd
import polars as pl
import psycopg2
import pytest

from mage_ai.io import postgres as postgres_module

ENGINES = ['pandas', 'polars']


def frame(engine, data):
    if engine == 'polars':
        return pl.DataFrame(data)
    return pd.DataFrame(data)


def run(pg, statement):
    with pg.cursor() as cursor:
        cursor.execute(statement)
    pg.commit()


def rows(pg, schema, table='t', columns='id, val'):
    with pg.cursor() as cursor:
        cursor.execute(f'SELECT {columns} FROM {schema}.{table} ORDER BY {columns}')
        result = cursor.fetchall()
    pg.rollback()
    return result


@pytest.fixture
def keyed_table(pg, schema):
    run(pg, f'CREATE TABLE {schema}.t (id integer PRIMARY KEY, val text)')
    run(pg, f"INSERT INTO {schema}.t VALUES (1, 'old1'), (2, 'old2')")
    return 't'


NEW = {'id': [2, 3], 'val': ['new2', 'new3']}


def export(client, schema, data, engine, table='t', **kwargs):
    kwargs.setdefault('if_exists', 'append')
    client.export(
        frame(engine, data), schema_name=schema, table_name=table, verbose=False, **kwargs
    )


# --- conflict methods -------------------------------------------------------------------


@pytest.mark.parametrize('engine', ENGINES)
def test_existing_key_without_conflict_handling_raises_and_writes_nothing(
    mage_postgres,
    pg,
    schema,
    keyed_table,
    engine,
):
    with pytest.raises(psycopg2.errors.UniqueViolation):
        export(mage_postgres, schema, NEW, engine)

    assert rows(pg, schema) == [(1, 'old1'), (2, 'old2')]


@pytest.mark.parametrize('engine', ENGINES)
def test_unique_constraints_without_a_method_use_copy(
    mage_postgres,
    pg,
    schema,
    keyed_table,
    engine,
):
    # No method means no ON CONFLICT clause, so an existing key fails.
    with pytest.raises(psycopg2.errors.UniqueViolation):
        export(mage_postgres, schema, NEW, engine, unique_constraints=['id'])


@pytest.mark.parametrize('engine', ENGINES)
def test_a_method_without_unique_constraints_is_ignored(
    mage_postgres,
    pg,
    schema,
    keyed_table,
    engine,
):
    # The PostgreSQL streaming sink sends IGNORE with an empty constraint list by default.
    export(
        mage_postgres,
        schema,
        {'id': [3], 'val': ['new3']},
        engine,
        unique_conflict_method='IGNORE',
        unique_constraints=[],
    )

    assert rows(pg, schema) == [(1, 'old1'), (2, 'old2'), (3, 'new3')]


@pytest.mark.parametrize('method', ['UPDATE', 'update', ' Update '])
@pytest.mark.parametrize('engine', ENGINES)
def test_update_replaces_existing_rows_and_inserts_new_ones(
    mage_postgres,
    pg,
    schema,
    keyed_table,
    engine,
    method,
):
    export(
        mage_postgres, schema, NEW, engine, unique_conflict_method=method, unique_constraints=['id']
    )

    assert rows(pg, schema) == [(1, 'old1'), (2, 'new2'), (3, 'new3')]


@pytest.mark.parametrize('method', ['IGNORE', 'ignore'])
@pytest.mark.parametrize('engine', ENGINES)
def test_ignore_keeps_existing_rows_and_inserts_new_ones(
    mage_postgres,
    pg,
    schema,
    keyed_table,
    engine,
    method,
):
    export(
        mage_postgres, schema, NEW, engine, unique_conflict_method=method, unique_constraints=['id']
    )

    assert rows(pg, schema) == [(1, 'old1'), (2, 'old2'), (3, 'new3')]


@pytest.mark.parametrize('engine', ENGINES)
def test_an_unknown_method_raises_before_writing(mage_postgres, pg, schema, keyed_table, engine):
    with pytest.raises(ValueError, match='UPSERT'):
        export(
            mage_postgres,
            schema,
            NEW,
            engine,
            unique_conflict_method='UPSERT',
            unique_constraints=['id'],
        )

    assert rows(pg, schema) == [(1, 'old1'), (2, 'old2')]


# --- duplicate keys inside one export ------------------------------------------------------


DUPLICATES = {'id': [3, 4, 3, 5, 4], 'val': ['a', 'b', 'c', 'd', 'e']}


@pytest.mark.parametrize('engine', ENGINES)
def test_update_with_duplicate_keys_raises_and_writes_nothing(
    mage_postgres,
    pg,
    schema,
    keyed_table,
    engine,
):
    with pytest.raises(ValueError) as error:
        export(
            mage_postgres,
            schema,
            DUPLICATES,
            engine,
            unique_conflict_method='UPDATE',
            unique_constraints=['id'],
        )

    assert '4 rows' in str(error.value)
    assert '(3,)' in str(error.value) and '(4,)' in str(error.value)
    assert rows(pg, schema) == [(1, 'old1'), (2, 'old2')]


@pytest.mark.parametrize('page_size', [1, 2, 3, 1000])
@pytest.mark.parametrize('engine', ENGINES)
def test_ignore_with_duplicate_keys_keeps_the_first_at_any_page_size(
    mage_postgres,
    pg,
    schema,
    keyed_table,
    engine,
    page_size,
):
    export(
        mage_postgres,
        schema,
        DUPLICATES,
        engine,
        insert_page_size=page_size,
        unique_conflict_method='IGNORE',
        unique_constraints=['id'],
    )

    assert rows(pg, schema) == [(1, 'old1'), (2, 'old2'), (3, 'a'), (4, 'b'), (5, 'd')]


@pytest.mark.parametrize('engine', ENGINES)
def test_rows_with_a_null_key_never_count_as_duplicates(mage_postgres, pg, schema, engine):
    # PostgreSQL treats NULLs as distinct in a unique constraint.
    run(pg, f'CREATE TABLE {schema}.t (id integer UNIQUE, val text)')

    export(
        mage_postgres,
        schema,
        {'id': [None, None, 1], 'val': ['x', 'y', 'z']},
        engine,
        unique_conflict_method='UPDATE',
        unique_constraints=['id'],
    )

    with pg.cursor() as cursor:
        cursor.execute(f'SELECT count(*), count(id) FROM {schema}.t')
        assert cursor.fetchone() == (3, 1)


@pytest.mark.parametrize('engine', ENGINES)
def test_composite_keys(mage_postgres, pg, schema, engine):
    run(pg, f'CREATE TABLE {schema}.t (a integer, b text, val text, PRIMARY KEY (a, b))')
    run(pg, f"INSERT INTO {schema}.t VALUES (1, 'x', 'old')")

    export(
        mage_postgres,
        schema,
        {'a': [1, 1], 'b': ['x', 'y'], 'val': ['new', 'other']},
        engine,
        unique_conflict_method='UPDATE',
        unique_constraints=['a', 'b'],
    )
    assert rows(pg, schema, columns='a, b, val') == [(1, 'x', 'new'), (1, 'y', 'other')]

    with pytest.raises(ValueError, match="'a', 'b'"):
        export(
            mage_postgres,
            schema,
            {'a': [2, 2], 'b': ['z', 'z'], 'val': ['p', 'q']},
            engine,
            unique_conflict_method='UPDATE',
            unique_constraints=['a', 'b'],
        )


# --- unique indexes -----------------------------------------------------------------------


@pytest.mark.parametrize('engine', ENGINES)
def test_missing_unique_index_raises_a_clear_error(mage_postgres, pg, schema, engine):
    run(pg, f'CREATE TABLE {schema}.t (id integer, val text)')

    with pytest.raises(ValueError, match='unique index or constraint'):
        export(
            mage_postgres,
            schema,
            NEW,
            engine,
            unique_conflict_method='UPDATE',
            unique_constraints=['id'],
        )

    export(mage_postgres, schema, NEW, engine)
    assert rows(pg, schema) == [(2, 'new2'), (3, 'new3')]


@pytest.mark.parametrize('engine', ENGINES)
def test_created_table_gets_the_unique_constraint(mage_postgres, pg, schema, engine):
    export(
        mage_postgres,
        schema,
        NEW,
        engine,
        if_exists='replace',
        unique_conflict_method='UPDATE',
        unique_constraints=['id'],
    )
    export(
        mage_postgres,
        schema,
        {'id': [3, 4], 'val': ['newer3', 'new4']},
        engine,
        unique_conflict_method='UPDATE',
        unique_constraints=['id'],
    )

    assert rows(pg, schema) == [(2, 'new2'), (3, 'newer3'), (4, 'new4')]
    with pg.cursor() as cursor:
        cursor.execute(
            'SELECT count(*) FROM pg_index WHERE indrelid = to_regclass(%s) AND indisunique',
            (f'{schema}.t',),
        )
        assert cursor.fetchone()[0] == 1


# --- batching ----------------------------------------------------------------------------


BIG = 2501


@pytest.mark.parametrize('page_size', [1, 7, 1000, 2500, 2501, 5000])
@pytest.mark.parametrize('engine', ENGINES)
def test_upsert_pages(mage_postgres, pg, schema, engine, page_size):
    run(pg, f'CREATE TABLE {schema}.t (id integer PRIMARY KEY, val text, n smallint)')
    run(pg, f"INSERT INTO {schema}.t SELECT g, 'old', 0 FROM generate_series(0, 999) g")
    data = {
        'id': list(range(BIG)),
        'val': [f'v{i}' for i in range(BIG)],
        'n': [(-32768 if i % 2 else 32767) for i in range(BIG)],
    }

    export(
        mage_postgres,
        schema,
        data,
        engine,
        insert_page_size=page_size,
        unique_conflict_method='UPDATE',
        unique_constraints=['id'],
    )

    with pg.cursor() as cursor:
        cursor.execute(
            f"SELECT count(*), count(*) FILTER (WHERE val = 'v' || id), min(n), max(n) "
            f'FROM {schema}.t',
        )
        assert cursor.fetchone() == (BIG, BIG, -32768, 32767)


@pytest.mark.parametrize('engine', ENGINES)
def test_copy_in_several_chunks(mage_postgres, pg, schema, engine, monkeypatch):
    monkeypatch.setattr(postgres_module, 'COPY_CHUNK_ROWS', 7)
    data = {'id': list(range(50)), 'val': [f'v{i}' for i in range(50)]}

    export(mage_postgres, schema, data, engine, if_exists='replace')

    assert rows(pg, schema) == [(i, f'v{i}') for i in sorted(range(50))]


@pytest.mark.parametrize('engine', ENGINES)
def test_a_bad_row_in_a_late_chunk_writes_nothing(mage_postgres, pg, schema, engine, monkeypatch):
    monkeypatch.setattr(postgres_module, 'COPY_CHUNK_ROWS', 7)
    run(pg, f'CREATE TABLE {schema}.t (id integer, val varchar(3))')
    data = {'id': list(range(30)), 'val': ['ok'] * 25 + ['too long'] + ['ok'] * 4}

    with pytest.raises(psycopg2.errors.StringDataRightTruncation):
        export(mage_postgres, schema, data, engine)

    assert rows(pg, schema) == []


# --- write policies and failures ------------------------------------------------------------


@pytest.mark.parametrize('engine', ENGINES)
def test_failed_replace_keeps_the_old_rows(mage_postgres, pg, schema, keyed_table, engine):
    with pytest.raises(ValueError):
        export(
            mage_postgres,
            schema,
            DUPLICATES,
            engine,
            if_exists='replace',
            unique_conflict_method='UPDATE',
            unique_constraints=['id'],
        )
    with pytest.raises(psycopg2.errors.UniqueViolation):
        export(
            mage_postgres, schema, {'id': [7, 7], 'val': ['a', 'b']}, engine, if_exists='replace'
        )

    assert rows(pg, schema) == [(1, 'old1'), (2, 'old2')]


@pytest.mark.parametrize('engine', ENGINES)
def test_client_is_usable_after_a_failed_export(mage_postgres, pg, schema, keyed_table, engine):
    with pytest.raises(psycopg2.errors.UniqueViolation):
        export(mage_postgres, schema, NEW, engine)

    export(mage_postgres, schema, {'id': [9], 'val': ['after']}, engine)

    assert rows(pg, schema) == [(1, 'old1'), (2, 'old2'), (9, 'after')]


@pytest.mark.parametrize('engine', ENGINES)
def test_fail_policy(mage_postgres, pg, schema, keyed_table, engine):
    with pytest.raises(ValueError, match='already exists'):
        export(mage_postgres, schema, NEW, engine, if_exists='fail')

    assert rows(pg, schema) == [(1, 'old1'), (2, 'old2')]


@pytest.mark.parametrize('engine', ENGINES)
def test_replace_deletes_rows_and_keeps_the_table(mage_postgres, pg, schema, keyed_table, engine):
    export(mage_postgres, schema, NEW, engine, if_exists='replace')

    assert rows(pg, schema) == [(2, 'new2'), (3, 'new3')]
    with pg.cursor() as cursor:
        cursor.execute(
            'SELECT format_type(atttypid, atttypmod) FROM pg_attribute '
            "WHERE attrelid = to_regclass(%s) AND attname = 'id'",
            (f'{schema}.t',),
        )
        assert cursor.fetchone()[0] == 'integer'


@pytest.mark.parametrize('engine', ENGINES)
def test_drop_table_on_replace_with_a_dependent_view(
    mage_postgres, pg, schema, keyed_table, engine
):
    run(pg, f'CREATE VIEW {schema}.v AS SELECT id FROM {schema}.t')

    with pytest.raises(psycopg2.errors.DependentObjectsStillExist):
        export(mage_postgres, schema, NEW, engine, if_exists='replace', drop_table_on_replace=True)
    export(
        mage_postgres,
        schema,
        NEW,
        engine,
        if_exists='replace',
        drop_table_on_replace=True,
        cascade_on_drop=True,
    )

    assert rows(pg, schema) == [(2, 'new2'), (3, 'new3')]
    with pg.cursor() as cursor:
        cursor.execute('SELECT to_regclass(%s)', (f'{schema}.v',))
        assert cursor.fetchone()[0] is None


@pytest.mark.parametrize('engine', ENGINES)
def test_upsert_twice_changes_nothing(mage_postgres, pg, schema, keyed_table, engine):
    for _ in range(2):
        export(
            mage_postgres,
            schema,
            NEW,
            engine,
            unique_conflict_method='UPDATE',
            unique_constraints=['id'],
        )

    assert rows(pg, schema) == [(1, 'old1'), (2, 'new2'), (3, 'new3')]
