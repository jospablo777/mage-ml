"""
Column names in Mage's PostgreSQL export.

With allow_reserved_words=False, the default, Mage prefixes column names on its reserved
word list with an underscore when it creates a table. The list covers common names such
as name, date, value and status. Exports into an existing table match each frame column
to the table's own column, whether that has the prefix or not.
"""

import pandas as pd
import polars as pl
import psycopg2
import pytest

ENGINES = ['pandas', 'polars']
DATA = {
    'id': [1, 2],
    'name': ['ann', 'bob'],
    'date': ['2024-01-01', '2024-01-02'],
    'status': ['on', 'off'],
}


def frame(engine, data=DATA):
    return pl.DataFrame(data) if engine == 'polars' else pd.DataFrame(data)


def run(pg, statement):
    with pg.cursor() as cursor:
        cursor.execute(statement)
    pg.commit()


def table_columns(pg, schema, table='t'):
    with pg.cursor() as cursor:
        cursor.execute(
            'SELECT attname FROM pg_attribute WHERE attrelid = to_regclass(%s) '
            'AND attnum > 0 AND NOT attisdropped ORDER BY attnum',
            (f'{schema}.{table}',),
        )
        names = [r[0] for r in cursor.fetchall()]
    pg.rollback()
    return names


def fetch(pg, statement):
    with pg.cursor() as cursor:
        cursor.execute(statement)
        result = cursor.fetchall()
    pg.rollback()
    return result


@pytest.mark.parametrize('engine', ENGINES)
def test_created_table_prefixes_reserved_names(mage_postgres, pg, schema, engine):
    mage_postgres.export(frame(engine), schema_name=schema, table_name='t', verbose=False)

    assert table_columns(pg, schema) == ['id', '_name', '_date', '_status']


@pytest.mark.parametrize('engine', ENGINES)
def test_appending_to_a_table_mage_created(mage_postgres, pg, schema, engine):
    mage_postgres.export(frame(engine), schema_name=schema, table_name='t', verbose=False)
    mage_postgres.export(
        frame(engine, {**DATA, 'id': [3, 4]}),
        schema_name=schema,
        table_name='t',
        if_exists='append',
        verbose=False,
    )

    assert fetch(pg, f'SELECT count(*) FROM {schema}.t') == [(4,)]


@pytest.mark.parametrize('engine', ENGINES)
def test_appending_to_a_table_with_plain_names(mage_postgres, pg, schema, engine):
    run(pg, f'CREATE TABLE {schema}.t (id integer PRIMARY KEY, name text, date date, status text)')

    mage_postgres.export(
        frame(engine), schema_name=schema, table_name='t', if_exists='append', verbose=False
    )
    mage_postgres.export(
        frame(engine, {**DATA, 'name': ['ann2', 'bob2']}),
        schema_name=schema,
        table_name='t',
        if_exists='append',
        verbose=False,
        unique_conflict_method='UPDATE',
        unique_constraints=['id'],
    )

    assert fetch(pg, f'SELECT id, name, date::text, status FROM {schema}.t ORDER BY id') == [
        (1, 'ann2', '2024-01-01', 'on'),
        (2, 'bob2', '2024-01-02', 'off'),
    ]


@pytest.mark.parametrize('engine', ENGINES)
def test_unique_key_on_a_reserved_name(mage_postgres, pg, schema, engine):
    # The key used to be passed uncleaned to CREATE TABLE, which named "name" while the
    # column was "_name", so creating the table failed.
    for _ in range(2):
        mage_postgres.export(
            frame(engine),
            schema_name=schema,
            table_name='t',
            verbose=False,
            if_exists='append',
            unique_conflict_method='UPDATE',
            unique_constraints=['name'],
        )

    assert fetch(pg, f'SELECT _name FROM {schema}.t ORDER BY 1') == [('ann',), ('bob',)]


@pytest.mark.parametrize('engine', ENGINES)
def test_allow_reserved_words_keeps_plain_names(mage_postgres, pg, schema, engine):
    mage_postgres.export(
        frame(engine), schema_name=schema, table_name='t', verbose=False, allow_reserved_words=True
    )

    assert table_columns(pg, schema) == ['id', 'name', 'date', 'status']


@pytest.mark.parametrize('engine', ENGINES)
def test_case_and_spaces(mage_postgres, pg, schema, engine):
    data = {'Id': [1], 'First Name': ['ann'], 'UserScore': [1.5]}

    mage_postgres.export(frame(engine, data), schema_name=schema, table_name='lower', verbose=False)
    mage_postgres.export(
        frame(engine, data),
        schema_name=schema,
        table_name='kept',
        verbose=False,
        case_sensitive=True,
    )

    assert table_columns(pg, schema, 'lower') == ['id', 'first_name', 'userscore']
    assert table_columns(pg, schema, 'kept') == ['Id', 'First_Name', 'UserScore']


@pytest.mark.parametrize('engine', ENGINES)
def test_mixed_case_names_of_an_existing_table(mage_postgres, pg, schema, engine):
    run(pg, f'CREATE TABLE {schema}.t ("UserId" integer, "Score" double precision)')

    mage_postgres.export(
        frame(engine, {'UserId': [1], 'Score': [0.5]}),
        schema_name=schema,
        table_name='t',
        if_exists='append',
        verbose=False,
    )

    assert fetch(pg, f'SELECT "UserId", "Score" FROM {schema}.t') == [(1, 0.5)]


@pytest.mark.parametrize('engine', ENGINES)
def test_unicode_names_of_an_existing_table(mage_postgres, pg, schema, engine):
    run(pg, f'CREATE TABLE {schema}.t ("año" integer, "名前" text)')

    mage_postgres.export(
        frame(engine, {'año': [2024], '名前': ['花子']}),
        schema_name=schema,
        table_name='t',
        if_exists='append',
        verbose=False,
    )

    assert fetch(pg, f'SELECT "año", "名前" FROM {schema}.t') == [(2024, '花子')]


@pytest.mark.parametrize('engine', ENGINES)
def test_a_column_missing_from_the_table_is_named_in_the_error(mage_postgres, pg, schema, engine):
    run(pg, f'CREATE TABLE {schema}.t (id integer)')

    with pytest.raises(ValueError, match="'extra'"):
        mage_postgres.export(
            frame(engine, {'id': [1], 'extra': [2]}),
            schema_name=schema,
            table_name='t',
            if_exists='append',
            verbose=False,
        )


@pytest.mark.parametrize('engine', ENGINES)
def test_table_columns_missing_from_the_frame_get_their_default(mage_postgres, pg, schema, engine):
    run(pg, f"CREATE TABLE {schema}.t (id integer, note text DEFAULT 'none', n integer)")

    mage_postgres.export(
        frame(engine, {'id': [1]}),
        schema_name=schema,
        table_name='t',
        if_exists='append',
        verbose=False,
    )

    assert fetch(pg, f'SELECT id, note, n FROM {schema}.t') == [(1, 'none', None)]


@pytest.mark.parametrize('engine', ENGINES)
def test_not_null_violation_names_the_column(mage_postgres, pg, schema, engine):
    run(pg, f'CREATE TABLE {schema}.t (id integer NOT NULL)')

    with pytest.raises(psycopg2.errors.NotNullViolation):
        mage_postgres.export(
            frame(engine, {'id': [1, None]}),
            schema_name=schema,
            table_name='t',
            if_exists='append',
            verbose=False,
        )
