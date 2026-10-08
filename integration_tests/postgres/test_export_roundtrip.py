"""
Round trip: PostgreSQL, Mage's load, Mage's export, PostgreSQL.

The destination is compared with the source column by column in SQL. exact_types and
Polars frames must arrive unchanged through COPY and through INSERT ... ON CONFLICT, into
an existing table with the source types and into a table Mage creates.
"""

import psycopg2
import pytest

from integration_tests.data import postgres_dataset

LOADS = {
    'pandas_exact': dict(exact_types=True),
    'polars': dict(polars=True),
    'pandas_default': dict(),
}
PATHS = {
    'copy': dict(),
    'upsert': dict(unique_constraints=['id'], unique_conflict_method='UPDATE'),
}
# The default load turns bigint and numeric into float and merges NaN with NULL in float
# columns. Nothing else may change.
DEFAULT_LOAD_LOSSES = {'c_bigint', 'c_numeric', 'c_numeric_free', 'c_real'}

# Column types Mage chooses when it creates the table, by load.
CREATED_TYPES = {
    'pandas_exact': {
        'id': 'bigint',
        'c_smallint': 'bigint',
        'c_integer': 'bigint',
        'c_bigint': 'bigint',
        'c_numeric': 'numeric',
        'c_numeric_free': 'numeric',
        'c_real': 'double precision',
        'c_double': 'double precision',
        'c_bool': 'boolean',
        'c_text': 'text',
        'c_varchar': 'text',
        'c_char': 'text',
        'c_bytea': 'bytea',
        'c_date': 'date',
        'c_time': 'time without time zone',
        'c_timetz': 'time with time zone',
        'c_timestamp': 'timestamp without time zone',
        'c_timestamptz': 'timestamp with time zone',
        'c_interval': 'interval',
        'c_uuid': 'text',
        'c_json': 'jsonb',
        'c_jsonb': 'jsonb',
        'c_int_array': 'bigint[]',
        'c_bigint_array': 'bigint[]',
        'c_float_array': 'double precision[]',
        'c_bool_array': 'boolean[]',
        'c_text_array': 'text[]',
        'c_numeric_array': 'numeric[]',
        'c_date_array': 'date[]',
        'c_uuid_array': 'text[]',
        'c_int_matrix': 'jsonb',
        'c_enum': 'text',
    },
    'polars': {
        'id': 'integer',
        'c_smallint': 'smallint',
        'c_integer': 'integer',
        'c_bigint': 'bigint',
        'c_numeric': 'numeric(38,10)',
        'c_numeric_free': 'text',
        'c_real': 'real',
        'c_double': 'double precision',
        'c_bool': 'boolean',
        'c_text': 'text',
        'c_varchar': 'text',
        'c_char': 'text',
        'c_bytea': 'bytea',
        'c_date': 'date',
        'c_time': 'time without time zone',
        'c_timetz': 'text',
        'c_timestamp': 'timestamp without time zone',
        'c_timestamptz': 'timestamp with time zone',
        'c_interval': 'interval',
        'c_uuid': 'text',
        'c_json': 'text',
        'c_jsonb': 'text',
        'c_int_array': 'integer[]',
        'c_bigint_array': 'bigint[]',
        'c_float_array': 'double precision[]',
        'c_bool_array': 'boolean[]',
        'c_text_array': 'text[]',
        'c_numeric_array': 'text[]',
        'c_date_array': 'date[]',
        'c_uuid_array': 'text[]',
        'c_int_matrix': 'jsonb',
        'c_enum': 'text',
    },
}


# Row 5 holds the largest bigint. The default load turns it into a float that rounds up
# to 2**63, which no bigint column accepts; test_default_load_cannot_write_the_largest_bigint
# covers it.
DEFAULT_LOAD_ROWS = 'id <> 5'


def load(client, schema, mode, where='true'):
    return client.load(
        f'SELECT * FROM {schema}.src WHERE {where} ORDER BY id',
        verbose=False,
        **LOADS[mode],
    )


def rows_for(mode):
    return DEFAULT_LOAD_ROWS if mode == 'pandas_default' else 'true'


def column_types(pg, schema, table):
    return dict(postgres_dataset._format_types(pg, schema, table))


@pytest.mark.parametrize('path', sorted(PATHS))
@pytest.mark.parametrize('mode', sorted(LOADS))
def test_round_trip_into_an_existing_table(mage_postgres, pg, schema, source_table, mode, path):
    frame = load(mage_postgres, schema, mode, rows_for(mode))
    postgres_dataset.create_like_table(pg, schema, 'src', 'dst')

    mage_postgres.export(
        frame,
        schema_name=schema,
        table_name='dst',
        if_exists='append',
        verbose=False,
        **PATHS[path],
    )

    problems = postgres_dataset.mismatches(pg, schema, 'src', 'dst', where=rows_for(mode))
    if mode == 'pandas_default':
        assert set(problems) == DEFAULT_LOAD_LOSSES, problems
    else:
        assert problems == {}


@pytest.mark.parametrize('path', sorted(PATHS))
@pytest.mark.parametrize('mode', sorted(LOADS))
def test_round_trip_into_a_table_mage_creates(mage_postgres, pg, schema, source_table, mode, path):
    frame = load(mage_postgres, schema, mode, rows_for(mode))

    mage_postgres.export(
        frame,
        schema_name=schema,
        table_name='dst',
        if_exists='replace',
        verbose=False,
        **PATHS[path],
    )

    problems = postgres_dataset.mismatches(pg, schema, 'src', 'dst', where=rows_for(mode))
    if mode == 'pandas_default':
        assert set(problems) == DEFAULT_LOAD_LOSSES, problems
    else:
        assert problems == {}


@pytest.mark.parametrize('path', sorted(PATHS))
def test_default_load_cannot_write_the_largest_bigint(
    mage_postgres, pg, schema, source_table, path
):
    """
    The default load reads bigint 9223372036854775807 as the float 2**63. Writing it back
    to a bigint column fails, and the client stays usable.
    """
    frame = load(mage_postgres, schema, 'pandas_default', 'id = 5')
    postgres_dataset.create_like_table(pg, schema, 'src', 'dst')

    with pytest.raises(psycopg2.errors.NumericValueOutOfRange):
        mage_postgres.export(
            frame,
            schema_name=schema,
            table_name='dst',
            if_exists='append',
            verbose=False,
            **PATHS[path],
        )

    exact = load(mage_postgres, schema, 'pandas_exact', 'id = 5')
    mage_postgres.export(
        exact,
        schema_name=schema,
        table_name='dst',
        if_exists='append',
        verbose=False,
        **PATHS[path],
    )
    assert postgres_dataset.mismatches(pg, schema, 'src', 'dst', where='id = 5') == {}


@pytest.mark.parametrize('mode', sorted(CREATED_TYPES))
def test_created_table_column_types(mage_postgres, pg, schema, source_table, mode):
    frame = load(mage_postgres, schema, mode)

    mage_postgres.export(frame, schema_name=schema, table_name='dst', verbose=False)

    assert column_types(pg, schema, 'dst') == CREATED_TYPES[mode]


@pytest.mark.parametrize('mode', ['pandas_exact', 'polars'])
def test_upsert_twice_leaves_the_same_rows(mage_postgres, pg, schema, source_table, mode):
    frame = load(mage_postgres, schema, mode)
    postgres_dataset.create_like_table(pg, schema, 'src', 'dst')

    for _ in range(2):
        mage_postgres.export(
            frame,
            schema_name=schema,
            table_name='dst',
            if_exists='append',
            verbose=False,
            **PATHS['upsert'],
        )

    assert postgres_dataset.mismatches(pg, schema, 'src', 'dst') == {}


def test_comparison_reports_a_change_in_every_column(pg, schema, source_table):
    """
    The comparison must not pass a table that differs. Change one value per column in a
    copy and expect every column to be reported.
    """
    postgres_dataset.create_like_table(pg, schema, 'src', 'dst')
    with pg.cursor() as cursor:
        cursor.execute(f'INSERT INTO {schema}.dst SELECT * FROM {schema}.src')
        # Row 3 is NULL in every column, so setting a value differs for every type.
        cursor.execute(f'SELECT * FROM {schema}.src WHERE id = 1')
        names = [d.name for d in cursor.description]
        for name in names:
            if name == 'id':
                continue
            cursor.execute(
                f'UPDATE {schema}.dst SET "{name}" = '
                f'(SELECT "{name}" FROM {schema}.src WHERE id = 1) WHERE id = 3',
            )
    pg.commit()

    problems = postgres_dataset.mismatches(pg, schema, 'src', 'dst')

    assert set(problems) == set(postgres_dataset.COLUMN_NAMES) - {'id'}
