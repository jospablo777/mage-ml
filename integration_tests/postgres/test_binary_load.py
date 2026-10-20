"""
Polars loads through binary COPY on the Rust client (postgres_native.load_polars) return
what the psycopg2 load returns: the same column types and values for every PostgreSQL type
of the load tests' table.
"""
from unittest.mock import patch

import pytest

from integration_tests.postgres.test_load import query
from mage_ai.io import postgres_native

pytestmark = pytest.mark.skipif(not postgres_native.loads_enabled(),
                                reason='column_atlas_native has no PgConnection')


def native_load(client, sql):
    """Loads and returns the frame and whether the native load produced it."""
    results = []
    real = postgres_native.load_polars

    def spy(*args, **kwargs):
        results.append(real(*args, **kwargs))
        return results[-1]

    with patch.object(postgres_native, 'load_polars', spy):
        frame = client.load(sql, verbose=False, polars=True)
    return frame, bool(results) and results[-1] is not None


def test_every_type_loads_as_the_psycopg2_load_returns_it(mage_postgres, schema, source_table):
    with patch.dict('os.environ', MAGE_POSTGRES_BINARY_LOAD='off'):
        expected = mage_postgres.load(query(schema), verbose=False, polars=True)
    frame, native = native_load(mage_postgres, query(schema))
    assert native, 'the load did not go through binary COPY'
    assert frame.schema == expected.schema
    for name in expected.columns:
        assert frame[name].equals(expected[name], check_dtypes=True), name


def test_the_connection_is_kept_for_the_next_load(mage_postgres, schema, source_table):
    native_load(mage_postgres, query(schema))
    first = mage_postgres._native
    _, native = native_load(mage_postgres, f'SELECT count(*) AS n FROM {schema}.src')
    assert native and mage_postgres._native is first


def test_uncommitted_changes_are_seen_through_psycopg2(mage_postgres, schema, source_table):
    mage_postgres.execute(f'CREATE TABLE {schema}.fresh AS SELECT 1 AS one')
    frame, native = native_load(mage_postgres, f'SELECT * FROM {schema}.fresh')
    assert not native
    assert frame.to_dicts() == [{'one': 1}]
    mage_postgres.conn.rollback()


def test_an_unknown_connection_setting_keeps_loads_on_psycopg2():
    settings = dict(dbname='d', user='u', password='p', host='h', port=5432,
                    sslcert='/client.crt')
    assert postgres_native.connection_settings(settings, 'h', 5432) is None
    del settings['sslcert']
    assert postgres_native.connection_settings(settings, 'h', 5432) == dict(
        POSTGRES_HOST='h', POSTGRES_PORT='5432', POSTGRES_DBNAME='d', POSTGRES_USER='u',
        POSTGRES_PASSWORD='p',
    )
