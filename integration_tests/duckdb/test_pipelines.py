"""
Mage pipelines on a DuckDB database file.

duckdb_polars reads the source table into Polars, transforms it with a LazyFrame and
exports the result with Mage's DuckDB client. duckdb_sql runs a DuckDB SQL block on the
Polars output of a Python block: Mage exports the upstream frame to a table, runs the
query, and stores the result in another table. Both results are compared with the same
transformation computed in SQL from the source.
"""
import pytest

from integration_tests.data import duckdb_dataset
from integration_tests.mage_runner import run_pipeline

DERIVED = """
CREATE TABLE expected AS
SELECT
    *,
    length(c_varchar) AS text_length,
    c_bigint::HUGEINT + 1 AS bigint_plus_one,
    len(c_int_list) AS list_length,
    year(c_date) AS date_year
FROM src
"""
DERIVED_COLUMNS = ['text_length', 'bigint_plus_one', 'list_length', 'date_year']


@pytest.fixture
def duckdb_database(duckdb_client, duckdb_path, monkeypatch):
    duckdb_client.conn.execute(DERIVED)
    monkeypatch.setenv('MAGE_TEST_DUCKDB_DATABASE', duckdb_path)
    return duckdb_client


def compare(client, table):
    columns = duckdb_dataset.COLUMN_NAMES + DERIVED_COLUMNS
    expected_types = dict(client.conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'expected'",
    ).fetchall())
    original = dict(duckdb_dataset.COLUMNS)
    duckdb_dataset.COLUMNS.update({c: expected_types[c] for c in DERIVED_COLUMNS})
    try:
        return duckdb_dataset.mismatches(client.conn, 'expected', table, columns=columns)
    finally:
        duckdb_dataset.COLUMNS.clear()
        duckdb_dataset.COLUMNS.update(original)


def test_polars_pipeline(mage_project, duckdb_database):
    rows = duckdb_database.conn.execute('SELECT count(*) FROM src').fetchone()[0]

    run_pipeline('duckdb_polars', expected_rows=rows)

    assert compare(duckdb_database, 'polars_result') == {}


def test_sql_block_on_a_polars_upstream(mage_project, duckdb_database):
    rows = duckdb_database.conn.execute('SELECT count(*) FROM src').fetchone()[0]

    run_pipeline('duckdb_sql', expected_rows=rows)

    tables = {
        name for (name,) in duckdb_database.conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'",
        ).fetchall()
    }
    upstream = [t for t in tables if 'duckdb_polars_load' in t]
    result = [t for t in tables if 'duckdb_sql_transform' in t]
    assert len(upstream) == 1 and len(result) == 1, tables
    # The upstream table holds the Polars frame exactly, and the SQL block's table the
    # query result.
    assert duckdb_dataset.mismatches(duckdb_database.conn, 'src', upstream[0]) == {}
    assert compare(duckdb_database, result[0]) == {}
