"""
duckdb_sql (a Polars loader and a DuckDB SQL transformer) exported with
`mage export service` and run by mage-service: it must leave the tables that the pipeline
run by Mage leaves, column by column.
"""
import duckdb

from integration_tests.exported_services import differences, run_exported
from integration_tests.mage_runner import run_pipeline
from integration_tests.duckdb.test_pipelines import duckdb_database  # noqa: F401

KEEP = {'src', 'expected'}


def snapshot(path: str) -> dict:
    connection = duckdb.connect(path)
    try:
        result = {}
        names = connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'",
        ).fetchall()
        for (name,) in names:
            if name in KEEP:
                continue
            columns = connection.execute(
                'SELECT column_name, data_type FROM information_schema.columns '
                "WHERE table_schema = 'main' AND table_name = ? ORDER BY ordinal_position",
                [name],
            ).fetchall()
            summary = {'rows': connection.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]}
            for column, data_type in columns:
                value = f"coalesce(CAST(\"{column}\" AS VARCHAR), 'NULL')"
                digest = connection.execute(
                    f"SELECT md5(coalesce(string_agg({value}, '|' ORDER BY {value}), '')) "
                    f'FROM "{name}"'
                ).fetchone()[0]
                summary[column] = (data_type, digest)
            result[name] = summary
        return result
    finally:
        connection.close()


def drop(path: str, names) -> None:
    connection = duckdb.connect(path)
    try:
        for name in names:
            connection.execute(f'DROP TABLE IF EXISTS "{name}"')
    finally:
        connection.close()


def test_an_exported_duckdb_sql_pipeline_leaves_the_tables_mage_leaves(
    mage_project, duckdb_database, duckdb_path, tmp_path,  # noqa: F811
):
    rows = duckdb_database.conn.execute('SELECT count(*) FROM src').fetchone()[0]
    run_pipeline('duckdb_sql', expected_rows=rows)
    # The service is another process; DuckDB lets one process write a database file.
    duckdb_database.conn.close()
    expected = snapshot(duckdb_path)
    assert expected
    drop(duckdb_path, expected)

    run_exported(mage_project, 'duckdb_sql', tmp_path, dict(expected_rows=rows))

    assert differences(expected, snapshot(duckdb_path)) == {}
