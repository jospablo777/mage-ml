"""
PostgreSQL SQL pipelines exported with `mage export service` and run by mage-service
without Mage. Each pipeline runs in Mage first; every table it leaves in the test schema
is fingerprinted and dropped, the service runs the same pipeline, and its tables must be
the same: the same names, columns, types and rows. Python blocks in the pipelines check
the frames the SQL blocks hand them, in both runs.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from integration_tests.mage_runner import run_pipeline

REPO = Path(__file__).resolve().parents[2]
SERVICE = REPO / 'mage_ai' / 'pipeline_services' / 'service'
BINARY = SERVICE / 'target' / 'debug' / 'mage-service'
ROWS = 200


@pytest.fixture(scope='session')
def service_binary():
    if not BINARY.is_file():
        subprocess.run(['cargo', 'build', '-p', 'mage-service'], cwd=SERVICE, check=True)
    return BINARY


def tables(pg, schema, keep=('src',)):
    """
    Per table: its row count and, per column, its type and an order-independent hash of
    its values, so a difference names the column.
    """
    result = {}
    with pg.cursor() as cursor:
        cursor.execute(
            'SELECT table_name FROM information_schema.tables WHERE table_schema = %s',
            (schema,),
        )
        for (name,) in cursor.fetchall():
            if name in keep:
                continue
            cursor.execute(
                'SELECT column_name, data_type FROM information_schema.columns '
                'WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position',
                (schema, name),
            )
            columns = cursor.fetchall()
            cursor.execute(f'SELECT count(*) FROM {schema}.{name}')
            summary = {'rows': cursor.fetchone()[0]}
            for column, data_type in columns:
                value = f'coalesce("{column}"::text, \'NULL\')'
                cursor.execute(
                    f"SELECT md5(coalesce(string_agg({value}, '|' ORDER BY {value}), '')) "
                    f'FROM {schema}.{name}'
                )
                summary[column] = (data_type, cursor.fetchone()[0])
            result[name] = summary
    pg.rollback()
    return result


def drop_tables(pg, schema, names):
    with pg.cursor() as cursor:
        for name in names:
            cursor.execute(f'DROP TABLE IF EXISTS {schema}.{name} CASCADE')
    pg.commit()


def create_raw_destination(pg, schema):
    """pg_sql_raw inserts into a table that exists, as in test_sql_blocks.py."""
    with pg.cursor() as cursor:
        cursor.execute(f'DROP TABLE IF EXISTS {schema}.pg_sql_raw_dst')
        cursor.execute(f"""
            CREATE TABLE {schema}.pg_sql_raw_dst (
                id bigint, c_big bigint, c_text text, c_date date, c_tstz timestamptz,
                c_int_list bigint[], c_struct jsonb
            )
        """)
    pg.commit()


# (pipeline, its variables, a setup before each run)
CASES = [
    ('pg_sql_pandas', lambda schema, rows: dict(rows=ROWS), None),
    ('pg_sql_polars', lambda schema, rows: dict(rows=ROWS), None),
    ('pg_sql_to_python', lambda schema, rows: dict(schema=schema, expected_rows=rows), None),
    ('pg_sql_raw', lambda schema, rows: dict(schema=schema, rows=ROWS), create_raw_destination),
    # A raw query: it leaves no tables, and its Python check verifies the frame it gets.
    (
        'pg_sql_raw_to_python',
        lambda schema, rows: dict(schema=schema, expected_rows=rows),
        None,
    ),
]


@pytest.mark.parametrize('pipeline, variables, setup', CASES, ids=[c[0] for c in CASES])
def test_an_exported_sql_pipeline_leaves_the_tables_mage_leaves(
    pipeline, variables, setup, mage_project, pg, profile_schema, source_table, source_rows,
    service_binary, tmp_path,
):
    from mage_ai.pipeline_services import export

    values = variables(profile_schema, len(source_rows))
    if setup:
        setup(pg, profile_schema)
    run_pipeline(pipeline, **values)
    expected = tables(pg, profile_schema)
    drop_tables(pg, profile_schema, expected)
    if setup:
        setup(pg, profile_schema)

    captured = export.capture(mage_project, [pipeline], name=pipeline.replace('_', '-'))
    out = export.write(captured, str(tmp_path / 'service'), force=True)
    assert 'psycopg2-binary' in captured.requirements or 'psycopg2' in captured.requirements
    arguments = []
    for key, value in values.items():
        arguments += ['--var', f'{key}={value}']
    result = subprocess.run(
        [str(service_binary), 'run', pipeline, '--json', *arguments],
        capture_output=True, text=True, timeout=600,
        env=dict(
            os.environ,
            MAGE_SERVICE_DIR=str(out),
            MAGE_SERVICE_DATA=str(tmp_path / 'data'),
            MAGE_SERVICE_WORKER=str(out / 'python/mage_ai/pipeline_services/runtime/worker.py'),
            MAGE_SERVICE_PYTHON=sys.executable,
            # The exported copy of Mage first; the repository for integration_tests.data,
            # which the test pipelines' Python blocks import.
            PYTHONPATH=os.pathsep.join([str(out / 'python'), str(REPO)]),
        ),
    )
    assert result.returncode == 0, result.stdout[-6000:] + result.stderr[-3000:]
    run = json.loads(result.stdout[result.stdout.index('{'):])
    assert {b['status'] for b in run['blocks']} == {'completed'}
    actual = tables(pg, profile_schema)
    assert sorted(actual) == sorted(expected)
    differences = {
        f'{table}.{column}': (expected[table].get(column), actual[table].get(column))
        for table in expected
        for column in set(expected[table]) | set(actual[table])
        if expected[table].get(column) != actual[table].get(column)
    }
    assert differences == {}
