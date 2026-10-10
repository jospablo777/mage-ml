"""
The r_dbi pipeline (an R loader that reads PostgreSQL with DBI, an R transformer and an
R exporter) exported with `mage export service` and run by mage-service without Mage. It
must store what the pipeline run by Mage stores (test_pipelines.py).
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from integration_tests.r.test_pipelines import dbi_mismatches, limited_values

REPO = Path(__file__).resolve().parents[2]
SERVICE = REPO / 'mage_ai' / 'pipeline_services' / 'service'
BINARY = SERVICE / 'target' / 'debug' / 'mage-service'


@pytest.fixture(scope='session')
def service_binary():
    if not BINARY.is_file():
        subprocess.run(['cargo', 'build', '-p', 'mage-service'], cwd=SERVICE, check=True)
    return BINARY


def test_an_exported_r_pipeline_stores_what_mage_stores(
    mage_project, pg, profile_schema, source_table, source_rows, service_binary, tmp_path,
):
    from mage_ai.pipeline_services import export

    captured = export.capture(mage_project, ['r_dbi'], name='r-dbi')
    out = export.write(captured, str(tmp_path / 'service'), force=True)
    assert captured.needs_r
    assert (out / 'r' / 'rv.lock').is_file()
    dockerfile = (out / 'Dockerfile').read_text()
    assert 'rv sync' in dockerfile and 'slim-trixie' in dockerfile
    with pg.cursor() as cursor:
        cursor.execute(f'DROP TABLE IF EXISTS {profile_schema}.r_dbi_dst')
    pg.commit()

    result = subprocess.run(
        [
            str(service_binary), 'run', 'r_dbi', '--json',
            '--var', f'max_id={10**9}',
            '--var', f'expected_rows={len(source_rows)}',
        ],
        capture_output=True, text=True, timeout=600,
        env=dict(
            os.environ,
            MAGE_SERVICE_DIR=str(out),
            MAGE_SERVICE_DATA=str(tmp_path / 'data'),
            MAGE_SERVICE_WORKER=str(out / 'python/mage_ai/pipeline_services/runtime/worker.py'),
            MAGE_SERVICE_PYTHON=sys.executable,
            # The exported copy of Mage's Python code, R runner included.
            PYTHONPATH=str(out / 'python'),
        ),
    )
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-4000:]
    run = json.loads(result.stdout[result.stdout.index('{'):])
    assert {b['status'] for b in run['blocks']} == {'completed'}

    with pg.cursor() as cursor:
        cursor.execute(f'SELECT count(*) FROM {profile_schema}.r_dbi_dst')
        assert cursor.fetchone()[0] == len(source_rows)
    assert dbi_mismatches(pg, profile_schema) == {}
    limited = limited_values(pg, profile_schema)
    for name in ('c_integer', 'c_bigint', 'c_numeric_free'):
        assert limited[name] == (1, 0), (name, limited[name])


def test_an_exported_sql_r_polars_pipeline_leaves_the_tables_mage_leaves(
    mage_project, pg, profile_schema, source_table, service_binary, tmp_path,
):
    """r_sql: a SQL loader, an R transformer, a Polars transformer and a SQL exporter."""
    from integration_tests.mage_runner import run_pipeline
    from integration_tests.postgres.test_exported_sql_service import drop_tables, tables
    from mage_ai.pipeline_services import export

    run_pipeline('r_sql', schema=profile_schema)
    expected = tables(pg, profile_schema)
    assert expected
    drop_tables(pg, profile_schema, expected)

    out = export.write(
        export.capture(mage_project, ['r_sql'], name='r-sql'), str(tmp_path / 'service'),
        force=True,
    )
    result = subprocess.run(
        [str(service_binary), 'run', 'r_sql', '--json', '--var', f'schema={profile_schema}'],
        capture_output=True, text=True, timeout=600,
        env=dict(
            os.environ,
            MAGE_SERVICE_DIR=str(out),
            MAGE_SERVICE_DATA=str(tmp_path / 'data'),
            MAGE_SERVICE_WORKER=str(out / 'python/mage_ai/pipeline_services/runtime/worker.py'),
            MAGE_SERVICE_PYTHON=sys.executable,
            PYTHONPATH=os.pathsep.join([str(out / 'python'), str(REPO)]),
        ),
    )
    assert result.returncode == 0, result.stdout[-6000:] + result.stderr[-3000:]
    assert tables(pg, profile_schema) == expected
