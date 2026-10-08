"""
Mage pipelines that read from PostgreSQL, transform, and write back.

The pipelines live in integration_tests/project. The loader checks what it fetched in
its @test functions, the transformer adds columns that SQL can compute the same way, and
the exporter writes every column back. Values cross Mage's variable storage between the
blocks. The expected table is built from the source with the same transformation in SQL.
"""

import logging
import shutil
import types
from pathlib import Path

import pytest

from integration_tests.data import postgres_dataset

PROJECT = Path(__file__).resolve().parents[1] / 'project'
DERIVED_COLUMNS = [
    'text_len',
    'not_bool',
    'int_diff',
    'date_year',
    'int_array_len',
    'json_kind',
    'bytea_len',
]
EXPECTED_SQL = """
CREATE TABLE {schema}.expected AS
SELECT
    s.*,
    char_length(c_text)::bigint AS text_len,
    NOT c_bool AS not_bool,
    c_integer::bigint - c_smallint::bigint AS int_diff,
    extract(year FROM c_date)::bigint AS date_year,
    cardinality(c_int_array)::bigint AS int_array_len,
    jsonb_typeof(c_jsonb) AS json_kind,
    octet_length(c_bytea)::bigint AS bytea_len
FROM {schema}.src s
"""


@pytest.fixture(scope='module')
def mage_project():
    """
    A Mage project in a temporary directory with its own metadata database, holding the
    pipelines from integration_tests/project.
    """
    from mage_ai.orchestration.db import db_connection
    from mage_ai.orchestration.db.database_manager import database_manager
    from mage_ai.tests.base_test import (
        drop_test_db,
        remove_test_directory,
        set_up_test_repo,
    )

    project = types.SimpleNamespace()
    set_up_test_repo(project)
    shutil.copytree(PROJECT, project.repo_path, dirs_exist_ok=True)
    database_manager.run_migrations(log_level=logging.ERROR)
    db_connection.start_session(force=True)
    yield project.repo_path
    db_connection.close_session()
    drop_test_db()
    remove_test_directory(project._test_directory)


def run_pipeline(pipeline_uuid, **variables):
    from mage_ai.data_preparation.executors.pipeline_executor import PipelineExecutor
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
    from mage_ai.orchestration.triggers.api import trigger_pipeline

    run = trigger_pipeline(pipeline_uuid, variables=variables)
    scheduler = PipelineScheduler(run)
    scheduler.start(should_schedule=False)
    PipelineExecutor(
        Pipeline.get(pipeline_uuid), execution_partition=run.execution_partition
    ).execute(
        pipeline_run_id=run.id,
        global_vars=run.get_variables(),
        allow_blocks_to_fail=False,
    )
    scheduler.schedule()
    run.refresh()

    statuses = {b.block_uuid: b.status.value for b in run.block_runs}
    assert set(statuses.values()) == {'completed'}, statuses
    assert run.status == PipelineRun.PipelineRunStatus.COMPLETED
    return run


def build_expected(pg, schema):
    with pg.cursor() as cursor:
        cursor.execute(EXPECTED_SQL.format(schema=schema))
    pg.commit()


@pytest.mark.parametrize('upsert', [False, True], ids=['replace', 'upsert'])
@pytest.mark.parametrize('engine', ['pandas', 'polars'])
def test_pipeline_round_trip(mage_project, pg, schema, source_table, source_rows, engine, upsert):
    build_expected(pg, schema)
    if upsert:
        # The upsert goes into an existing table with the source column types.
        postgres_dataset.create_like_table(pg, schema, 'expected', 'dst')
        with pg.cursor() as cursor:
            cursor.execute(f'ALTER TABLE {schema}.dst ADD PRIMARY KEY (id)')
        pg.commit()

    run_pipeline(
        f'postgres_{engine}',
        schema=schema,
        expected_rows=len(source_rows),
        upsert=upsert,
        if_exists='append' if upsert else 'replace',
    )

    columns = postgres_dataset.COLUMN_NAMES + DERIVED_COLUMNS
    assert postgres_dataset.mismatches(pg, schema, 'expected', 'dst', columns=columns) == {}
