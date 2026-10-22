"""
Golden runs of the playground's batch pipelines (ROADMAP P0): the playground project
(playground/shop_lab) on the playground seed at a small scale, in a database of its own,
each pipeline run as `mage run` runs it and compared with its golden file
(integration_tests/playground/golden/<pipeline>.yaml): every block's outputs as data, and
the rows of the tables it exports.

    MAGE_GOLDEN_RECORD=1 pytest integration_tests/playground   # writes the golden files

The live pipelines read now() and are left out. Pipelines with R blocks run in the R test
environment (MAGE_TEST_R_PROJECT_DIR, `make -C integration_tests r-env`), and skip without it.
"""
import logging
import os
import re
import shutil
import subprocess
import sys
import types
from pathlib import Path

import psycopg2
import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
PLAYGROUND = REPO / 'playground' / 'shop_lab'
SEED = REPO / 'playground' / 'seed' / 'seed.py'
GOLDEN = Path(__file__).resolve().parent / 'golden'
DATABASE = 'playground_golden'
SCALE = '0.02'
RECORD = os.getenv('MAGE_GOLDEN_RECORD') == '1'
# Each pipeline, and the tables it exports: their rows are its effect counts.
PIPELINES = {
    'explore_web_events': [],
    'geo_store_coverage': ['geo_customer_coverage'],
    'pandas_sales': ['pandas_daily_revenue', 'pandas_category_margin'],
    'polars_customer_360': ['polars_customer_360'],
    'polyglot_churn': ['polyglot_churn_scores', 'polyglot_churn_summary'],
    'r_dplyr_cohorts': ['r_cohort_retention'],
    'rust_postgres_margins': ['rust_category_margin', 'rust_customer_value'],
    'rust_session_scores': ['rust_session_scores'],
    'shape_broadcast_web_events': [],
    'shape_diamond_store_scorecard': ['store_scorecard'],
    'shape_merge_customer_signals': ['customer_signals'],
}
# Pipelines with R blocks: they run in the R test environment (integration_tests/r_env).
WITH_R = {'polyglot_churn', 'r_dplyr_cohorts', 'shape_broadcast_web_events',
          'shape_diamond_store_scorecard'}


@pytest.fixture(scope='module')
def playground_db(postgres_settings):
    """The playground seed in its own database: the shared test database stays as it is."""
    admin = psycopg2.connect(**postgres_settings)
    admin.autocommit = True
    with admin.cursor() as cursor:
        cursor.execute('SELECT 1 FROM pg_database WHERE datname = %s', (DATABASE,))
        if cursor.fetchone() is None:
            cursor.execute(f'CREATE DATABASE {DATABASE}')
    admin.close()
    env = dict(
        os.environ,
        PGDATABASE=DATABASE,
        PGHOST=postgres_settings['host'],
        PGPASSWORD=postgres_settings['password'],
        PGPORT=str(postgres_settings['port']),
        PGUSER=postgres_settings['user'],
        SEED_SCALE=SCALE,
    )
    subprocess.run([sys.executable, str(SEED)], check=True, env=env)
    # The seed keeps a seeded schema as it is; one from another seed version or scale (a
    # database a local run left) is replaced, or the golden runs compare other rows.
    version = int(re.search(r'^SEED_VERSION = (\d+)', SEED.read_text(), re.M).group(1))
    if seeded(postgres_settings) != (version, float(SCALE)):
        subprocess.run([sys.executable, str(SEED), '--force'], check=True, env=env)
    assert seeded(postgres_settings) == (version, float(SCALE))
    with psycopg2.connect(**dict(postgres_settings, dbname=DATABASE)) as connection, \
            connection.cursor() as cursor:
        cursor.execute('CREATE SCHEMA IF NOT EXISTS analytics')
    return env


def seeded(postgres_settings):
    with psycopg2.connect(**dict(postgres_settings, dbname=DATABASE)) as connection, \
            connection.cursor() as cursor:
        cursor.execute('SELECT version, scale FROM shop.seed_info')
        version, scale = cursor.fetchone()
    return version, float(scale)


@pytest.fixture(scope='module')
def project(playground_db, monkeypatch_module):
    """
    A copy of the playground project. It shares the metadata database a session fixture
    (mage_runner) opened, and gives the repository path back when done.
    """
    from mage_ai.orchestration.db import db_connection
    from mage_ai.orchestration.db.database_manager import database_manager
    from mage_ai.settings.repo import get_repo_path, set_repo_path
    from mage_ai.tests.base_test import (
        drop_test_db,
        remove_test_directory,
        set_up_test_repo,
    )

    for key in ('PGDATABASE', 'PGHOST', 'PGPASSWORD', 'PGPORT', 'PGUSER'):
        monkeypatch_module.setenv(key, playground_db[key])
    r_project = os.getenv('MAGE_TEST_R_PROJECT_DIR')
    if r_project and shutil.which('Rscript') and shutil.which('rv'):
        monkeypatch_module.setenv('MAGE_R_PROJECT_DIR', str(Path(r_project).resolve()))
        monkeypatch_module.setenv('MAGE_R_SYNC', 'check')
    previous = get_repo_path()
    own_database = db_connection.session is None
    project = types.SimpleNamespace()
    set_up_test_repo(project)
    shutil.copytree(PLAYGROUND, project.repo_path, dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns('.file_versions', '__pycache__', 'target'))
    # The playground keeps its data at the container's path; the copy keeps it beside it.
    data = Path(project._test_directory) / 'mage_data'
    for metadata in Path(project.repo_path).rglob('metadata.yaml'):
        if not metadata.is_file():
            continue
        text = metadata.read_text()
        metadata.write_text(text.replace('/home/src/mage_data', str(data)))
    if own_database:
        database_manager.run_migrations(log_level=logging.ERROR)
        db_connection.start_session(force=True)
    yield project.repo_path
    set_repo_path(previous)
    if own_database:
        db_connection.close_session()
        drop_test_db()
    remove_test_directory(project._test_directory)


@pytest.fixture(scope='module')
def monkeypatch_module():
    patch = pytest.MonkeyPatch()
    yield patch
    patch.undo()


def effects(tables):
    return {
        table: dict(
            data_provider='postgres',
            profile='default',
            sql=f'SELECT count(*) FROM analytics.{table}',
        )
        for table in tables
    }


@pytest.mark.parametrize('pipeline_uuid', sorted(PIPELINES))
def test_the_pipeline_matches_its_golden_run(project, pipeline_uuid):
    from mage_ai.data_preparation.executors.pipeline_executor import PipelineExecutor
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.orchestration import golden

    if pipeline_uuid in WITH_R and not os.getenv('MAGE_R_PROJECT_DIR'):
        pytest.skip('R is not configured: MAGE_TEST_R_PROJECT_DIR, Rscript and rv')
    pipeline = Pipeline.get(pipeline_uuid, repo_path=project)
    PipelineExecutor(pipeline).execute(record_run=True, update_status=False)
    run = golden.latest_completed_run(pipeline)

    golden_file = GOLDEN / f'{pipeline_uuid}.yaml'
    target = Path(golden.golden_path(pipeline))
    target.parent.mkdir(parents=True, exist_ok=True)
    if RECORD:
        target.write_text(yaml.safe_dump(dict(effects=effects(PIPELINES[pipeline_uuid]))))
        golden.record(pipeline, run)
        GOLDEN.mkdir(exist_ok=True)
        shutil.copy(target, golden_file)
        return
    if not golden_file.exists():
        pytest.fail(f'No golden file for {pipeline_uuid}: run with MAGE_GOLDEN_RECORD=1.')
    shutil.copy(golden_file, target)
    comparison = golden.compare(pipeline, run)
    assert comparison['passed'], golden.format_comparison(comparison)
