"""
A worker process killed in the middle of a block, in a real scheduler run (PostgreSQL
metadata database, Redis locks): the scheduler finds the block run's job gone, resets the
block run, and a new worker claims it with the next attempt. The block runs again, the
downstream exporter writes once, and the run completes. With block fusion the killed
process is a stage.
"""
import datetime as dt
import os
import secrets
import signal
import subprocess
import sys
import textwrap
import time

import psycopg2
import pytest
import yaml

from integration_tests.soak import project

SLOW_LOAD = project.CONNECT + '''
import time

import polars as pl

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(**kwargs):
    record('starts', pid=os.getpid())
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {os.environ['SOAK_SCHEMA']}.starts")
        starts = cursor.fetchone()[0]
    if starts == 1:
        # The test kills this process while it sleeps.
        time.sleep(120)
    return pl.DataFrame({'n': range(1000)}).with_columns(
        run=pl.lit(kwargs['pipeline_run_id']),
        pid=pl.lit(os.getpid()),
    )
'''


def write_project(root, fused: bool) -> None:
    (root / 'pipelines' / 'crash_chain').mkdir(parents=True)
    (root / 'metadata.yaml').write_text(yaml.safe_dump(dict(
        project_uuid='crash', variables_dir=str(root / 'mage_data'),
    )))
    (root / 'io_config.yaml').write_text('version: 0.1.1\ndefault: {}\n')
    blocks = dict(project.BLOCKS)
    blocks[('data_loaders', 'crash_load')] = SLOW_LOAD
    for (folder, uuid), code in blocks.items():
        (root / folder).mkdir(exist_ok=True)
        (root / folder / '__init__.py').touch()
        (root / folder / f'{uuid}.py').write_text(textwrap.dedent(code).lstrip())
    metadata = dict(
        blocks=project.chain('crash_load'), name='crash_chain', type='python',
        uuid='crash_chain',
    )
    if fused:
        metadata['block_fusion'] = 'chains'
    folder = root / 'pipelines' / 'crash_chain'
    (folder / 'metadata.yaml').write_text(yaml.safe_dump(metadata))
    (folder / '__init__.py').touch()
    start = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).strftime(
        '%Y-%m-%d %H:%M:%S',
    )
    project.set_triggers(root, 'crash_chain', [project.trigger('once', '@once', start)])


def query(connection, sql, params=None):
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


def wait_for(condition, seconds: float, what: str):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(0.5)
    raise AssertionError(f'Timed out waiting for {what}.')


@pytest.mark.skipif(
    bool(os.getenv('MAGE_TEST_SOAK_FUSION')),
    reason='Covers both modes itself; runs once, in the non-fusion soak pass.',
)
@pytest.mark.parametrize('fused', [False, True], ids=['block_by_block', 'fused'])
def test_a_killed_worker_is_replaced_and_the_exporter_writes_once(
    fused, postgres_settings, redis_url, tmp_path,
):
    suffix = secrets.token_hex(6)
    metadata_db, schema = f'crash_meta_{suffix}', f'crash_{suffix}'
    admin = psycopg2.connect(**postgres_settings)
    admin.autocommit = True
    with admin.cursor() as cursor:
        cursor.execute(f'CREATE DATABASE {metadata_db}')
        cursor.execute(f'CREATE SCHEMA {schema}')
        cursor.execute(
            f'CREATE TABLE {schema}.results (pipeline_uuid text, pipeline_run_id bigint, '
            'trigger_name text, total bigint, run_in_frame bigint, load_pid bigint, '
            'export_pid bigint, written_at timestamptz DEFAULT now())'
        )
        cursor.execute(f'CREATE TABLE {schema}.starts (pid bigint)')
    root = tmp_path / 'project'
    write_project(root, fused)
    settings = postgres_settings
    env = {k: v for k, v in os.environ.items() if k != 'ENV'}
    env.update(
        MAGE_REPO_PATH=str(root),
        MAGE_DATA_DIR=str(tmp_path / 'data'),
        MAGE_DATABASE_CONNECTION_URL=(
            f"postgresql+psycopg2://{settings['user']}:{settings['password']}@"
            f"{settings['host']}:{settings['port']}/{metadata_db}"
        ),
        REDIS_URL=redis_url.rsplit('/', 1)[0] + '/1',
        SCHEDULER_TRIGGER_INTERVAL='1',
        PYTHONUNBUFFERED='1',
        SOAK_SCHEMA=schema,
    )
    log = (tmp_path / 'scheduler.log').open('w')
    process = subprocess.Popen(
        [sys.executable, '-c',
         'from mage_ai.server.scheduler_manager import run_scheduler; run_scheduler()'],
        cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
    )
    meta = psycopg2.connect(**dict(settings, dbname=metadata_db))
    meta.autocommit = True
    results = psycopg2.connect(**settings)
    results.autocommit = True
    try:
        first = wait_for(
            lambda: query(results, f'SELECT pid FROM {schema}.starts'), 120,
            'the loader to start',
        )[0][0]
        os.kill(first, signal.SIGKILL)

        status = wait_for(
            lambda: [s for (s,) in query(
                meta, 'SELECT lower(status::text) FROM pipeline_run',
            ) if s in ('completed', 'failed', 'cancelled')],
            180, 'the run to finish',
        )
        log.flush()
        assert status == ['completed'], (tmp_path / 'scheduler.log').read_text()[-4000:]

        starts = [pid for (pid,) in query(results, f'SELECT pid FROM {schema}.starts')]
        assert len(starts) == 2 and starts[1] != first, starts
        written = query(results, f'SELECT total, load_pid FROM {schema}.results')
        assert written == [(project.CHAIN_TOTAL, starts[1])], written

        (attempt, crashes, block_status), = query(meta, '''
            SELECT attempt, (metrics->>'crashes')::int, lower(status::text)
            FROM block_run WHERE block_uuid = 'crash_load'
        ''')
        assert block_status == 'completed'
        assert crashes == 1
        assert attempt == 2, 'The replacement worker claims the next attempt.'
    finally:
        for connection in (meta, results):
            connection.close()
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
        log.close()
        with admin.cursor() as cursor:
            cursor.execute(
                'SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s',
                (metadata_db,),
            )
            cursor.execute(f'DROP DATABASE IF EXISTS {metadata_db}')
            cursor.execute(f'DROP SCHEMA {schema} CASCADE')
        admin.close()
