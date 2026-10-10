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


class Scheduler:
    """Mage's scheduler process for a project, with its metadata database and results."""

    def __init__(self, postgres_settings, redis_url, tmp_path, fused: bool):
        suffix = secrets.token_hex(6)
        self.metadata_db, self.schema = f'crash_meta_{suffix}', f'crash_{suffix}'
        self.settings = postgres_settings
        self.tmp_path = tmp_path
        self.admin = psycopg2.connect(**postgres_settings)
        self.admin.autocommit = True
        with self.admin.cursor() as cursor:
            cursor.execute(f'CREATE DATABASE {self.metadata_db}')
            cursor.execute(f'CREATE SCHEMA {self.schema}')
            cursor.execute(
                f'CREATE TABLE {self.schema}.results (pipeline_uuid text, '
                'pipeline_run_id bigint, trigger_name text, total bigint, run_in_frame bigint, '
                'load_pid bigint, export_pid bigint, written_at timestamptz DEFAULT now())'
            )
            cursor.execute(f'CREATE TABLE {self.schema}.starts (pid bigint)')
        root = tmp_path / 'project'
        write_project(root, fused)
        settings = postgres_settings
        self.env = {k: v for k, v in os.environ.items() if k != 'ENV'}
        self.env.update(
            MAGE_REPO_PATH=str(root),
            MAGE_DATA_DIR=str(tmp_path / 'data'),
            MAGE_DATABASE_CONNECTION_URL=(
                f"postgresql+psycopg2://{settings['user']}:{settings['password']}@"
                f"{settings['host']}:{settings['port']}/{self.metadata_db}"
            ),
            REDIS_URL=redis_url.rsplit('/', 1)[0] + '/1',
            SCHEDULER_TRIGGER_INTERVAL='1',
            PYTHONUNBUFFERED='1',
            SOAK_SCHEMA=self.schema,
        )
        self.root = root
        self.log = (tmp_path / 'scheduler.log').open('a')
        self.process = None
        self.start()
        self.meta = psycopg2.connect(**dict(settings, dbname=self.metadata_db))
        self.meta.autocommit = True
        self.results = psycopg2.connect(**settings)
        self.results.autocommit = True

    def start(self):
        self.process = subprocess.Popen(
            [sys.executable, '-c',
             'from mage_ai.server.scheduler_manager import run_scheduler; run_scheduler()'],
            cwd=self.root, env=self.env, stdout=self.log, stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def kill(self):
        """The scheduler and its workers, at once, as a crashed host or a forced deploy."""
        os.killpg(self.process.pid, signal.SIGKILL)
        self.process.wait(timeout=30)

    def starts(self):
        return [pid for (pid,) in query(self.results, f'SELECT pid FROM {self.schema}.starts')]

    def wait_for_completion(self):
        status = wait_for(
            lambda: [s for (s,) in query(
                self.meta, 'SELECT lower(status::text) FROM pipeline_run',
            ) if s in ('completed', 'failed', 'cancelled')],
            180, 'the run to finish',
        )
        self.log.flush()
        assert status == ['completed'], (self.tmp_path / 'scheduler.log').read_text()[-4000:]

    def loader_block_run(self):
        (row,) = query(self.meta, """
            SELECT attempt, (metrics->>'crashes')::int, (metrics->>'interruptions')::int,
                lower(status::text)
            FROM block_run WHERE block_uuid = 'crash_load'
        """)
        return row

    def close(self):
        for connection in (self.meta, self.results):
            connection.close()
        if self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
        self.log.close()
        with self.admin.cursor() as cursor:
            cursor.execute(
                'SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s',
                (self.metadata_db,),
            )
            cursor.execute(f'DROP DATABASE IF EXISTS {self.metadata_db}')
            cursor.execute(f'DROP SCHEMA {self.schema} CASCADE')
        self.admin.close()


SKIP_IN_FUSION_PASS = pytest.mark.skipif(
    bool(os.getenv('MAGE_TEST_SOAK_FUSION')),
    reason='Covers both modes itself; runs once, in the non-fusion soak pass.',
)


@SKIP_IN_FUSION_PASS
@pytest.mark.parametrize('fused', [False, True], ids=['block_by_block', 'fused'])
def test_a_killed_worker_is_replaced_and_the_exporter_writes_once(
    fused, postgres_settings, redis_url, tmp_path,
):
    scheduler = Scheduler(postgres_settings, redis_url, tmp_path, fused)
    try:
        first = wait_for(scheduler.starts, 120, 'the loader to start')[0]
        os.kill(first, signal.SIGKILL)
        scheduler.wait_for_completion()

        starts = scheduler.starts()
        assert len(starts) == 2 and starts[1] != first, starts
        written = query(
            scheduler.results, f'SELECT total, load_pid FROM {scheduler.schema}.results',
        )
        assert written == [(project.CHAIN_TOTAL, starts[1])], written
        attempt, crashes, interruptions, status = scheduler.loader_block_run()
        assert (status, crashes, interruptions) == ('completed', 1, None)
        assert attempt == 2, 'The replacement worker claims the next attempt.'
    finally:
        scheduler.close()


@SKIP_IN_FUSION_PASS
def test_a_scheduler_restart_interrupts_a_block_without_counting_a_crash(
    postgres_settings, redis_url, tmp_path,
):
    """
    The scheduler and its worker are killed while a block runs, and a new scheduler starts:
    the block runs again as an interruption, not as a crash of the block.
    """
    scheduler = Scheduler(postgres_settings, redis_url, tmp_path, fused=False)
    try:
        wait_for(scheduler.starts, 120, 'the loader to start')
        scheduler.kill()
        scheduler.start()
        scheduler.wait_for_completion()

        assert len(scheduler.starts()) == 2
        written = query(scheduler.results, f'SELECT total FROM {scheduler.schema}.results')
        assert written == [(project.CHAIN_TOTAL,)], written
        attempt, crashes, interruptions, status = scheduler.loader_block_run()
        assert (status, crashes, interruptions, attempt) == ('completed', None, 1, 2)
    finally:
        scheduler.close()
