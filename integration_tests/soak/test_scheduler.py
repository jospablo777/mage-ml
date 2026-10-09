"""
Mage's scheduler running for a while with many concurrent pipelines.

The scheduler runs as in production, in its own process with a PostgreSQL metadata
database and Redis for locks, and creates runs from the pipelines' triggers: once at start
and every minute. While it runs, the test samples Mage's tables; then it deactivates the
triggers, waits for the runs to finish and checks every run.

MAGE_TEST_SOAK_SECONDS sets how long the triggers stay active (default 150, which crosses
two minute boundaries) and MAGE_TEST_SOAK_PIPELINES the number of chain pipelines.
"""
import datetime as dt
import os
import secrets
import signal
import subprocess
import sys
import time
from collections import Counter, defaultdict

import psycopg2
import pytest

from integration_tests.soak import project

SOAK_SECONDS = int(os.getenv('MAGE_TEST_SOAK_SECONDS', '150'))
CHAIN_PIPELINES = int(os.getenv('MAGE_TEST_SOAK_PIPELINES', '12'))
DRAIN_SECONDS = int(os.getenv('MAGE_TEST_SOAK_DRAIN_SECONDS', '300'))
FINAL = ('completed', 'failed', 'cancelled')


@pytest.fixture
def soak(postgres_settings, redis_url, tmp_path):
    suffix = secrets.token_hex(6)
    metadata_db = f'soak_meta_{suffix}'
    schema = f'soak_{suffix}'

    admin = psycopg2.connect(**postgres_settings)
    admin.autocommit = True
    with admin.cursor() as cursor:
        cursor.execute(f'CREATE DATABASE {metadata_db}')
        cursor.execute(f'CREATE SCHEMA {schema}')
        cursor.execute(
            f'CREATE TABLE {schema}.results (pipeline_uuid text, pipeline_run_id bigint, '
            'trigger_name text, total bigint, run_in_frame bigint, load_pid bigint, '
            'export_pid bigint, '
            'written_at timestamptz DEFAULT now())'
        )
        cursor.execute(f'CREATE TABLE {schema}.attempts (pipeline_run_id bigint)')

    root = tmp_path / 'project'
    start = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).strftime(
        '%Y-%m-%d %H:%M:%S',
    )
    triggers = project.write_project(root, CHAIN_PIPELINES, start)

    settings = postgres_settings
    env = {k: v for k, v in os.environ.items() if k != 'ENV'}
    env.update(
        MAGE_REPO_PATH=str(root),
        MAGE_DATA_DIR=str(tmp_path / 'data'),
        MAGE_DATABASE_CONNECTION_URL=(
            f"postgresql+psycopg2://{settings['user']}:{settings['password']}@"
            f"{settings['host']}:{settings['port']}/{metadata_db}"
        ),
        # Database 1 keeps the scheduler's locks apart from the Redis lock tests.
        REDIS_URL=redis_url.rsplit('/', 1)[0] + '/1',
        SCHEDULER_TRIGGER_INTERVAL='1',
        # The log is a file, so output is block-buffered and lost when the process stops.
        PYTHONUNBUFFERED='1',
        SOAK_SCHEMA=schema,
    )
    log_path = tmp_path / 'scheduler.log'
    log = log_path.open('w')
    process = subprocess.Popen(
        [sys.executable, '-c',
         'from mage_ai.server.scheduler_manager import run_scheduler; run_scheduler()'],
        cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
    )
    meta = psycopg2.connect(**dict(settings, dbname=metadata_db))
    meta.autocommit = True
    results = psycopg2.connect(**settings)
    results.autocommit = True

    yield dict(
        root=root, triggers=triggers, process=process, log_path=log_path,
        meta=meta, results=results, schema=schema,
    )

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


def query(connection, sql, params=None):
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


def runs(meta):
    return query(meta, '''
        SELECT pr.id, pr.pipeline_uuid, lower(pr.status::text), ps.name, pr.execution_date
        FROM pipeline_run pr JOIN pipeline_schedule ps ON ps.id = pr.pipeline_schedule_id
    ''')


def fanout_running_block_runs(meta):
    """The most block runs running at once in one fan-out pipeline run."""
    rows = query(meta, '''
        SELECT br.pipeline_run_id, COUNT(*) FROM block_run br
        JOIN pipeline_run pr ON pr.id = br.pipeline_run_id
        WHERE pr.pipeline_uuid = 'soak_fanout' AND lower(br.status::text) = 'running'
        GROUP BY br.pipeline_run_id
    ''')
    return max((count for _, count in rows), default=0)


def block_runs(meta, run_ids):
    """The block runs of some pipeline runs, for failure messages."""
    return query(
        meta,
        'SELECT pipeline_run_id, block_uuid, lower(status::text), started_at, completed_at '
        'FROM block_run WHERE pipeline_run_id = ANY(%s) ORDER BY pipeline_run_id, id',
        (list(run_ids),),
    )


def scheduler_log(soak) -> str:
    return soak['log_path'].read_text(errors='replace')


def wait_for_triggers(soak, timeout: int = 180) -> None:
    """Wait until the scheduler has migrated its database and synced every trigger."""
    expected = sum(len(triggers) for triggers in soak['triggers'].values())
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        assert soak['process'].poll() is None, scheduler_log(soak)[-5000:]
        try:
            if query(soak['meta'], 'SELECT COUNT(*) FROM pipeline_schedule')[0][0] >= expected:
                return
        except psycopg2.Error:
            pass  # Tables are missing or locked while the migrations run.
        time.sleep(1)
    raise AssertionError(f'triggers were not synced\n{scheduler_log(soak)[-5000:]}')


def test_scheduler_runs_every_pipeline_run_once(soak):
    meta, process = soak['meta'], soak['process']
    wait_for_triggers(soak)
    deadline = time.monotonic() + SOAK_SECONDS
    most_fanout_block_runs = 0
    most_active_runs = 0
    while time.monotonic() < deadline:
        assert process.poll() is None, scheduler_log(soak)[-5000:]
        most_fanout_block_runs = max(most_fanout_block_runs, fanout_running_block_runs(meta))
        most_active_runs = max(most_active_runs, sum(
            1 for run in runs(meta) if run[2] == 'running'
        ))
        time.sleep(0.5)

    for uuid, triggers in soak['triggers'].items():
        project.set_triggers(
            soak['root'], uuid, [dict(t, status='inactive') for t in triggers],
        )
    drain_deadline = time.monotonic() + DRAIN_SECONDS
    while time.monotonic() < drain_deadline:
        assert process.poll() is None, scheduler_log(soak)[-5000:]
        most_fanout_block_runs = max(most_fanout_block_runs, fanout_running_block_runs(meta))
        if all(run[2] in FINAL for run in runs(meta)):
            break
        time.sleep(1)

    all_runs = runs(meta)
    log = scheduler_log(soak)
    unfinished = [run for run in all_runs if run[2] not in FINAL]
    assert unfinished == [], (unfinished, block_runs(meta, [run[0] for run in unfinished]))

    # Every trigger ran once at start, and the minute trigger once per minute, without
    # two runs for one execution date.
    by_trigger = defaultdict(list)
    for _, pipeline_uuid, _, trigger, execution_date in all_runs:
        by_trigger[(pipeline_uuid, trigger)].append(execution_date)
    for uuid in soak['triggers']:
        assert len(by_trigger[(uuid, 'once')]) == 1, (uuid, by_trigger[(uuid, 'once')])
        minutes = by_trigger[(uuid, 'every_minute')]
        assert len(minutes) == len(set(minutes)), (uuid, sorted(minutes))
        assert len(minutes) >= SOAK_SECONDS // 60 - 1, (uuid, sorted(minutes))

    statuses = Counter((pipeline_uuid, status) for _, pipeline_uuid, status, _, _ in all_runs)
    for uuid in soak['triggers']:
        expected = 'failed' if uuid == 'soak_fail' else 'completed'
        other = {s for (p, s) in statuses if p == uuid and s != expected}
        assert not other, (uuid, statuses, log[-5000:])

    # Each completed run wrote its result once, with the value of its own frame.
    results = query(soak['results'], f'''
        SELECT pipeline_run_id, pipeline_uuid, total, run_in_frame
        FROM {soak['schema']}.results
    ''')
    written = Counter(row[0] for row in results)
    completed = {run[0]: run[1] for run in all_runs if run[2] == 'completed'}
    assert set(written) == set(completed)
    assert [run_id for run_id, count in written.items() if count != 1] == []
    for run_id, _, total, run_in_frame in results:
        expected = project.FANOUT_TOTAL if completed[run_id] == 'soak_fanout' else (
            project.CHAIN_TOTAL
        )
        assert (total, run_in_frame) == (expected, run_id), (run_id, completed[run_id])

    # With fusion, a chain's loader and exporter ran in the same process, as one stage;
    # without it, each block ran in its own process.
    pids = query(soak['results'], f'''
        SELECT load_pid, export_pid FROM {soak['schema']}.results WHERE load_pid IS NOT NULL
    ''')
    assert pids
    same = [load_pid == export_pid for load_pid, export_pid in pids]
    assert all(same) if os.getenv('MAGE_TEST_SOAK_FUSION') else not any(same), pids

    # The flaky loader failed once per run and passed on its retry.
    attempts = Counter(row[0] for row in query(
        soak['results'], f"SELECT pipeline_run_id FROM {soak['schema']}.attempts",
    ))
    retry_runs = [run[0] for run in all_runs if run[1] == 'soak_retry']
    assert {run_id: attempts[run_id] for run_id in retry_runs} == {
        run_id: 2 for run_id in retry_runs
    }

    assert most_fanout_block_runs <= project.FANOUT_BLOCK_RUN_LIMIT
    assert most_active_runs >= 2, 'no runs overlapped'
    assert process.poll() is None
