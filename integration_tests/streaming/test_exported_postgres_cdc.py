"""
An exported stream from PostgreSQL changes (logical replication) to a PostgreSQL table, run
by mage-service: values keep their types and digits, an update of another column reads the
unchanged large value from the table, a delete has the key, a stopped service continues after
the last change it handled and repeats nothing, and while the stream reads nothing of
interest, the slot follows the server's log.
"""
import json
import time
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)

# Text that does not compress, so PostgreSQL keeps it out of the row and an update of another
# column leaves it out of the log.
LARGE = "(SELECT string_agg(md5(i::text), '') FROM generate_series(1, 700) i)"


@pytest.fixture
def cdc(pg, schema):
    with pg.cursor() as cursor:
        cursor.execute(f'''
            CREATE TABLE {schema}.events (
                id integer PRIMARY KEY, name text, amount numeric(40, 10), flag boolean,
                created timestamptz, tags text[], doc jsonb, big bigint, body text, raw bytea
            );
            CREATE TABLE {schema}.other (id integer PRIMARY KEY);
            CREATE TABLE {schema}.changes (
                id integer, name text, amount numeric, flag boolean, created text,
                tags jsonb, doc jsonb, big numeric, body text, raw text,
                _mage_operation text, _mage_table text, _mage_lsn numeric,
                _mage_commit_time text, _mage_deleted_at text
            );
        ''')
    pg.commit()
    names = dict(slot=f'it_{uuid.uuid4().hex[:12]}', publication=f'it_{uuid.uuid4().hex[:12]}')
    yield names
    with pg.cursor() as cursor:
        cursor.execute(
            'SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots '
            'WHERE slot_name = %s AND active_pid IS NOT NULL', (names['slot'],),
        )
        for _ in range(50):
            cursor.execute('SELECT active FROM pg_replication_slots WHERE slot_name = %s',
                           (names['slot'],))
            row = cursor.fetchone()
            if row is None or not row[0]:
                break
            time.sleep(0.1)
        cursor.execute('SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots '
                       'WHERE slot_name = %s', (names['slot'],))
        cursor.execute(f"DROP PUBLICATION IF EXISTS {names['publication']}")
    pg.commit()


def make_pipeline(project, postgres_settings, schema, names):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'pg_cdc_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, settings, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language='yaml',
                             pipeline=pipeline)
        Path(block.file_path).write_text('\n'.join(
            f'{k}: {json.dumps(v)}' for k, v in settings.items()
        ))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    connection = dict(
        host=postgres_settings['host'], port=int(postgres_settings['port']),
        database=postgres_settings['dbname'], password="{{ env_var('PG_PASSWORD') }}",
        # TLS for the replication connection too; the server logs in with SCRAM.
        sslmode='require',
    )
    source = add('source', 'data_loader', dict(
        connector_type='postgres', user=postgres_settings['user'],
        replication_slot=names['slot'], publication_name=names['publication'],
        tables=[f'{schema}.events'], batch_size=100, batch_timeout=0.2, **connection,
    ))
    add('sink', 'data_exporter', dict(
        connector_type='postgres', username=postgres_settings['user'], schema=schema,
        # name and raw are reserved words, which MageML's sink prefixes with _.
        table='changes', allow_reserved_words=True, **connection,
    ), [source])
    return pipeline


def execute(pg, *statements):
    with pg.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)
    pg.commit()


def query(pg, sql):
    with pg.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    pg.rollback()
    return rows


def wait_for(predicate, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.2)
    return predicate()


def test_changes_reach_a_table_exactly_and_a_restart_continues(
    mage_project, postgres_settings, pg, schema, cdc, tmp_path,
):
    pipeline = make_pipeline(mage_project, postgres_settings, schema, cdc)
    count = f'SELECT count(*) FROM {schema}.changes'
    slot = f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{cdc['slot']}'"
    logs = []
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        environment = dict(PG_PASSWORD=postgres_settings['password'])
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 **environment)
        logs.append(service.log)
        service.start()
        try:
            service.wait_running()
            # The source makes the slot when it starts; changes before that are not read.
            assert wait_for(lambda: query(pg, slot)), 'the replication slot'
            execute(
                pg,
                f"INSERT INTO {schema}.events VALUES (1, 'it''s one', "
                "123456789012345678901234567890.0123456789, true, "
                "'2024-01-01 12:00:00.123456+00', '{a,\"b c\",NULL}', '{\"k\": [1, null]}', "
                f"9223372036854775807, {LARGE}, '\\x00ff')",
                f"INSERT INTO {schema}.events (id, name) VALUES (2, 'two')",
            )
            execute(
                pg,
                f"UPDATE {schema}.events SET name = 'one again' WHERE id = 1",
                f'DELETE FROM {schema}.events WHERE id = 2',
                f'INSERT INTO {schema}.events (id, name) '
                'SELECT i, \'n\' || i FROM generate_series(10, 509) i',
            )
            first = wait_for(lambda: query(pg, count)[0][0] >= 504)
            # Changes of other tables are not read, and the slot follows the log past them.
            execute(pg, f'INSERT INTO {schema}.other VALUES (1)')
            log_end = query(pg, 'SELECT pg_current_wal_lsn()')[0][0]
            followed = wait_for(lambda: query(
                pg, f"{slot} AND confirmed_flush_lsn >= '{log_end}'::pg_lsn",
            ), timeout=30)
        finally:
            service.stop()
        execute(pg, f"INSERT INTO {schema}.events (id, name) VALUES (1000, 'while stopped')")
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data2',
                                 **environment)
        logs.append(service.log)
        service.start()
        try:
            service.wait_running()
            second = wait_for(lambda: query(pg, count)[0][0] >= 505)
            time.sleep(2)
            stream = service.streams()[0]
        finally:
            service.stop()
    finally:
        pipeline.delete()

    # Both services' logs: a failure here once showed only the count.
    assert first and second, (query(pg, count), '\n'.join(
        f'--- {log.name}\n{log.read_text()[-4000:]}' for log in logs if log.exists()
    ))
    assert followed, (query(pg, slot), log_end)
    assert stream['last_error'] is None, stream
    rows = query(pg, f'''
        SELECT id, name, amount, flag, created, tags, doc, big, length(body), raw,
               _mage_operation, _mage_table
        FROM {schema}.changes WHERE id < 10 ORDER BY _mage_lsn
    ''')
    large = query(pg, f'SELECT length(body) FROM {schema}.events WHERE id = 1')[0][0]
    assert large > 20_000
    assert rows == [
        (1, "it's one", Decimal('123456789012345678901234567890.0123456789'), True,
         '2024-01-01T12:00:00.123456+00:00', ['a', 'b c', None], {'k': [1, None]},
         Decimal('9223372036854775807'), large, 'AP8=', 'insert', 'events'),
        (2, 'two', None, None, None, None, None, None, None, None, 'insert', 'events'),
        # The body was not in the log; it is read from the table.
        (1, 'one again', Decimal('123456789012345678901234567890.0123456789'), True,
         '2024-01-01T12:00:00.123456+00:00', ['a', 'b c', None], {'k': [1, None]},
         Decimal('9223372036854775807'), large, 'AP8=', 'update', 'events'),
        (2, None, None, None, None, None, None, None, None, None, 'delete', 'events'),
    ]
    deleted = query(pg, f"SELECT _mage_deleted_at FROM {schema}.changes "
                        "WHERE _mage_operation = 'delete'")
    assert deleted[0][0]
    # Every change once: nothing repeated after the restart.
    ids = [r[0] for r in query(pg, f'SELECT id FROM {schema}.changes WHERE id >= 10')]
    assert sorted(ids) == list(range(10, 510)) + [1000]
    commit_times = query(pg, f'SELECT DISTINCT _mage_commit_time FROM {schema}.changes')
    assert all(t[0].endswith('+00:00') for t in commit_times)


def test_a_missing_slot_without_create_slot_stops_with_the_reason(
    mage_project, postgres_settings, pg, schema, cdc, tmp_path,
):
    pipeline = make_pipeline(mage_project, postgres_settings, schema, cdc)
    source = next(b for b in pipeline.blocks_by_uuid.values() if b.type == 'data_loader')
    Path(source.file_path).write_text(Path(source.file_path).read_text() + '\ncreate_slot: false\n')
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 PG_PASSWORD=postgres_settings['password'])
        service.start()
        try:
            def failed():
                try:
                    found = service.streams()[0]
                except (OSError, IndexError):
                    return None
                return found if found.get('last_error') else None

            stream = wait_for(failed, timeout=30)
        finally:
            service.stop()
    finally:
        pipeline.delete()

    assert stream, 'the stream did not say why it stopped'
    assert cdc['slot'] in stream['last_error'], stream
    assert 'does not exist' in stream['last_error'], stream
