"""
A database mirrored by an exported stream (ADR-0045): the change capture source copies the
tables' rows first (snapshot: initial), then streams their changes, and the PostgreSQL sink
keeps a table per source table (mirror: upserts by key, deletes delete). A restarted service
does not copy again, and the changes made while it was down arrive. A table added to the
source's tables joins the publication, is copied at the next start, then followed.
"""
import json
import signal
import time
import uuid
from pathlib import Path

import pytest

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.streaming.test_exported_postgres_cdc import (
    execute,
    query,
    wait_for,
)


@pytest.fixture
def source(pg, schema):
    execute(
        pg,
        f'CREATE TABLE {schema}.orders (id integer PRIMARY KEY, item text, qty integer)',
        f'CREATE TABLE {schema}.customers (id integer PRIMARY KEY, name text)',
        f"INSERT INTO {schema}.orders SELECT i, 'item ' || i, i FROM generate_series(1, 2500) i",
        f"INSERT INTO {schema}.customers VALUES (7, 'ann'), (8, 'bo')",
        f'CREATE TABLE {schema}.extra (id integer PRIMARY KEY, v text)',
        f"INSERT INTO {schema}.extra VALUES (1, 'a'), (2, 'b')",
    )
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

    name = f'pg_mirror_{uuid.uuid4().hex[:8]}'
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
        sslmode='disable',
    )
    source = add('source', 'data_loader', dict(
        connector_type='postgres', user=postgres_settings['user'],
        replication_slot=names['slot'], publication_name=names['publication'],
        tables=[f'{schema}.orders', f'{schema}.customers'], snapshot='initial',
        batch_size=1000, batch_timeout=0.2, **connection,
    ))
    add('sink', 'data_exporter', dict(
        connector_type='postgres', username=postgres_settings['user'], schema=schema,
        table='mirror_{_mage_table}', mirror=dict(key=['id']), **connection,
    ), [source])
    return pipeline


def same(pg, schema, table, columns):
    """The mirror holds exactly the source table's rows."""
    source = query(pg, f'SELECT {columns} FROM {schema}.{table} ORDER BY id')
    try:
        # name is an SQL word: MageML's sink writes it as _name.
        mirrored = columns.replace('name', '_name')
        mirror = query(pg, f'SELECT {mirrored} FROM {schema}.mirror_{table} ORDER BY id')
    except Exception:
        # Not made yet.
        pg.rollback()
        return False
    return source == mirror and source


def test_a_database_is_copied_then_mirrored_and_a_restart_does_not_copy_again(
    mage_project, postgres_settings, pg, schema, source, tmp_path,
):
    pipeline = make_pipeline(mage_project, postgres_settings, schema, source)
    service = None
    env = dict(PG_PASSWORD=postgres_settings['password'])
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        manifest = json.loads((directory / 'service.json').read_text())
        assert manifest['pipelines'][0]['stream']['source']['config']['snapshot'] == 'initial'
        binary = stream_service_binary()
        service = RunningService(binary, directory, tmp_path / 'data', **env)
        service.start()

        def mirror_count():
            try:
                return query(pg, f'SELECT count(*) FROM {schema}.mirror_orders')[0][0]
            except Exception:
                pg.rollback()
                return 0

        wait_for(lambda: mirror_count() == 2500, timeout=120)
        assert wait_for(lambda: same(pg, schema, 'customers', 'id, name'), timeout=60)
        # Changes after the copy: an insert, an update and a delete of each table.
        execute(
            pg,
            f"INSERT INTO {schema}.orders VALUES (2501, 'new', 1)",
            f"UPDATE {schema}.orders SET qty = 99 WHERE id = 1",
            f'DELETE FROM {schema}.orders WHERE id = 2',
            f'DELETE FROM {schema}.customers WHERE id = 8',
        )
        assert wait_for(lambda: same(pg, schema, 'orders', 'id, item, qty'), timeout=60)
        assert wait_for(lambda: same(pg, schema, 'customers', 'id, name'), timeout=60)
        records = list((tmp_path / 'data').rglob('initial-copy-*.done'))
        assert len(records) == 1, records
        # Copied rows carry the slot's position at the copy; a second copy would carry a
        # later one.
        stamp = f'SELECT min(_mage_lsn), max(_mage_lsn) FROM {schema}.mirror_orders ' \
                'WHERE id BETWEEN 4 AND 2500'
        copied_at = query(pg, stamp)
        assert copied_at[0][0] == copied_at[0][1], copied_at

        service.process.send_signal(signal.SIGTERM)
        assert service.process.wait(timeout=60) == 0
        # Changed while the service is down; the copy is not repeated after the restart.
        execute(pg, f"UPDATE {schema}.orders SET item = 'later' WHERE id = 3")
        service = RunningService(binary, directory, tmp_path / 'data', **env)
        service.start()
        assert wait_for(lambda: same(pg, schema, 'orders', 'id, item, qty'), timeout=60)
        time.sleep(2)
        assert query(pg, stamp) == copied_at, 'the rows were copied again'

        # A table added to the source's tables: published, copied at the next start, then
        # followed.
        service.process.send_signal(signal.SIGTERM)
        assert service.process.wait(timeout=60) == 0
        execute(pg, f"INSERT INTO {schema}.extra VALUES (3, 'c')")
        path = directory / 'service.json'
        manifest = json.loads(path.read_text())
        manifest['pipelines'][0]['stream']['source']['config']['tables'].append(
            f'{schema}.extra')
        path.write_text(json.dumps(manifest))
        service = RunningService(binary, directory, tmp_path / 'data', **env)
        service.start()
        assert wait_for(lambda: same(pg, schema, 'extra', 'id, v'), timeout=60)
        execute(pg, f"UPDATE {schema}.extra SET v = 'later' WHERE id = 1")
        assert wait_for(lambda: same(pg, schema, 'extra', 'id, v'), timeout=60)
        assert query(pg, stamp) == copied_at, 'the tables synced already were copied again'
        assert f'{schema}.extra' in records[0].read_text()
    except AssertionError:
        if service and service.log.is_file():
            print(service.log.read_text()[-6000:])
        pg.rollback()
        print('tables:', query(pg, "SELECT table_name, (xpath('/row/c/text()', query_to_xml("
                                   "format('SELECT count(*) AS c FROM %I.%I', table_schema, "
                                   f"table_name), false, true, '')))[1]::text FROM "
                                   f"information_schema.tables WHERE table_schema = '{schema}'"))
        try:
            print('streams:', json.dumps(service.streams(), indent=1)[:3000])
        except Exception as error:
            print('streams:', error)
        raise
    finally:
        if service and service.process and service.process.poll() is None:
            service.stop()
        pipeline.delete()
