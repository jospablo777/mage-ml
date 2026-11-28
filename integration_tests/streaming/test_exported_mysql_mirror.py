"""
A MySQL database mirrored into PostgreSQL by an exported stream (ADR-0048): the change
capture source copies the tables' rows without a global lock, then reads the binary log from
the position read before the copy; the PostgreSQL sink keeps a table per source table
(mirror: upserts by key, deletes delete). The position is saved after the sinks: a restarted
service does not copy again, and the changes made while it was down arrive. A table added to
the source's tables is copied at the next start, then followed.
"""
import json
import signal
import time
import uuid
from pathlib import Path

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.streaming.test_exported_postgres_cdc import query, wait_for


def run(my, *statements):
    with my.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)


def make_pipeline(project, mysql_settings, database, postgres_settings, schema):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'mysql_mirror_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, settings, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language='yaml',
                             pipeline=pipeline)
        Path(block.file_path).write_text('\n'.join(
            f'{k}: {json.dumps(v)}' for k, v in settings.items()
        ))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', dict(
        connector_type='mysql', host=mysql_settings['host'], port=mysql_settings['port'],
        database=database, user=mysql_settings['user'],
        password="{{ env_var('MYSQL_PASSWORD') }}", sslmode='disable',
        tables=['orders', 'customers'], snapshot='initial', batch_size=1000,
        batch_timeout=0.2,
    ))
    add('sink', 'data_exporter', dict(
        connector_type='postgres', host=postgres_settings['host'],
        port=int(postgres_settings['port']), database=postgres_settings['dbname'],
        username=postgres_settings['user'], password="{{ env_var('PG_PASSWORD') }}",
        sslmode='disable', schema=schema, table='mirror_{_mage_table}',
        mirror=dict(key=['id']),
    ), [source])
    return pipeline


def rows(my, table, columns):
    with my.cursor() as cursor:
        cursor.execute(f'SELECT {columns} FROM {table} ORDER BY id')
        return [tuple(str(v) for v in row) for row in cursor.fetchall()]


def mirrored(pg, schema, table, columns):
    try:
        found = query(pg, f'SELECT {columns} FROM {schema}.mirror_{table} ORDER BY id')
    except Exception:
        # Not made yet.
        pg.rollback()
        return None
    return [tuple(str(v) for v in row) for row in found]


def test_a_mysql_database_is_copied_then_mirrored_and_a_restart_does_not_copy_again(
    mage_project, mysql_settings, mysql_database, my, postgres_settings, pg, schema, tmp_path,
):
    run(
        my,
        'CREATE TABLE orders (id INT PRIMARY KEY, item VARCHAR(40), qty INT, '
        'price DECIMAL(8,2))',
        'CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(40))',
        'CREATE TABLE ignored (id INT PRIMARY KEY)',
    )
    values = ', '.join(f"({i}, 'item {i}', {i}, {i}.50)" for i in range(1, 2501))
    run(my, f'INSERT INTO orders VALUES {values}',
        "INSERT INTO customers VALUES (7, 'ann'), (8, 'bo')")
    pipeline = make_pipeline(mage_project, mysql_settings, mysql_database, postgres_settings,
                             schema)
    env = dict(MYSQL_PASSWORD=mysql_settings['password'],
               PG_PASSWORD=postgres_settings['password'])
    columns = 'id, item, qty, price'
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        manifest = json.loads((directory / 'service.json').read_text())
        assert manifest['pipelines'][0]['stream']['source']['kind'] == 'mysql'
        binary = stream_service_binary()
        service = RunningService(binary, directory, tmp_path / 'data', **env)
        service.start()

        assert wait_for(lambda: mirrored(pg, schema, 'orders', columns)
                        == rows(my, 'orders', columns), timeout=120)
        assert wait_for(lambda: mirrored(pg, schema, 'customers', 'id, _name')
                        == rows(my, 'customers', 'id, name'), timeout=60)
        # Changes after the copy: inserts, updates, deletes, and a table not read.
        run(
            my,
            "INSERT INTO orders VALUES (2501, 'new', 1, 9.99)",
            'UPDATE orders SET qty = 99 WHERE id = 1',
            'DELETE FROM orders WHERE id = 2',
            'DELETE FROM customers WHERE id = 8',
            'INSERT INTO ignored VALUES (1)',
        )
        assert wait_for(lambda: mirrored(pg, schema, 'orders', columns)
                        == rows(my, 'orders', columns), timeout=60)
        assert wait_for(lambda: mirrored(pg, schema, 'customers', 'id, _name')
                        == rows(my, 'customers', 'id, name'), timeout=60)
        assert mirrored(pg, schema, 'ignored', 'id') is None
        saved = list((tmp_path / 'data').rglob('mysql-binlog-position.json'))
        assert len(saved) == 1, saved
        # Copied rows carry the position read before the copy; a second copy, a later one.
        stamp = f'SELECT min(_mage_binlog), max(_mage_binlog) FROM {schema}.mirror_orders ' \
                'WHERE id BETWEEN 4 AND 2500'
        copied_at = query(pg, stamp)
        assert copied_at[0][0] == copied_at[0][1], copied_at

        service.process.send_signal(signal.SIGTERM)
        assert service.process.wait(timeout=60) == 0
        # Changed while the service is down: read from the saved position, not copied again.
        run(my, "UPDATE orders SET item = 'later' WHERE id = 3")
        service = RunningService(binary, directory, tmp_path / 'data', **env)
        service.start()
        assert wait_for(lambda: mirrored(pg, schema, 'orders', columns)
                        == rows(my, 'orders', columns), timeout=60)
        time.sleep(2)
        assert query(pg, stamp) == copied_at, 'the rows were copied again'

        # A table added to the source's tables: copied at the next start, then followed.
        service.process.send_signal(signal.SIGTERM)
        assert service.process.wait(timeout=60) == 0
        run(my, 'INSERT INTO ignored VALUES (2), (3)', 'UPDATE orders SET qty = 7 WHERE id = 2501')
        path = directory / 'service.json'
        manifest = json.loads(path.read_text())
        manifest['pipelines'][0]['stream']['source']['config']['tables'].append('ignored')
        path.write_text(json.dumps(manifest))
        service = RunningService(binary, directory, tmp_path / 'data', **env)
        service.start()
        assert wait_for(lambda: mirrored(pg, schema, 'ignored', 'id')
                        == rows(my, 'ignored', 'id'), timeout=60)
        run(my, 'DELETE FROM ignored WHERE id = 1')
        assert wait_for(lambda: mirrored(pg, schema, 'ignored', 'id')
                        == rows(my, 'ignored', 'id'), timeout=60)
        assert wait_for(lambda: mirrored(pg, schema, 'orders', columns)
                        == rows(my, 'orders', columns), timeout=60)
        assert query(pg, stamp) == copied_at, 'the tables synced already were copied again'
        assert 'named since the last run' in service.log.read_text()
    except AssertionError:
        if service and service.log.is_file():
            print(service.log.read_text()[-6000:])
        print('mysql:', rows(my, 'orders', columns)[:3])
        print('mirror:', (mirrored(pg, schema, 'orders', columns) or [])[:3])
        raise
    finally:
        if service and service.process and service.process.poll() is None:
            service.stop()
        pipeline.delete()
