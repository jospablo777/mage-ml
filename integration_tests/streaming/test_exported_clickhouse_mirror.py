"""
A MySQL database mirrored into ClickHouse by an exported stream (ADR-0049): the copy and the
changes go into a ReplacingMergeTree per source table, versioned by each change's commit
time, so `SELECT ... FINAL` gives the source's rows: updated rows replaced, deleted keys
gone, decimals exact. A restarted service does not copy again.
"""
import json
import os
import signal
import time
import uuid
from decimal import Decimal
from pathlib import Path

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.streaming.test_exported_postgres_cdc import wait_for


def run(my, *statements):
    with my.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)


def make_pipeline(project, mysql_settings, database):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'ch_mirror_{uuid.uuid4().hex[:8]}'
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
        connector_type='clickhouse', profile='clickhouse',
        config=dict(table_name='copy_{_mage_table}'),
        mirror=dict(key=dict(orders=['id'], customers=['id'])),
    ), [source])
    return pipeline


def source_rows(my, table, columns):
    with my.cursor() as cursor:
        cursor.execute(f'SELECT {columns} FROM {table} ORDER BY id')
        return [tuple(Decimal(v) if isinstance(v, Decimal) else v for v in row)
                for row in cursor.fetchall()]


def mirrored(ch, table, columns):
    try:
        result = ch.query(f'SELECT {columns} FROM copy_{table} FINAL ORDER BY id')
    except Exception:
        # Not made yet.
        return None
    return [tuple(Decimal(str(v)) if isinstance(v, Decimal) else v for v in row)
            for row in result.result_rows]


def test_a_mysql_database_is_mirrored_into_clickhouse(
    mage_project, mysql_settings, mysql_database, my, clickhouse_settings,
    clickhouse_database, ch, tmp_path,
):
    run(
        my,
        'CREATE TABLE orders (id INT PRIMARY KEY, item VARCHAR(40), qty INT, '
        'price DECIMAL(12,2))',
        'CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(40))',
    )
    values = ', '.join(f"({i}, 'item {i}', {i}, {i}.25)" for i in range(1, 1001))
    run(my, f'INSERT INTO orders VALUES {values}',
        "INSERT INTO customers VALUES (7, 'ann'), (8, 'bo')")
    pipeline = make_pipeline(mage_project, mysql_settings, mysql_database)
    env = dict(
        MYSQL_PASSWORD=mysql_settings['password'],
        **{k: v for k, v in os.environ.items() if k.startswith('MAGE_TEST_CLICKHOUSE_')},
    )
    columns = 'id, item, qty, price'
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        manifest = json.loads((directory / 'service.json').read_text())
        sink = manifest['pipelines'][0]['stream']['sinks'][0]
        assert sink['kind'] == 'clickhouse' and sink['config']['mirror']
        binary = stream_service_binary()
        service = RunningService(binary, directory, tmp_path / 'data', **env)
        service.start()

        assert wait_for(lambda: mirrored(ch, 'orders', columns)
                        == source_rows(my, 'orders', columns), timeout=120)
        assert wait_for(lambda: mirrored(ch, 'customers', 'id, _name')
                        == source_rows(my, 'customers', 'id, name'), timeout=60)
        engine = ch.query(
            "SELECT engine_full FROM system.tables WHERE database = currentDatabase() "
            "AND name = 'copy_orders'"
        ).result_rows[0][0]
        assert engine.startswith('ReplacingMergeTree(_mage_version, _mage_is_deleted)'), engine
        assert 'ORDER BY id' in engine, engine

        # Changes after the copy: an insert, updates (one key twice), deletes.
        run(
            my,
            "INSERT INTO orders VALUES (1001, 'new', 1, 9.99)",
            'UPDATE orders SET qty = 99 WHERE id = 1',
            'UPDATE orders SET qty = 100 WHERE id = 1',
            'DELETE FROM orders WHERE id = 2',
            'DELETE FROM customers WHERE id = 8',
        )
        assert wait_for(lambda: mirrored(ch, 'orders', columns)
                        == source_rows(my, 'orders', columns), timeout=60)
        assert wait_for(lambda: mirrored(ch, 'customers', 'id, _name')
                        == source_rows(my, 'customers', 'id, name'), timeout=60)

        service.process.send_signal(signal.SIGTERM)
        assert service.process.wait(timeout=60) == 0
        copied = ch.query('SELECT count() FROM copy_orders WHERE _mage_version = 0')
        run(my, "UPDATE orders SET item = 'later' WHERE id = 3")
        service = RunningService(binary, directory, tmp_path / 'data', **env)
        service.start()
        assert wait_for(lambda: mirrored(ch, 'orders', columns)
                        == source_rows(my, 'orders', columns), timeout=60)
        time.sleep(2)
        again = ch.query('SELECT count() FROM copy_orders WHERE _mage_version = 0')
        assert again.result_rows == copied.result_rows, 'the rows were copied again'
    except AssertionError:
        if service and service.log.is_file():
            print(service.log.read_text()[-6000:])
        print('mysql:', source_rows(my, 'orders', columns)[:3])
        print('clickhouse:', (mirrored(ch, 'orders', columns) or [])[:3])
        raise
    finally:
        if service and service.process and service.process.poll() is None:
            service.stop()
        pipeline.delete()
