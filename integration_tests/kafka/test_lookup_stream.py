"""
A lookup block in an exported service: each order read from Kafka gets its product's row
from PostgreSQL, numbers with every digit, and an order whose product has no row goes on
with product null. As a temporal join (ADR-0043), each order gets the product's version
valid when it was ordered, from a history the sink's history mode wrote.
"""
import json
import textwrap
import uuid
from pathlib import Path

from kafka.admin import KafkaAdminClient, NewTopic

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.kafka.test_exactly_once import read_committed
from integration_tests.kafka.test_stream_conformance import wait_for

ORDERS = 500


LOOKUP_QUERY = """
          query: SELECT id, name, price FROM {schema}.products WHERE id::text = ANY($1)"""
LOOKUP_AS_OF = """
          history: {schema}.products_history
          as_of: ordered_at
          max_wait_seconds: 2"""


def make_pipeline(project, topics, schema, reads=LOOKUP_QUERY):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'lookup_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language='yaml')
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topics[0]}
        consumer_group: g_{name}
        auto_offset_reset: earliest
        batch_size: 100
        timeout_ms: 200
    ''')
    lookup = add('products', 'transformer', f'''
        lookup:
          connection:
            host: "{{{{ env_var('MAGE_TEST_POSTGRES_HOST') }}}}"
            port: "{{{{ env_var('MAGE_TEST_POSTGRES_PORT') }}}}"
            database: "{{{{ env_var('MAGE_TEST_POSTGRES_DBNAME') }}}}"
            username: "{{{{ env_var('MAGE_TEST_POSTGRES_USER') }}}}"
            password: "{{{{ env_var('MAGE_TEST_POSTGRES_PASSWORD') }}}}"
            sslmode: disable{reads.format(schema=schema)}
          key: product_id
          match: id
          into: product
    ''', [source])
    add('out', 'data_exporter', f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topics[1]}
    ''', [lookup])
    return pipeline


def test_each_order_gets_its_product(kafka_bootstrap, mage_project, pg, schema, tmp_path):
    from kafka import KafkaProducer

    with pg.cursor() as cursor:
        cursor.execute(f'CREATE TABLE {schema}.products (id BIGINT, name TEXT, '
                       'price NUMERIC(30, 10))')
        cursor.execute(f"INSERT INTO {schema}.products VALUES "
                       "(1, 'tea', 12345678901234567890.0123456789), (2, 'cake', 3.5)")
    pg.commit()
    topics = [f'it_orders_{uuid.uuid4().hex[:8]}', f'it_enriched_{uuid.uuid4().hex[:8]}']
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=2, replication_factor=1) for t in topics])
    admin.close()
    pipeline = make_pipeline(mage_project, topics, schema)
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        manifest = json.loads((directory / 'service.json').read_text())
        assert [b['language'] for b in manifest['pipelines'][0]['blocks']] == ['lookup']
        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap,
                                 value_serializer=lambda v: json.dumps(v).encode())
        for i in range(ORDERS):
            # Products 1 and 2 exist; 3 has no row.
            producer.send(topics[0], dict(order=i, product_id=i % 3 + 1))
        producer.flush()
        producer.close()
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=kafka_bootstrap)
        service.start()

        def enriched():
            assert service.process.poll() is None, service.log.read_text()[-4000:]
            found = read_committed(kafka_bootstrap, topics[1])
            return found if len(found) >= ORDERS else None

        orders = {o['order']: o for o in wait_for(enriched, timeout=180)}

        assert len(orders) == ORDERS
        assert orders[0]['product'] == {'id': 1, 'name': 'tea',
                                        'price': 12345678901234567890.0123456789}
        assert orders[1]['product']['name'] == 'cake'
        assert orders[2]['product'] is None
        # Read as text, the price keeps every digit.
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(bootstrap_servers=kafka_bootstrap, group_id=None)
        parts = [TopicPartition(topics[1], p) for p in (0, 1)]
        consumer.assign(parts)
        consumer.seek_to_beginning(*parts)
        raw = [r.value.decode() for rs in consumer.poll(timeout_ms=5000).values() for r in rs]
        consumer.close()
        assert any('12345678901234567890.0123456789' in text for text in raw)
    except AssertionError:
        if service and service.log.is_file():
            print(service.log.read_text()[-6000:])
        raise
    finally:
        if service and service.process and service.process.poll() is None:
            service.stop()
        pipeline.delete()


def test_each_order_gets_the_products_version_when_it_was_ordered(
    kafka_bootstrap, mage_project, postgres_settings, pg, schema, tmp_path,
):
    from kafka import KafkaProducer

    from mage_ai.streaming.sinks.postgres import PostgresSink

    history = PostgresSink(dict(
        connector_type='postgres', host=postgres_settings['host'],
        port=postgres_settings['port'], database=postgres_settings['dbname'],
        username=postgres_settings['user'], password=postgres_settings['password'],
        schema=schema, table='products_history', history=dict(key='id'),
    ))
    history.batch_write([
        dict(id=1, name='tea', price=10, _mage_operation='insert',
             _mage_commit_time='2026-11-01T00:00:00Z'),
        dict(id=1, name='tea', price=12, _mage_operation='update',
             _mage_commit_time='2026-11-03T00:00:00Z'),
        dict(id=2, name='cake', price=3, _mage_operation='insert',
             _mage_commit_time='2026-11-01T00:00:00Z'),
        dict(id=2, name='cake', price=3, _mage_operation='delete',
             _mage_commit_time='2026-11-04T00:00:00Z'),
    ])
    history.destroy()
    topics = [f'it_orders_{uuid.uuid4().hex[:8]}', f'it_enriched_{uuid.uuid4().hex[:8]}']
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in topics])
    admin.close()
    pipeline = make_pipeline(mage_project, topics, schema, reads=LOOKUP_AS_OF)
    orders = [
        dict(order=0, product_id=1, ordered_at='2026-11-02T10:00:00Z'),
        dict(order=1, product_id=1, ordered_at='2026-11-03T10:00:00Z'),
        dict(order=2, product_id=2, ordered_at='2026-11-02T00:00:00Z'),
        dict(order=3, product_id=2, ordered_at='2026-11-04T00:00:00Z'),
        dict(order=4, product_id=1, ordered_at='2026-10-01T00:00:00Z'),
    ]
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap,
                                 value_serializer=lambda v: json.dumps(v).encode())
        for order in orders:
            producer.send(topics[0], order)
        producer.flush()
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=kafka_bootstrap)
        service.start()

        def enriched(count):
            assert service.process.poll() is None, service.log.read_text()[-4000:]
            found = read_committed(kafka_bootstrap, topics[1])
            return found if len(found) >= count else None

        got = {o['order']: o['product'] for o in wait_for(lambda: enriched(5), timeout=180)}
        assert {k: v and v['price'] for k, v in got.items()} == {
            0: 10, 1: 12, 2: 3, 3: None, 4: None,
        }, got
        assert got[0]['valid_to'].startswith('2026-11-03')

        # Past the history's progress (11-04): the batch waits max_wait_seconds, then
        # joins the version there is.
        producer.send(topics[0], dict(order=5, product_id=1,
                                      ordered_at='2026-12-01T00:00:00Z'))
        producer.flush()
        producer.close()
        got = {o['order']: o['product'] for o in wait_for(lambda: enriched(6), timeout=60)}
        assert got[5]['price'] == 12
        assert 'is not through this batch' in service.log.read_text()
    except AssertionError:
        if service and service.log.is_file():
            print(service.log.read_text()[-6000:])
        raise
    finally:
        if service and service.process and service.process.poll() is None:
            service.stop()
        pipeline.delete()
