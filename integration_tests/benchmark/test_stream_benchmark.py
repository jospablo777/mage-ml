"""
Throughput of an exported stream (release build): Kafka in, a Python transformer, Kafka out,
for messages already waiting in the topic, so the service's own speed is measured.

- identity: the block returns its messages; what the engine, the worker protocol and the
  JSON round trip in Python cost;
- enrich: the block adds two fields to each message, a light block's work.

test_sink_throughput measures the sinks after the identity block: PostgreSQL, ClickHouse,
Parquet files, RabbitMQ and IBM MQ, each with its services' settings; it skips those not
running. test_source_throughput measures the RabbitMQ, IBM MQ and NATS sources, into Kafka,
from a queue their sink filled; test_postgres_change_throughput, PostgreSQL changes read by
logical replication.

MAGE_BENCHMARK_MESSAGES sets the messages (200,000 by default); MAGE_BENCHMARK_RESULTS names
a JSON file for the results.
"""
import json
import os
import textwrap
import time
import uuid
from pathlib import Path

import pytest

from integration_tests.exported_services import (
    RunningService,
    build_rust_blocks,
    export_service,
    stream_service_binary,
)

MESSAGES = int(os.getenv('MAGE_BENCHMARK_MESSAGES', '200000'))
BLOCKS = {
    'identity': '''
        @transformer
        def transform(messages, *args, **kwargs):
            return messages
    ''',
    'enrich': '''
        @transformer
        def transform(messages, *args, **kwargs):
            return [dict(m, total=m['price'] * m['units'], large=m['units'] > 5)
                    for m in messages]
    ''',
    # The same with Polars, as blocks that score a model on a table do.
    'polars_enrich': '''
        import polars as pl

        @transformer
        def transform(messages, *args, **kwargs):
            return pl.DataFrame(messages).with_columns(
                total=pl.col('price') * pl.col('units'), large=pl.col('units') > 5,
            )
    ''',
    # Takes a DataFrame: the engine sends the batch as JSON lines that Polars reads at once.
    'polars_frame': '''
        import polars as pl

        @transformer
        def transform(messages: pl.DataFrame, *args, **kwargs):
            return messages.with_columns(
                total=pl.col('price') * pl.col('units'), large=pl.col('units') > 5,
            )
    ''',
    # Rust stream blocks: one resident program, messages as raw bytes or JSON values.
    'rust_identity': '''
        use mage::prelude::*;

        fn transform(messages: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
            messages
        }
    ''',
    'rust_enrich': '''
        use mage::prelude::*;

        fn transform(messages: Vec<Value>) -> Result<Vec<Value>> {
            Ok(messages
                .into_iter()
                .map(|mut m| {
                    let price = m["price"].as_f64().unwrap_or(0.0);
                    let units = m["units"].as_f64().unwrap_or(0.0);
                    m["total"] = json!(price * units);
                    m["large"] = json!(units > 5.0);
                    m
                })
                .collect())
        }
    ''',
    # A RabbitMQ source hands blocks Mage's Payload(method, properties, body).
    'rabbitmq_body': '''
        import json

        @transformer
        def transform(messages, *args, **kwargs):
            return [json.loads(m.body) for m in messages]
    ''',
}


@pytest.fixture(scope='module')
def binary():
    return stream_service_binary(release=True)


def produce(bootstrap, topic, count):
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=bootstrap, linger_ms=20, batch_size=1 << 20)
    for i in range(count):
        producer.send(topic, json.dumps(dict(
            order_id=i, customer=f'c{i % 5000}', region='north', price=12.5 + i % 100,
            units=i % 9, at='2026-10-14T10:00:00Z',
        )).encode())
    producer.flush()
    producer.close()


def count_until(bootstrap, topic, count, timeout):
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
    partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic) or [0]]
    try:
        return _timed(lambda: sum(consumer.end_offsets(partitions).values()), count, timeout)
    finally:
        consumer.close()


def _timed(count, total, timeout, failed=lambda: False, every=0.01):
    """
    Polls count() until it reaches total: (count, seconds from the first message written to
    the last). The service's start and its first batch's wait are not measured. Counting a
    database or a queue often slows it: `every` spaces the counts.
    """
    deadline = time.time() + timeout
    written, first, checked = 0, None, time.time()
    while written < total and time.time() < deadline:
        written = count()
        if written and first is None:
            first = (time.perf_counter(), written)
        if time.time() - checked > 2:
            checked = time.time()
            if failed():
                break
        time.sleep(every)
    if first is None:
        return written, float('inf')
    # The messages already written at the first look took part of a poll interval.
    started, already = first
    seconds = time.perf_counter() - started
    return written, seconds * total / max(total - already, 1)


@pytest.mark.parametrize('block', ['enrich', 'identity', 'polars_enrich', 'polars_frame',
                                   'rust_enrich', 'rust_identity'])
def test_stream_throughput(block, mage_project, kafka_bootstrap, binary, tmp_path):
    from kafka.admin import KafkaAdminClient, NewTopic

    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    topics = [f'bench_in_{uuid.uuid4().hex[:8]}', f'bench_out_{uuid.uuid4().hex[:8]}']
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in topics])
    name = f'bench_{block}_{uuid.uuid4().hex[:6]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING,
                               repo_path=mage_project)
    try:
        def add(suffix, kind, code, language, upstream=()):
            created = Block.create(f'{name}_{suffix}', kind, mage_project, language=language,
                                   pipeline=pipeline)
            Path(created.file_path).write_text(textwrap.dedent(code))
            pipeline.add_block(created, upstream_block_uuids=[b.uuid for b in upstream])
            return created

        source = add('source', 'data_loader', f'''
            connector_type: kafka
            bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
            topic: {topics[0]}
            consumer_group: {name}
            auto_offset_reset: earliest
            batch_size: 5000
            timeout_ms: 100
        ''', 'yaml')
        language = 'rust' if block.startswith('rust_') else 'python'
        step = add('block', 'transformer', BLOCKS[block], language, [source])
        add('sink', 'data_exporter', f'''
            connector_type: kafka
            bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
            topic: {topics[1]}
        ''', 'yaml', [step])
        produce(kafka_bootstrap, topics[0], MESSAGES)
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        build_rust_blocks(directory)
        service = RunningService(binary, directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=kafka_bootstrap)
        service.start()
        try:
            written, seconds = count_until(kafka_bootstrap, topics[1], MESSAGES, timeout=900)
        finally:
            service.stop()
    finally:
        pipeline.delete()
        admin.delete_topics(topics)

    assert written >= MESSAGES
    result = dict(block=block, messages=MESSAGES, seconds=round(seconds, 2),
                  messages_per_second=round(MESSAGES / seconds))
    print(f'\nstream benchmark: {json.dumps(result)}')
    _record(result)


def _kafka_source(topic, group):
    return f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topic}
        consumer_group: {group}
        auto_offset_reset: earliest
        batch_size: 5000
        timeout_ms: 100
    '''


def _stream(mage_project, name, source_yaml, sink_yaml, block='identity'):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING,
                               repo_path=mage_project)

    def add(suffix, kind, code, language, upstream=()):
        created = Block.create(f'{name}_{suffix}', kind, mage_project, language=language,
                               pipeline=pipeline)
        Path(created.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(created, upstream_block_uuids=[b.uuid for b in upstream])
        return created

    source = add('source', 'data_loader', source_yaml, 'yaml')
    step = add('block', 'transformer', BLOCKS[block], 'python', [source])
    add('sink', 'data_exporter', sink_yaml, 'yaml', [step])
    return pipeline


def _run(binary, mage_project, pipeline, tmp_path, count, env):
    """Runs the stream until `count()` reaches MESSAGES: (messages, seconds, status)."""
    directory = export_service(mage_project, pipeline.uuid, tmp_path / pipeline.uuid)
    service = RunningService(binary, directory, tmp_path / pipeline.uuid / 'data', **env)
    service.start()
    try:
        written, seconds = _timed(
            count, MESSAGES, 900, lambda: service.streams()[0]['state'] == 'failed', every=0.1,
        )
        return written, seconds, service.streams()[0]
    finally:
        service.stop()


def _record(result):
    if os.getenv('MAGE_BENCHMARK_RESULTS'):
        path = Path(os.environ['MAGE_BENCHMARK_RESULTS'])
        previous = json.loads(path.read_text()) if path.exists() else []
        path.write_text(json.dumps(previous + [result], indent=2))


def _sink_case(sink, request, tmp_path):
    """The sink's YAML, how many rows it holds, and the variables the service needs."""
    if sink == 'postgres':
        settings = request.getfixturevalue('postgres_settings')
        import psycopg2

        schema = f'bench_{uuid.uuid4().hex[:8]}'

        def count():
            connection = psycopg2.connect(**settings)
            try:
                with connection.cursor() as cursor:
                    cursor.execute(f'SELECT count(*) FROM {schema}.orders')
                    return cursor.fetchone()[0]
            except psycopg2.Error:
                return 0
            finally:
                connection.close()
        yaml = (f'connector_type: postgres\nhost: "{settings["host"]}"\n'
                f'port: {settings["port"]}\ndatabase: {settings["dbname"]}\n'
                f'username: {settings["user"]}\npassword: "{{{{ env_var(\'PG_PASSWORD\') }}}}"\n'
                f'schema: {schema}\ntable: orders\nsslmode: disable\n')
        return yaml, count, dict(PG_PASSWORD=settings['password'])
    if sink == 'clickhouse':
        request.getfixturevalue('clickhouse_database')
        ch = request.getfixturevalue('ch')

        def count():
            try:
                return ch.command('SELECT count() FROM events')
            except Exception:
                return 0
        return ('connector_type: clickhouse\nprofile: clickhouse\nconfig:\n  table_name: events\n',
                count, {})
    if sink == 'parquet':
        folder = tmp_path / 'lake'

        def count():
            import pyarrow.parquet as pq

            return sum(pq.ParquetFile(f).metadata.num_rows for f in folder.rglob('*.parquet'))
        yaml = f'connector_type: object_storage\nurl: "file://{folder}"\nformat: parquet\n'
        return yaml, count, {}
    if sink == 'rabbitmq':
        settings = request.getfixturevalue('rabbitmq_settings')
        channel = request.getfixturevalue('rabbitmq_channel')
        queue = request.getfixturevalue('rabbitmq_queue')

        def count():
            return channel.queue_declare(queue, passive=True).method.message_count
        yaml = (f'connector_type: rabbitmq\nconnection_host: "127.0.0.1"\n'
                f'connection_port: {settings["connection_port"]}\n'
                f'username: {settings["username"]}\n'
                f'password: "{{{{ env_var(\'RABBIT_PASSWORD\') }}}}"\nqueue_name: {queue}\n')
        return yaml, count, dict(RABBIT_PASSWORD=settings['password'])
    if sink == 'ibm_mq':
        mq = request.getfixturevalue('ibm_mq')
        queue = mq.define(depth=max(MESSAGES, 100_000))
        request.addfinalizer(lambda: mq.delete(queue))
        libraries = os.path.dirname(mq.library)

        def count():
            return mq.depth(queue)[0]
        yaml = (f'connector_type: ibm_mq\nqueue_manager: QM1\nchannel: DEV.APP.SVRCONN\n'
                f'connection_name: "127.0.0.1({mq.port})"\nqueue: {queue}\nuser: {mq.user}\n'
                f'password: "{{{{ env_var(\'MQ_PASSWORD\') }}}}"\n')
        return yaml, count, dict(MQ_PASSWORD=mq.password, MAGE_SERVICE_MQ_LIBRARY=mq.library,
                                 LD_LIBRARY_PATH=libraries, DYLD_LIBRARY_PATH=libraries,
                                 MQ_OVERRIDE_DATA_PATH=str(tmp_path / 'mq-data'))
    if sink == 'nats':
        url = request.getfixturevalue('nats_url')
        from integration_tests.nats.test_streaming import jetstream

        stream = f'bench_{uuid.uuid4().hex[:8]}'
        request.addfinalizer(lambda: jetstream(url, lambda js: js.delete_stream(stream)))

        def count():
            async def action(js):
                return (await js.stream_info(stream)).state.messages

            try:
                return jetstream(url, action)
            except Exception:
                return 0
        yaml = (f'connector_type: nats_js\nserver_url: {url}\nstream_name: {stream}\n'
                f'subject: {stream}.events\n')
        return yaml, count, {}
    raise ValueError(sink)


@pytest.mark.parametrize('sink', ['clickhouse', 'ibm_mq', 'nats', 'parquet', 'postgres',
                                  'rabbitmq'])
def test_sink_throughput(sink, request, mage_project, kafka_bootstrap, binary, tmp_path):
    from kafka.admin import KafkaAdminClient, NewTopic

    yaml, count, env = _sink_case(sink, request, tmp_path)
    topic = f'bench_in_{uuid.uuid4().hex[:8]}'
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    name = f'bench_sink_{sink}_{uuid.uuid4().hex[:6]}'
    pipeline = _stream(mage_project, name, _kafka_source(topic, name), yaml)
    try:
        produce(kafka_bootstrap, topic, MESSAGES)
        written, seconds, status = _run(binary, mage_project, pipeline, tmp_path, count,
                                        dict(env, KAFKA_BOOTSTRAP=kafka_bootstrap))
    finally:
        pipeline.delete()
        admin.delete_topics([topic])

    assert written >= MESSAGES, status
    result = dict(sink=sink, messages=MESSAGES, seconds=round(seconds, 2),
                  messages_per_second=round(MESSAGES / seconds))
    print(f'\nsink benchmark: {json.dumps(result)}')
    _record(result)


@pytest.mark.parametrize('source,connections', [('ibm_mq', 1), ('ibm_mq', 4), ('nats', 1),
                                                ('rabbitmq', 1)])
def test_source_throughput(source, connections, request, mage_project, kafka_bootstrap, binary,
                           tmp_path):
    """
    The queue is filled by a stream into it, then read by a stream from it into Kafka. IBM MQ
    reads one message per round trip under syncpoint; `connections` read in parallel.
    """
    from kafka import KafkaConsumer, TopicPartition
    from kafka.admin import KafkaAdminClient, NewTopic

    queue_yaml, depth, env = _sink_case(source, request, tmp_path)
    env = dict(env, KAFKA_BOOTSTRAP=kafka_bootstrap)
    topics = [f'bench_fill_{uuid.uuid4().hex[:8]}', f'bench_out_{uuid.uuid4().hex[:8]}']
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in topics])
    name = f'bench_source_{source}_{uuid.uuid4().hex[:6]}'
    fill = _stream(mage_project, f'{name}_fill', _kafka_source(topics[0], name), queue_yaml)
    read = _stream(mage_project, f'{name}_read',
                   queue_yaml + ('batch_size: 5000\ntimeout: 0.1\nconsumer_name: bench\n'
                                 if source == 'nats' else 'batch_size: 5000\nbatch_timeout: 0.1\n')
                   + (f'connections: {connections}\n' if connections > 1 else ''),
                   f'''
                       connector_type: kafka
                       bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
                       topic: {topics[1]}
                   ''', block='rabbitmq_body' if source == 'rabbitmq' else 'identity')
    consumer = KafkaConsumer(bootstrap_servers=kafka_bootstrap)
    try:
        produce(kafka_bootstrap, topics[0], MESSAGES)
        filled, _, status = _run(binary, mage_project, fill, tmp_path, depth, env)
        assert filled >= MESSAGES, status

        def written():
            return sum(consumer.end_offsets([TopicPartition(topics[1], 0)]).values())
        written, seconds, status = _run(binary, mage_project, read, tmp_path, written, env)
    finally:
        consumer.close()
        fill.delete()
        read.delete()
        admin.delete_topics(topics)

    assert written >= MESSAGES, status
    result = dict(source=source, connections=connections, messages=MESSAGES,
                  seconds=round(seconds, 2),
                  messages_per_second=round(MESSAGES / seconds))
    print(f'\nsource benchmark: {json.dumps(result)}')
    _record(result)


def test_postgres_change_throughput(request, mage_project, kafka_bootstrap, binary, tmp_path):
    """
    PostgreSQL changes read by logical replication into Kafka: rows inserted in transactions
    of 10,000 into a table whose slot was made first, then read by the stream, so the reading
    is measured, not the inserts.
    """
    import psycopg2
    from kafka import KafkaConsumer, TopicPartition
    from kafka.admin import KafkaAdminClient, NewTopic

    settings = request.getfixturevalue('postgres_settings')
    schema = f'bench_{uuid.uuid4().hex[:8]}'
    slot = f'bench_{uuid.uuid4().hex[:8]}'
    pg = psycopg2.connect(**settings)
    pg.autocommit = True
    with pg.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA {schema}; CREATE TABLE {schema}.orders ('
                       'order_id bigint PRIMARY KEY, customer text, region text, '
                       'price numeric(10, 2), units integer, at timestamptz)')
        cursor.execute(f'CREATE PUBLICATION {slot} FOR TABLE {schema}.orders')
        cursor.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,))
        for start in range(0, MESSAGES, 10_000):
            cursor.execute(
                f"INSERT INTO {schema}.orders SELECT i, 'c' || i % 5000, 'north', "
                "12.5 + i % 100, i % 9, '2026-10-14T10:00:00Z' "
                f'FROM generate_series({start}, {min(start + 10_000, MESSAGES) - 1}) i'
            )
    topic = f'bench_out_{uuid.uuid4().hex[:8]}'
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    source = (f'connector_type: postgres\nhost: "{settings["host"]}"\n'
              f'port: {settings["port"]}\ndatabase: {settings["dbname"]}\n'
              f'user: {settings["user"]}\npassword: "{{{{ env_var(\'PG_PASSWORD\') }}}}"\n'
              f'sslmode: disable\nreplication_slot: {slot}\npublication_name: {slot}\n'
              f'tables: [{schema}.orders]\nbatch_size: 5000\nbatch_timeout: 0.1\n')
    pipeline = _stream(mage_project, f'bench_cdc_{uuid.uuid4().hex[:6]}', source, f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topic}
    ''')
    consumer = KafkaConsumer(bootstrap_servers=kafka_bootstrap)
    directory = export_service(mage_project, pipeline.uuid, tmp_path / pipeline.uuid)
    service = RunningService(binary, directory, tmp_path / pipeline.uuid / 'data',
                             PG_PASSWORD=settings['password'], KAFKA_BOOTSTRAP=kafka_bootstrap)
    try:
        service.start()
        written, seconds = _timed(
            lambda: sum(consumer.end_offsets([TopicPartition(topic, 0)]).values()), MESSAGES,
            900, lambda: service.streams()[0]['state'] == 'failed', every=0.1,
        )
        status = service.streams()[0]
    finally:
        service.stop()
        consumer.close()
        pipeline.delete()
        admin.delete_topics([topic])
        with pg.cursor() as cursor:
            cursor.execute('SELECT pg_drop_replication_slot(slot_name) FROM '
                           'pg_replication_slots WHERE slot_name = %s', (slot,))
            cursor.execute(f'DROP PUBLICATION IF EXISTS {slot}; DROP SCHEMA {schema} CASCADE')
        pg.close()

    assert written >= MESSAGES, status
    result = dict(source='postgres_changes', messages=MESSAGES, seconds=round(seconds, 2),
                  messages_per_second=round(MESSAGES / seconds))
    print(f'\nsource benchmark: {json.dumps(result)}')
    _record(result)
