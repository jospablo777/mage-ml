"""
Throughput of an exported stream (release build): Kafka in, a Python transformer, Kafka out,
for messages already waiting in the topic, so the service's own speed is measured.

- identity: the block returns its messages; what the engine, the worker protocol and the
  JSON round trip in Python cost;
- enrich: the block adds two fields to each message, a light block's work.

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
    deadline = time.time() + timeout
    total = 0
    while time.time() < deadline:
        total = sum(consumer.end_offsets(partitions).values())
        if total >= count:
            break
        time.sleep(0.05)
    consumer.close()
    return total


@pytest.mark.parametrize('block', sorted(BLOCKS))
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
        step = add('block', 'transformer', BLOCKS[block], 'python', [source])
        add('sink', 'data_exporter', f'''
            connector_type: kafka
            bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
            topic: {topics[1]}
        ''', 'yaml', [step])
        produce(kafka_bootstrap, topics[0], MESSAGES)
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        service = RunningService(binary, directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=kafka_bootstrap)
        service.start()
        try:
            service.wait_running()
            started = time.perf_counter()
            written = count_until(kafka_bootstrap, topics[1], MESSAGES, timeout=900)
            seconds = time.perf_counter() - started
        finally:
            service.stop()
    finally:
        pipeline.delete()
        admin.delete_topics(topics)

    assert written >= MESSAGES
    result = dict(block=block, messages=MESSAGES, seconds=round(seconds, 2),
                  messages_per_second=round(MESSAGES / seconds))
    print(f'\nstream benchmark: {json.dumps(result)}')
    if os.getenv('MAGE_BENCHMARK_RESULTS'):
        path = Path(os.environ['MAGE_BENCHMARK_RESULTS'])
        previous = json.loads(path.read_text()) if path.exists() else []
        path.write_text(json.dumps(previous + [result], indent=2))
