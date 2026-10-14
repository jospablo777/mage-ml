"""
A Mage streaming pipeline (a Kafka source, a Python transformer, a Kafka sink) exported with
`mage export service` and run by mage-service built with Kafka, against Kafka 4.1: every
message is transformed once, positions are committed, and a restart neither loses nor
repeats messages.
"""
import json
import textwrap
import time
import uuid
from pathlib import Path

import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)

SOURCE = '''
connector_type: kafka
bootstrap_server: "{{ env_var('KAFKA_BOOTSTRAP') }}"
topic: %(topic)s
consumer_group: %(group)s
auto_offset_reset: earliest
batch_size: 50
timeout_ms: 200
'''
TRANSFORM = '''
import time
from typing import Dict, List


@transformer
def double(messages: List[Dict], *args, **kwargs):
    # Slow enough that batches are in flight when partitions move between instances.
    time.sleep(float(kwargs.get('delay', 0)))
    if kwargs.get('identity'):
        return messages
    for m in messages:
        if m['id'] in kwargs.get('reject', []):
            raise ValueError(f"message {m['id']} is not accepted")
    return [dict(m, doubled=m['amount'] * 2) for m in messages if m['id'] % 10 != 0]
'''
SINK = '''
connector_type: kafka
bootstrap_server: "{{ env_var('KAFKA_BOOTSTRAP') }}"
topic: %(topic)s
'''


@pytest.fixture
def topics(kafka_bootstrap):
    names = [f'it_in_{uuid.uuid4().hex[:10]}', f'it_out_{uuid.uuid4().hex[:10]}']
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(n, num_partitions=2, replication_factor=1) for n in names])
    yield names
    admin.delete_topics(names)


@pytest.fixture
def stream_pipeline(mage_project, topics):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'kafka_stream_{uuid.uuid4().hex[:8]}'
    group = f'{name}_group'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING,
                               repo_path=mage_project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, mage_project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', SOURCE % dict(topic=topics[0], group=group), 'yaml')
    double = add('double', 'transformer', TRANSFORM, 'python', [source])
    add('sink', 'data_exporter', SINK % dict(topic=topics[1]), 'yaml', [double])
    yield pipeline, group
    pipeline.delete()


def produce(bootstrap, topic, ids):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: str(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    for i in ids:
        producer.send(topic, key=i, value=dict(id=i, amount=i))
    producer.flush()
    producer.close()


def consume(consumer, count, timeout=120):
    messages = []
    deadline = time.time() + timeout
    while len(messages) < count and time.time() < deadline:
        for records in consumer.poll(timeout_ms=500).values():
            messages.extend(json.loads(r.value) for r in records)
    return messages


def committed(bootstrap, group, topic) -> int:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    offsets = admin.list_consumer_group_offsets(group)
    return sum(meta.offset for part, meta in offsets.items() if part.topic == topic)


def test_an_exported_kafka_stream_transforms_each_message_once_across_a_restart(
    mage_project, stream_pipeline, topics, kafka_bootstrap, tmp_path,
):
    pipeline, group = stream_pipeline
    directory = export_service(mage_project, pipeline.uuid, tmp_path)
    service = RunningService(
        stream_service_binary(), directory, tmp_path / 'data', KAFKA_BOOTSTRAP=kafka_bootstrap,
    )
    output = KafkaConsumer(
        topics[1], bootstrap_servers=kafka_bootstrap, auto_offset_reset='earliest',
        group_id=f'reader_{uuid.uuid4().hex[:8]}', consumer_timeout_ms=1000,
    )

    produce(kafka_bootstrap, topics[0], range(200))
    service.start()
    try:
        service.wait_running()
        first = consume(output, 180)
    finally:
        service.stop()

    assert sorted(m['id'] for m in first) == [i for i in range(200) if i % 10]
    assert all(m['doubled'] == m['amount'] * 2 for m in first)
    # A graceful stop commits every position it processed.
    assert committed(kafka_bootstrap, group, topics[0]) == 200

    produce(kafka_bootstrap, topics[0], range(200, 250))
    service.start()
    try:
        status = service.wait_running()
        second = consume(output, 45)
        time.sleep(2)
        second += consume(output, 1, timeout=2)
    finally:
        service.stop()

    assert status['last_error'] is None
    assert sorted(m['id'] for m in second) == [i for i in range(200, 250) if i % 10]
    assert committed(kafka_bootstrap, group, topics[0]) == 250


def test_partitions_move_between_instances_without_losing_or_repeating_messages(
    mage_project, stream_pipeline, topics, kafka_bootstrap, tmp_path,
):
    """
    A second instance joins the group, then the first leaves, while messages arrive and
    batches are in flight: no message is lost or written twice. A revoke lets the partition's
    batches finish and commits them before the partition moves (I-34), so the next owner
    neither repeats a batch whose write was under way nor redoes finished work.
    """
    pipeline, _ = stream_pipeline
    pipeline.variables = dict(delay=0.2)
    pipeline.save()
    directory = export_service(mage_project, pipeline.uuid, tmp_path)
    binary = stream_service_binary()
    first = RunningService(
        binary, directory, tmp_path / 'data-a', KAFKA_BOOTSTRAP=kafka_bootstrap,
    )
    second = RunningService(
        binary, directory, tmp_path / 'data-b', KAFKA_BOOTSTRAP=kafka_bootstrap,
    )
    output = KafkaConsumer(
        topics[1], bootstrap_servers=kafka_bootstrap, auto_offset_reset='earliest',
        group_id=f'reader_{uuid.uuid4().hex[:8]}', consumer_timeout_ms=1000,
    )
    received = []
    first.start()
    try:
        first.wait_running()
        produce(kafka_bootstrap, topics[0], range(0, 300))
        received += consume(output, 50)
        second.start()
        second.wait_running()
        produce(kafka_bootstrap, topics[0], range(300, 600))
        received += consume(output, 100)
        first.stop()
        produce(kafka_bootstrap, topics[0], range(600, 800))
        expected = [i for i in range(800) if i % 10]
        received += consume(output, len(expected) - len(received), timeout=180)
        time.sleep(3)
        received += consume(output, 1, timeout=3)
    finally:
        for service in (first, second):
            if service.process and service.process.poll() is None:
                service.stop()

    ids = [m['id'] for m in received]
    repeated = sorted({i for i in ids if ids.count(i) > 1})
    assert repeated == [], f'{len(repeated)} messages written twice: {repeated[:20]}'
    assert sorted(ids) == expected


def test_a_killed_service_loses_nothing_and_repeats_at_most_its_batches_in_flight(
    mage_project, stream_pipeline, topics, kafka_bootstrap, tmp_path,
):
    """
    kill -9 in the middle of a stream: no shutdown, no last commit. At the restart the
    stream reads again from the last committed positions, so every message is written, and
    only what was in flight at the kill (a batch per partition at most here, 50 messages
    each) is written twice.
    """
    pipeline, _ = stream_pipeline
    pipeline.variables = dict(delay=0.2)
    pipeline.save()
    directory = export_service(mage_project, pipeline.uuid, tmp_path)
    service = RunningService(
        stream_service_binary(), directory, tmp_path / 'data', KAFKA_BOOTSTRAP=kafka_bootstrap,
    )
    output = KafkaConsumer(
        topics[1], bootstrap_servers=kafka_bootstrap, auto_offset_reset='earliest',
        group_id=f'reader_{uuid.uuid4().hex[:8]}', consumer_timeout_ms=1000,
    )
    expected = [i for i in range(600) if i % 10]
    produce(kafka_bootstrap, topics[0], range(600))
    service.start()
    try:
        service.wait_running()
        received = consume(output, 100)
        service.process.kill()
        service.process.wait(timeout=30)
        service.start()
        service.wait_running()
        received += consume(output, len(expected) - len(received), timeout=180)
        time.sleep(3)
        received += consume(output, 1, timeout=3)
    finally:
        if service.process.poll() is None:
            service.stop()

    ids = [m['id'] for m in received]
    assert sorted(set(ids)) == expected
    # In flight at the kill: at most a batch of 50 messages per partition, 45 kept each.
    assert len(ids) - len(set(ids)) <= 2 * 45, f'{len(ids) - len(set(ids))} repeated'


# Values a JSON round trip through 64-bit numbers or lossy text would change.
EXACT = [
    b'{"id": 1, "big": 123456789012345678901234567890, "neg": -9223372036854775809}',
    b'{"id": 2, "money": 0.1, "tiny": 5e-324, "huge": 1.7976931348623157e308}',
    '{"id": 3, "text": "\u00f1and\u00fa \u6f22\u5b57 \U0001f680", "nested": {"a": [1, null, true]}}'
    .encode(),
    'plain text, not JSON: \u00e9t\u00e9'.encode(),
]


def raw_producer(bootstrap):
    return KafkaProducer(bootstrap_servers=bootstrap)


def test_message_values_pass_through_exactly(
    mage_project, stream_pipeline, topics, kafka_bootstrap, tmp_path,
):
    """
    Integers past 64 bits, the extremes of floats, Unicode and text that is not JSON come
    out of a stream whose block returns its messages as they are: the engine passes values
    to Python unparsed and writes what Python returns byte for byte.
    """
    pipeline, _ = stream_pipeline
    pipeline.variables = dict(identity=True)
    pipeline.save()
    directory = export_service(mage_project, pipeline.uuid, tmp_path)
    service = RunningService(
        stream_service_binary(), directory, tmp_path / 'data', KAFKA_BOOTSTRAP=kafka_bootstrap,
    )
    output = KafkaConsumer(
        topics[1], bootstrap_servers=kafka_bootstrap, auto_offset_reset='earliest',
        group_id=f'reader_{uuid.uuid4().hex[:8]}', consumer_timeout_ms=1000,
    )
    producer = raw_producer(kafka_bootstrap)
    for value in EXACT:
        producer.send(topics[0], value=value, partition=0)
    producer.flush()
    service.start()
    try:
        service.wait_running()
        received = []
        deadline = time.time() + 120
        while len(received) < len(EXACT) and time.time() < deadline:
            for records in output.poll(timeout_ms=500).values():
                received.extend(r.value for r in records)
    finally:
        service.stop()

    expected = [json.loads(v) for v in EXACT[:3]] + [EXACT[3].decode()]
    # Messages without a key spread over the output topic's partitions, which keep no order
    # between them.
    key = json.dumps
    assert sorted((json.loads(v) for v in received), key=key) == sorted(expected, key=key)
    first = next(v for v in map(json.loads, received) if isinstance(v, dict) and v['id'] == 1)
    assert first['big'] == 123456789012345678901234567890


def test_a_binary_message_stops_the_stream_without_being_rewritten(
    mage_project, stream_pipeline, topics, kafka_bootstrap, tmp_path,
):
    """
    A value that is neither JSON nor UTF-8 text is not turned into replacement characters:
    the batch fails, and with no dead-letter sink the stream stops with the reason and keeps
    its position, so nothing after it is skipped.
    """
    pipeline, group = stream_pipeline
    pipeline.variables = dict(identity=True)
    pipeline.save()
    directory = export_service(mage_project, pipeline.uuid, tmp_path)
    service = RunningService(
        stream_service_binary(), directory, tmp_path / 'data', KAFKA_BOOTSTRAP=kafka_bootstrap,
    )
    producer = raw_producer(kafka_bootstrap)
    producer.send(topics[0], value=b'\xff\xfe\x00binary', partition=0)
    producer.flush()
    service.start()
    try:
        service.wait_running()
        deadline = time.time() + 120
        status = None
        while time.time() < deadline:
            status = service.streams()[0]
            if status['state'] == 'failed':
                break
            time.sleep(0.5)
    finally:
        service.stop()

    assert status['state'] == 'failed', status
    assert 'neither JSON nor UTF-8 text' in status['last_error']
    assert committed(kafka_bootstrap, group, topics[0]) == 0


def test_a_paused_stream_reads_nothing_until_it_resumes_and_loses_nothing(
    mage_project, stream_pipeline, topics, kafka_bootstrap, tmp_path,
):
    """
    Pausing from the control API (the console's p) stops reading while the consumer stays in
    its group; resuming reads everything that arrived meanwhile.
    """
    pipeline, _ = stream_pipeline
    directory = export_service(mage_project, pipeline.uuid, tmp_path)
    service = RunningService(
        stream_service_binary(), directory, tmp_path / 'data', KAFKA_BOOTSTRAP=kafka_bootstrap,
    )
    output = KafkaConsumer(
        topics[1], bootstrap_servers=kafka_bootstrap, auto_offset_reset='earliest',
        group_id=f'reader_{uuid.uuid4().hex[:8]}', consumer_timeout_ms=1000,
    )
    service.start()
    try:
        service.wait_running()
        time.sleep(3)  # the group assigns the partitions
        service.control('pause')
        produce(kafka_bootstrap, topics[0], range(100))
        time.sleep(5)
        while_paused = consume(output, 1, timeout=2)
        snapshot = service.get('/v1/snapshot')['pipelines'][0]
        service.control('resume')
        after = consume(output, 90)
    finally:
        service.stop()

    assert while_paused == []
    assert snapshot['status'] == 'paused'
    assert snapshot['stream']['state'] == 'paused'
    assert sorted(m['id'] for m in after) == [i for i in range(100) if i % 10]


def test_backpressure_does_not_stall_the_stream(mage_project, kafka_bootstrap, tmp_path):
    """
    A stream that reads faster than it writes is paused and resumed every few batches. It
    used to pause in librdkafka, which fetched again up to 0.8 seconds after each resume, so
    the stream stood still most of the time (I-78); now it writes without such gaps.
    """
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'kafka_pressure_{uuid.uuid4().hex[:8]}'
    topics = [f'{name}_in', f'{name}_out']
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in topics])
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING,
                               repo_path=mage_project)
    try:
        blocks = [
            ('source', 'data_loader', 'yaml', (SOURCE % dict(topic=topics[0], group=name))
             .replace('batch_size: 50', 'batch_size: 5000')),
            ('pass', 'transformer', 'python',
             '@transformer\ndef identity(messages, *args, **kwargs):\n    return messages\n'),
            ('sink', 'data_exporter', 'yaml', SINK % dict(topic=topics[1])),
        ]
        upstream = []
        for suffix, kind, language, code in blocks:
            block = Block.create(f'{name}_{suffix}', kind, mage_project, language=language,
                                 pipeline=pipeline)
            Path(block.file_path).write_text(code)
            pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
            upstream = [block]
        # Large enough that librdkafka's queue, sized from the stream's byte budget, fills.
        count = 200_000
        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap, linger_ms=20,
                                 batch_size=1 << 20)
        for i in range(count):
            producer.send(topics[0], json.dumps(dict(id=i, note='x' * 100)).encode())
        producer.flush()
        producer.close()
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        # A release build: a debug engine reads too slowly to run ahead of its sink.
        service = RunningService(stream_service_binary(release=True), directory,
                                 tmp_path / 'data', KAFKA_BOOTSTRAP=kafka_bootstrap)
        consumer = KafkaConsumer(bootstrap_servers=kafka_bootstrap)
        partitions = [TopicPartition(topics[1], 0)]
        service.start()
        try:
            # The longest time without a new message written, from the first one: a pause
            # that stalls the stream shows here, whatever the machine's speed.
            written, stalled, last, deadline = 0, 0.0, None, time.time() + 120
            while written < count and time.time() < deadline:
                now = sum(consumer.end_offsets(partitions).values())
                at = time.perf_counter()
                if now > written:
                    if last is not None:
                        stalled = max(stalled, at - last)
                    written, last = now, at
                time.sleep(0.01)
            consumer.close()
        finally:
            service.stop()
    finally:
        pipeline.delete()
        admin.delete_topics(topics)

    assert written == count
    assert stalled < 0.4, f'the stream wrote nothing for {stalled:.2f} seconds'


def test_messages_that_fail_on_their_own_go_to_the_dead_letter_topic(
    mage_project, stream_pipeline, topics, kafka_bootstrap, tmp_path,
):
    """
    A dead-letter sink takes the messages a block fails on, each with its error, and the
    rest of their batch reaches the output topic; every position is committed.
    """
    from mage_ai.data_preparation.models.block import Block

    pipeline, group = stream_pipeline
    pipeline.variables = dict(reject=[3, 17, 41])
    pipeline.save()
    dead_topic = f'it_dead_{uuid.uuid4().hex[:10]}'
    KafkaAdminClient(bootstrap_servers=kafka_bootstrap).create_topics(
        [NewTopic(dead_topic, num_partitions=1, replication_factor=1)],
    )
    source = pipeline.get_block(f'{pipeline.uuid}_source')
    dead = Block.create(f'{pipeline.uuid}_dead', 'data_exporter', mage_project,
                        language='yaml', pipeline=pipeline)
    Path(dead.file_path).write_text(textwrap.dedent(f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {dead_topic}
        dead_letter: true
    '''))
    pipeline.add_block(dead, upstream_block_uuids=[source.uuid])
    directory = export_service(mage_project, pipeline.uuid, tmp_path)
    service = RunningService(
        stream_service_binary(), directory, tmp_path / 'data', KAFKA_BOOTSTRAP=kafka_bootstrap,
    )
    output = KafkaConsumer(
        topics[1], bootstrap_servers=kafka_bootstrap, auto_offset_reset='earliest',
        group_id=f'reader_{uuid.uuid4().hex[:8]}', consumer_timeout_ms=1000,
    )
    dead_letters = KafkaConsumer(
        dead_topic, bootstrap_servers=kafka_bootstrap, auto_offset_reset='earliest',
        group_id=f'reader_{uuid.uuid4().hex[:8]}', consumer_timeout_ms=1000,
    )
    produce(kafka_bootstrap, topics[0], range(60))
    expected = [i for i in range(60) if i % 10 and i not in (3, 17, 41)]
    service.start()
    try:
        service.wait_running()
        delivered = consume(output, len(expected))
        rejected = []
        deadline = time.time() + 60
        while len(rejected) < 3 and time.time() < deadline:
            for records in dead_letters.poll(timeout_ms=500).values():
                rejected.extend(records)
        time.sleep(2)
    finally:
        service.stop()

    assert sorted(m['id'] for m in delivered) == expected
    assert sorted(json.loads(r.value)['id'] for r in rejected) == [3, 17, 41]
    errors = {json.loads(r.value)['id']: dict(r.headers)['mage-error'].decode() for r in rejected}
    assert 'message 17 is not accepted' in errors[17], errors
    assert committed(kafka_bootstrap, group, topics[0]) == 60
