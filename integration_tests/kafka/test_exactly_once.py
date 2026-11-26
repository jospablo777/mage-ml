"""
Kafka to Kafka exactly once (I-36, delivery: exactly_once): the engine is killed (SIGKILL)
three times while it streams 100,000 messages, and a consumer that reads with read_committed
finds each input message's output once, none lost and none twice; a block that counts per
key with keyed state counts each message once, and a window outputs each window once. The
test runs its own broker.
"""
import json
import textwrap
import time
import uuid
from collections import defaultdict
from pathlib import Path

from kafka.admin import KafkaAdminClient, NewTopic

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.kafka.test_broker_restart import (  # noqa: F401 (fixture)
    broker,
    committed_now,
    explain,
)
from integration_tests.kafka.test_exported_stream import produce
from integration_tests.kafka.test_stream_conformance import make_pipeline, wait_for

# Enough that each kill lands mid-stream: the stream commits thousands a second.
TOTAL = 100000
KILLS = 3


def read_committed(bootstrap, topic):
    """Every committed message of the topic, as a read_committed consumer sees them."""
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False,
                             group_id=None, isolation_level='read_committed')
    try:
        parts = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
        consumer.assign(parts)
        consumer.seek_to_beginning(*parts)
        ends = consumer.end_offsets(parts)
        messages = []
        deadline = time.monotonic() + 120
        while any(consumer.position(p) < ends[p] for p in parts):
            assert time.monotonic() < deadline, 'timed out reading the output'
            for records in consumer.poll(timeout_ms=500).values():
                messages.extend(json.loads(r.value) for r in records)
        return messages
    finally:
        consumer.close()


def test_each_message_is_written_once_through_killed_engines(
    broker, mage_project, tmp_path,  # noqa: F811
):
    _, bootstrap = broker
    topics = [f'it_in_{uuid.uuid4().hex[:10]}', f'it_out_{uuid.uuid4().hex[:10]}']
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=2, replication_factor=1) for t in topics])
    admin.close()
    group = f'g_{uuid.uuid4().hex[:8]}'
    pipeline = make_pipeline(mage_project, topics, group, batch_size=100,
                             delivery='exactly_once', librdkafka={'debug': 'cgrp,fetch'},
                             sink_librdkafka={'debug': 'eos,msg'})
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        manifest = json.loads((directory / 'service.json').read_text())
        assert manifest['pipelines'][0]['stream']['delivery'] == 'exactly_once'
        produce(bootstrap, topics[0], range(TOTAL))
        binary = stream_service_binary()
        reached = 0
        for kill in range(KILLS):
            service = RunningService(binary, directory, tmp_path / 'data',
                                     KAFKA_BOOTSTRAP=bootstrap)
            service.start()
            # Killed once more has committed: mid-stream, with a transaction open.
            target = min(TOTAL - 1, reached + 5000)
            wait_for(lambda target=target: committed_now(bootstrap, group, topics[0]) >= target,
                     timeout=180)
            reached = committed_now(bootstrap, group, topics[0])
            service.process.kill()
            service.process.wait()
            print(f'killed {kill + 1}: {reached} of {TOTAL} committed')
        service = RunningService(binary, directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=bootstrap)
        service.start()
        wait_for(lambda: committed_now(bootstrap, group, topics[0]) == TOTAL, timeout=300)

        ids = [m['id'] for m in read_committed(bootstrap, topics[1])]

        assert sorted(set(ids)) == list(range(TOTAL)), 'a message was lost'
        assert reached < TOTAL, 'the last kill came after the stream had committed all'
        assert len(ids) == TOTAL, f'{len(ids) - TOTAL} messages written twice'
    except AssertionError:
        if service:
            explain(service, bootstrap, group, topics[0])
        raise
    finally:
        if service and service.process and service.process.poll() is None:
            service.stop()
        pipeline.delete()


COUNT = '''
@transformer
def count(messages, *args, **kwargs):
    state = kwargs['state']
    out = []
    for m in messages:
        state[m['key']] = state.get(m['key'], 0) + 1
        out.append(dict(m, seen=state[m['key']]))
    return out
'''


WINDOW = '''
window:
  key: key
  type: tumbling
  size_seconds: 100
  aggregate:
    n: count
    total: {sum: id}
'''


def counting_pipeline(project, topics, group, window=False):
    """
    Kafka, a block that counts per key with keyed state (or a window per key), Kafka;
    exactly once.
    """
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'{"window" if window else "counting"}_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, language, upstream=(), configuration=None):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language,
                             configuration=configuration)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topics[0]}
        consumer_group: {group}
        auto_offset_reset: earliest
        batch_size: 100
        timeout_ms: 200
        delivery: exactly_once
    ''' + ('''
        event_time:
          field: at
          format: epoch_s
          lateness_seconds: 0
          on_late: pass
    ''' if window else ''), 'yaml')
    if window:
        counted = add('per_key', 'transformer', WINDOW, 'yaml', [source])
    else:
        counted = add('count', 'transformer', COUNT, 'python', [source],
                      configuration=dict(state_key='key'))
    add('kafka_out', 'data_exporter', f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topics[1]}
    ''', 'yaml', [counted])
    return pipeline


def crash_after_state_saves(mage_project, tmp_path, bootstrap, window=False):
    """
    Streams TOTAL messages (key k0 to k9, event time `at` = id seconds) through the counting
    pipeline, with the engine ending itself right after its 5th state save, before that
    transaction commits, KILLS times; then to the end. Returns the read_committed output.
    """
    from kafka import KafkaProducer

    topics = [f'it_in_{uuid.uuid4().hex[:10]}', f'it_out_{uuid.uuid4().hex[:10]}']
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=2, replication_factor=1) for t in topics])
    admin.close()
    group = f'g_{uuid.uuid4().hex[:8]}'
    pipeline = counting_pipeline(mage_project, topics, group, window=window)
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        producer = KafkaProducer(bootstrap_servers=bootstrap,
                                 key_serializer=lambda k: k.encode(),
                                 value_serializer=lambda v: json.dumps(v).encode())
        for i in range(TOTAL):
            # One key per partition's worth of messages: a key's messages stay in order.
            key = f'k{i % 10}'
            producer.send(topics[0], key=key, value=dict(id=i, key=key, at=i))
        producer.flush()
        producer.close()
        binary = stream_service_binary()
        reached = 0
        for kill in range(KILLS):
            # The engine ends itself after its 5th state save, before that transaction
            # commits: the state saved is ahead of the committed offsets, and a restart
            # must fall back to the committed state (a kill from outside rarely lands there).
            service = RunningService(binary, directory, tmp_path / 'data',
                                     KAFKA_BOOTSTRAP=bootstrap,
                                     MAGE_SERVICE_FAULT_STATE_SAVES='5')
            service.start()
            wait_for(lambda service=service: service.process.poll() is not None, timeout=300)
            reached = committed_now(bootstrap, group, topics[0])
            print(f'crashed {kill + 1}: {reached} of {TOTAL} committed')
        assert reached < TOTAL, 'the last kill came after the stream had committed all'
        service = RunningService(binary, directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=bootstrap)
        service.start()
        wait_for(lambda: committed_now(bootstrap, group, topics[0]) == TOTAL, timeout=600)
        return read_committed(bootstrap, topics[1])
    except AssertionError:
        if service:
            explain(service, bootstrap, group, topics[0])
        raise
    finally:
        if service and service.process and service.process.poll() is None:
            service.stop()
        pipeline.delete()


def test_keyed_state_counts_each_message_once_through_killed_engines(
    broker, mage_project, tmp_path,  # noqa: F811
):
    _, bootstrap = broker
    out = crash_after_state_saves(mage_project, tmp_path, bootstrap)

    ids = [m['id'] for m in out]
    assert sorted(set(ids)) == list(range(TOTAL)), 'a message was lost'
    assert len(ids) == TOTAL, f'{len(ids) - TOTAL} messages written twice'
    # Each key counted each of its messages once: 1, 2, ... with no gap and no repeat.
    seen = defaultdict(list)
    for m in out:
        seen[m['key']].append(m['seen'])
    for key, counts in seen.items():
        assert sorted(counts) == list(range(1, len(counts) + 1)), key


def test_windows_are_output_once_through_killed_engines(
    broker, mage_project, tmp_path,  # noqa: F811
):
    _, bootstrap = broker
    out = crash_after_state_saves(mage_project, tmp_path, bootstrap, window=True)

    # Key k<j> has ids j, j + 10, ...: 10 in each 100-second window. Every window but each
    # key's last (no later event closes it) is output once, with its 10 messages.
    windows = defaultdict(list)
    for m in out:
        windows[(m['key'], m['window_start'])].append(m)
    twice = [w for w, ms in windows.items() if len(ms) > 1]
    assert not twice, f'{len(twice)} windows output more than once, such as {twice[:3]}'
    assert len(windows) == 10 * (TOTAL // 100 - 1), f'{len(windows)} windows output'
    for (key, start), [m] in windows.items():
        assert m['n'] == 10, (key, start, m)
