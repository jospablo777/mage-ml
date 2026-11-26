"""
Kafka to Kafka exactly once (I-36, delivery: exactly_once): the engine is killed (SIGKILL)
three times while it streams 100,000 messages, and a consumer that reads with read_committed
finds each input message's output once, none lost and none twice. The test runs its own
broker.
"""
import json
import time
import uuid

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
