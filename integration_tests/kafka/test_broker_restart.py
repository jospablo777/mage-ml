"""
A Kafka broker restart under a running stream (NEXT_RELEASE S3): the broker stops while the
engine reads and writes, comes back, and every message reaches the sink with its offsets
committed. The test runs its own broker, so restarting it touches no other test.
"""
import json
import socket
import subprocess
import time
import uuid

import pytest
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.kafka.test_exported_stream import committed, produce
from integration_tests.kafka.test_stream_conformance import make_pipeline, wait_for

IMAGE = 'apache/kafka:4.1.0'
TOTAL = 20000


def free_port() -> int:
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def admin_ready(bootstrap: str):
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=3000)
    except KafkaError:
        return None
    try:
        admin.list_topics()
        return admin
    except KafkaError:
        admin.close()
        return None


@pytest.fixture
def broker(kafka_bootstrap):
    """A one-node broker of the compose image on a free port, removed after the test."""
    if subprocess.run(['docker', 'version'], capture_output=True).returncode != 0:
        pytest.skip('Docker is not available')
    port = free_port()
    name = f'it-kafka-restart-{uuid.uuid4().hex[:8]}'
    subprocess.run([
        'docker', 'run', '-d', '--name', name, '-p', f'127.0.0.1:{port}:19092',
        '-e', 'KAFKA_NODE_ID=1',
        '-e', 'KAFKA_PROCESS_ROLES=broker,controller',
        '-e', 'KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:19092',
        '-e', f'KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,'
              f'EXTERNAL://127.0.0.1:{port}',
        '-e', 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP='
              'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT',
        '-e', 'KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093',
        '-e', 'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
        '-e', 'KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT',
        '-e', 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1',
        '-e', 'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1',
        '-e', 'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1',
        '-e', 'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0',
        '-e', 'KAFKA_HEAP_OPTS=-Xmx384m -Xms256m',
        IMAGE,
    ], check=True, capture_output=True)
    bootstrap = f'127.0.0.1:{port}'
    try:
        wait_for(lambda: admin_ready(bootstrap), timeout=120).close()
        yield name, bootstrap
    finally:
        subprocess.run(['docker', 'rm', '-f', name], capture_output=True)


def read_to_end(bootstrap, topic) -> list:
    """Every message in the topic, up to its end offsets, so repeats are counted."""
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False,
                             group_id=None)
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


def committed_now(bootstrap, group, topic) -> int:
    """The committed offsets, 0 while the broker or its group coordinator is not up."""
    try:
        return committed(bootstrap, group, topic)
    except KafkaError:
        return 0


def restart_under_stream(broker, project, tmp_path, halt, down, repeats, recovery=180):
    """Stops the broker under a running stream for `down` seconds, starts it again, and
    checks that every message reached the sink within `recovery` seconds of its return,
    with at most `repeats` written twice."""
    name, bootstrap = broker
    topics = [f'it_in_{uuid.uuid4().hex[:10]}', f'it_out_{uuid.uuid4().hex[:10]}']
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=2, replication_factor=1) for t in topics])
    admin.close()
    group = f'g_{uuid.uuid4().hex[:8]}'
    # librdkafka's group log goes to the service log, for a failure to explain itself.
    pipeline = make_pipeline(project, topics, group, batch_size=100,
                             librdkafka={'debug': 'cgrp'})
    service = None
    try:
        directory = export_service(project, pipeline.uuid, tmp_path)
        produce(bootstrap, topics[0], range(TOTAL))
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=bootstrap)
        service.start()
        wait_for(lambda: committed_now(bootstrap, group, topics[0]) >= 300)

        subprocess.run(['docker', *halt, name], check=True, capture_output=True)
        time.sleep(down)
        assert service.process.poll() is None, 'the engine exited while the broker was down'
        subprocess.run(['docker', 'start', name], check=True, capture_output=True)
        wait_for(lambda: admin_ready(bootstrap), timeout=120).close()

        wait_for(lambda: committed_now(bootstrap, group, topics[0]) == TOTAL, timeout=recovery)
        out = read_to_end(bootstrap, topics[1])
        ids = [m['id'] for m in out]

        assert sorted(set(ids)) == list(range(TOTAL)), 'a message was lost'
        assert len(ids) - TOTAL <= repeats, f'{len(ids) - TOTAL} messages written twice'
        assert service.process.poll() is None
        assert service.streams()[0]['messages_in'] >= TOTAL
    finally:
        if service:
            service.stop()
        pipeline.delete()


@pytest.mark.parametrize('halt, down, repeats', [
    (['stop', '-t', '5'], 8, 0),
    # A killed broker leaves produce requests unanswered; the idempotent producer can bump
    # its epoch and send them again under new sequence numbers, which the broker cannot tell
    # from new messages (one in 20,000 seen in CI): at least once, a batch at most.
    (['kill'], 8, 500),
    # Longer than the group's session timeout (45 s): the group takes the partitions away,
    # and the batches written but not yet committed then are read again (at least once).
    (['kill'], 60, TOTAL // 4),
], ids=['stopped', 'killed', 'killed-past-session-timeout'])
def test_a_broker_restart_loses_nothing(broker, mage_project, tmp_path, halt, down, repeats):
    """Down mid-stream for seconds: the engine waits, reconnects, and goes on from where it
    was; nothing is lost, and after a graceful stop nothing is written twice. Down past
    the session timeout, the group gives the partitions back after the restarted broker
    drops the old member, and nothing is lost."""
    restart_under_stream(broker, mage_project, tmp_path, halt, down, repeats)
