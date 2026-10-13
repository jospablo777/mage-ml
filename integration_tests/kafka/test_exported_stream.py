"""
A Mage streaming pipeline (a Kafka source, a Python transformer, a Kafka sink) exported with
`mage export service` and run by mage-service built with Kafka, against Kafka 4.1: every
message is transformed once, positions are committed, and a restart neither loses nor
repeats messages.
"""
import json
import os
import signal
import socket
import subprocess
import sys
import textwrap
import time
import urllib.request
import uuid
from pathlib import Path

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from integration_tests.exported_services import REPO, SERVICE, export_service

TOKEN = 'stream-test-token'  # pragma: allowlist secret
# Its own target folder: the other exported-service tests build the engine without Kafka,
# and test workers build at the same time.
TARGET = SERVICE / 'target' / 'kafka'

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
from typing import Dict, List


@transformer
def double(messages: List[Dict], *args, **kwargs):
    return [dict(m, doubled=m['amount'] * 2) for m in messages if m['id'] % 10 != 0]
'''
SINK = '''
connector_type: kafka
bootstrap_server: "{{ env_var('KAFKA_BOOTSTRAP') }}"
topic: %(topic)s
'''


def kafka_service_binary() -> Path:
    subprocess.run(
        ['cargo', 'build', '-q', '-p', 'mage-service', '--features', 'kafka'],
        cwd=SERVICE, check=True, env=dict(os.environ, CARGO_TARGET_DIR=str(TARGET)),
    )
    return TARGET / 'debug' / 'mage-service'


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


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


class Service:
    def __init__(self, binary: Path, directory: Path, data: Path, bootstrap: str):
        self.port = free_port()
        self.log = data.parent / f'service-{self.port}.log'
        self.env = dict(
            os.environ,
            MAGE_SERVICE_DIR=str(directory),
            MAGE_SERVICE_DATA=str(data),
            MAGE_SERVICE_PORT=str(self.port),
            MAGE_SERVICE_TOKEN=TOKEN,
            MAGE_SERVICE_WORKER=str(
                directory / 'python/mage_ai/pipeline_services/runtime/worker.py',
            ),
            MAGE_SERVICE_PYTHON=sys.executable,
            PYTHONPATH=os.pathsep.join([str(directory / 'python'), str(REPO)]),
            KAFKA_BOOTSTRAP=bootstrap,
        )
        self.binary = binary
        self.process = None

    def start(self):
        self.process = subprocess.Popen(
            [str(self.binary), 'serve'], env=self.env,
            stdout=self.log.open('a'), stderr=subprocess.STDOUT,
        )

    def streams(self) -> list:
        request = urllib.request.Request(
            f'http://127.0.0.1:{self.port}/v1/streams',
            headers={'Authorization': f'Bearer {TOKEN}'},
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.loads(response.read())['streams']

    def wait_running(self, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            assert self.process.poll() is None, self.log.read_text()[-4000:]
            try:
                streams = self.streams()
                if streams and streams[0]['state'] == 'running':
                    return streams[0]
            except OSError:
                pass
            time.sleep(0.5)
        raise AssertionError(f'the stream did not start:\n{self.log.read_text()[-4000:]}')

    def stop(self):
        self.process.send_signal(signal.SIGTERM)
        assert self.process.wait(timeout=60) == 0, self.log.read_text()[-4000:]


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
    service = Service(kafka_service_binary(), directory, tmp_path / 'data', kafka_bootstrap)
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
