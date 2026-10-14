"""
An exported stream reading Avro messages in Confluent's wire format from Kafka, with Mage's
serde_config (AVRO and a schema registry): the engine reads each schema from the registry
once and hands the blocks JSON, with decimals exact, dates and timestamps ISO text and
unions their value; a message that is not Avro goes to the dead-letter topic with why.
"""
import io
import json
import struct
import textwrap
import threading
import time
import uuid
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)

fastavro = pytest.importorskip('fastavro')

SCHEMA = {
    'type': 'record', 'name': 'Order', 'namespace': 'shop', 'fields': [
        {'name': 'id', 'type': 'long'},
        {'name': 'note', 'type': ['null', 'string'], 'default': None},
        {'name': 'price', 'type': {'type': 'bytes', 'logicalType': 'decimal',
                                   'precision': 20, 'scale': 2}},
        {'name': 'day', 'type': {'type': 'int', 'logicalType': 'date'}},
        {'name': 'at', 'type': {'type': 'long', 'logicalType': 'timestamp-millis'}},
    ],
}
SCHEMA_ID = 42


class Registry(BaseHTTPRequestHandler):
    """The schema registry's read of a schema by id, behind basic authentication."""

    requests = []

    def do_GET(self):  # noqa: N802
        Registry.requests.append((self.path, self.headers.get('Authorization')))
        if self.path != f'/schemas/ids/{SCHEMA_ID}':
            self.send_response(404)
            self.end_headers()
            return
        body = json.dumps(dict(schema=json.dumps(SCHEMA))).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/vnd.schemaregistry.v1+json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def registry():
    server = ThreadingHTTPServer(('127.0.0.1', 0), Registry)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    Registry.requests = []
    yield f'http://127.0.0.1:{server.server_port}'
    server.shutdown()


def framed(record) -> bytes:
    body = io.BytesIO()
    fastavro.schemaless_writer(body, fastavro.parse_schema(SCHEMA), record)
    return b'\x00' + struct.pack('>I', SCHEMA_ID) + body.getvalue()


def test_avro_messages_reach_the_blocks_as_exact_json(
    mage_project, kafka_bootstrap, registry, tmp_path,
):
    from datetime import date, datetime, timezone

    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'avro_{uuid.uuid4().hex[:8]}'
    topics = [f'{name}_in', f'{name}_out', f'{name}_dead']
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in topics])
    admin.close()
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING,
                               repo_path=mage_project)
    try:
        def add(suffix, kind, code, language, upstream=()):
            block = Block.create(f'{name}_{suffix}', kind, mage_project, language=language,
                                 pipeline=pipeline)
            Path(block.file_path).write_text(textwrap.dedent(code))
            pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
            return block

        source = add('source', 'data_loader', f'''
            connector_type: kafka
            bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
            topic: {topics[0]}
            consumer_group: {name}
            auto_offset_reset: earliest
            batch_size: 50
            timeout_ms: 200
            serde_config:
              serialization_method: AVRO
              schema_registry_url: {registry}
              schema_registry_username: reader
              schema_registry_password: "{{{{ env_var('REGISTRY_PASSWORD') }}}}"
        ''', 'yaml')
        step = add('tag', 'transformer', '''
            @transformer
            def tag(messages, *args, **kwargs):
                return [dict(m, seen=True) for m in messages]
        ''', 'python', [source])
        add('sink', 'data_exporter', f'''
            connector_type: kafka
            bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
            topic: {topics[1]}
        ''', 'yaml', [step])
        add('dead', 'data_exporter', f'''
            connector_type: kafka
            bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
            topic: {topics[2]}
            dead_letter: true
        ''', 'yaml', [source])
        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap)
        at = datetime(2026, 10, 14, 17, 46, 40, 123000, tzinfo=timezone.utc)
        for i in range(30):
            producer.send(topics[0], framed(dict(
                id=2**53 + i, note='Zürich 🚀' if i % 2 else None,
                price=Decimal('-12345678901234.5') + i, day=date(2026, 10, 14), at=at,
            )))
        producer.send(topics[0], b'{"not": "avro"}')
        producer.flush()
        producer.close()
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 KAFKA_BOOTSTRAP=kafka_bootstrap, REGISTRY_PASSWORD='s3cret')
        service.start()
        try:
            service.wait_running()
            out = read(kafka_bootstrap, topics[1], 30)
            dead = read(kafka_bootstrap, topics[2], 1)
        finally:
            service.stop()
    finally:
        pipeline.delete()
        admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
        admin.delete_topics(topics)
        admin.close()

    messages = sorted((json.loads(r.value) for r in out), key=lambda m: m['id'])
    assert len(messages) == 30
    assert messages[0] == dict(id=2**53, note=None, price='-12345678901234.50', day='2026-10-14',
                               at='2026-10-14T17:46:40.123Z', seen=True)
    assert messages[1]['note'] == 'Zürich 🚀' and messages[1]['price'] == '-12345678901233.50'
    assert [r.value for r in dead] == [b'{"not": "avro"}']
    assert "Confluent's Avro format" in dict(dead[0].headers)['mage-error'].decode()
    # The schema was read once, signed in.
    paths = [path for path, _ in Registry.requests]
    assert paths == [f'/schemas/ids/{SCHEMA_ID}']
    assert Registry.requests[0][1] == 'Basic cmVhZGVyOnMzY3JldA=='


def read(bootstrap, topic, count, timeout=120):
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap, auto_offset_reset='earliest',
                             group_id=f'reader_{uuid.uuid4().hex[:8]}')
    found = []
    deadline = time.time() + timeout
    while len(found) < count and time.time() < deadline:
        for batch in consumer.poll(timeout_ms=500).values():
            found.extend(batch)
    consumer.close()
    return found
