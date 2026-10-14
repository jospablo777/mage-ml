"""
A Mage streaming pipeline (a RabbitMQ source, a Python transformer written for Mage's
RabbitMQ source, a RabbitMQ sink) exported with `mage export service` and run by mage-service
built with RabbitMQ, against RabbitMQ 4.3: every message is transformed once with its values
exact, deliveries are acknowledged only after the sink's publisher confirms, and a restart or
a kill loses nothing.
"""
import json
import textwrap
import time
import uuid
from pathlib import Path

import pika
import pytest

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)

CONNECTION = '''
connector_type: rabbitmq
connection_host: "127.0.0.1"
connection_port: %(port)s
username: %(user)s
password: "{{ env_var('RABBIT_PASSWORD') }}"
queue_name: %(queue)s
'''
SOURCE = CONNECTION + '''
batch_size: 50
batch_timeout: 0.2
'''
TRANSFORM = '''
import json
import time
from typing import List


@transformer
def double(messages: List, *args, **kwargs):
    time.sleep(float(kwargs.get('delay', 0)))
    rows = []
    for m in messages:
        row = json.loads(m.body)
        if row['id'] % 10 == 0:
            continue
        rows.append(dict(row, doubled=row['amount'] * 2, tag=m.method.delivery_tag,
                         source=m.properties.headers.get('source')))
    return rows
'''


@pytest.fixture
def queues(rabbitmq_channel):
    names = [f'it_in_{uuid.uuid4().hex[:10]}', f'it_out_{uuid.uuid4().hex[:10]}']
    for name in names:
        rabbitmq_channel.queue_declare(name, durable=True)
    yield names
    if rabbitmq_channel.is_open:
        for name in names:
            rabbitmq_channel.queue_delete(name)


@pytest.fixture
def stream_pipeline(mage_project, queues, rabbitmq_settings):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'rabbit_stream_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING,
                               repo_path=mage_project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, mage_project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    settings = dict(port=rabbitmq_settings['connection_port'],
                    user=rabbitmq_settings['username'])
    source = add('source', 'data_loader', SOURCE % dict(settings, queue=queues[0]), 'yaml')
    double = add('double', 'transformer', TRANSFORM, 'python', [source])
    add('sink', 'data_exporter', CONNECTION % dict(settings, queue=queues[1]), 'yaml', [double])
    yield pipeline
    pipeline.delete()


def service_for(mage_project, pipeline, tmp_path, rabbitmq_settings, name='data'):
    directory = export_service(mage_project, pipeline.uuid, tmp_path)
    return RunningService(stream_service_binary(), directory, tmp_path / name,
                          RABBIT_PASSWORD=rabbitmq_settings['password'])


def publish(channel, queue, ids):
    for i in ids:
        # 2**70 + i: integers past 64 bits keep every digit.
        channel.basic_publish(
            '', queue, json.dumps(dict(id=i, amount=i, big=2**70 + i)).encode(),
            pika.BasicProperties(delivery_mode=2, headers=dict(source='it')),
        )


def receive(channel, queue, count, timeout=120):
    """Messages from the output queue, acknowledged as they are read."""
    messages = []
    deadline = time.time() + timeout
    while len(messages) < count and time.time() < deadline:
        method, properties, body = channel.basic_get(queue, auto_ack=True)
        if method is None:
            time.sleep(0.1)
            continue
        messages.append((properties, json.loads(body)))
    return messages


def ready(channel, queue) -> int:
    """Messages waiting in a queue; unacknowledged ones count once their consumer is gone."""
    return channel.queue_declare(queue, passive=True).method.message_count


def test_an_exported_rabbitmq_stream_transforms_each_message_once_across_a_restart(
    mage_project, stream_pipeline, queues, rabbitmq_channel, rabbitmq_settings, tmp_path,
):
    service = service_for(mage_project, stream_pipeline, tmp_path, rabbitmq_settings)
    publish(rabbitmq_channel, queues[0], range(200))
    service.start()
    try:
        service.wait_running()
        first = receive(rabbitmq_channel, queues[1], 180)
    finally:
        service.stop()

    rows = [row for _, row in first]
    assert sorted(r['id'] for r in rows) == [i for i in range(200) if i % 10]
    assert all(r['doubled'] == r['amount'] * 2 and r['big'] == 2**70 + r['id'] for r in rows)
    assert all(r['source'] == 'it' and isinstance(r['tag'], int) for r in rows)
    # Each output names the input position it came from, for consumers that dedupe.
    assert all(p.message_id and p.delivery_mode == 2 for p, _ in first)
    assert len({p.message_id for p, _ in first}) == len(first)
    # A graceful stop acknowledges every delivery it processed: nothing is redelivered.
    assert ready(rabbitmq_channel, queues[0]) == 0

    publish(rabbitmq_channel, queues[0], range(200, 250))
    service.start()
    try:
        status = service.wait_running()
        second = receive(rabbitmq_channel, queues[1], 45)
        time.sleep(2)
        second += receive(rabbitmq_channel, queues[1], 1, timeout=2)
    finally:
        service.stop()

    assert status['last_error'] is None
    assert sorted(r['id'] for _, r in second) == [i for i in range(200, 250) if i % 10]
    assert ready(rabbitmq_channel, queues[0]) == 0


def test_a_killed_service_loses_nothing(
    mage_project, stream_pipeline, queues, rabbitmq_channel, rabbitmq_settings, tmp_path,
):
    """
    kill -9 in the middle of a stream: deliveries not yet acknowledged go back to the queue
    with the connection, so the restarted service writes every message; only batches in
    flight at the kill are written twice.
    """
    stream_pipeline.variables = dict(delay=0.2)
    stream_pipeline.save()
    service = service_for(mage_project, stream_pipeline, tmp_path, rabbitmq_settings)
    expected = [i for i in range(600) if i % 10]
    publish(rabbitmq_channel, queues[0], range(600))
    service.start()
    try:
        service.wait_running()
        received = receive(rabbitmq_channel, queues[1], 100)
        service.process.kill()
        service.process.wait(timeout=30)
        service.start()
        service.wait_running()
        # Until every message arrived: the batch written twice counts in received, so a
        # count of what is left stopped reading short of it.
        deadline = time.time() + 180
        while {r['id'] for _, r in received} != set(expected) and time.time() < deadline:
            received += receive(rabbitmq_channel, queues[1], len(expected), timeout=5)
        time.sleep(3)
        received += receive(rabbitmq_channel, queues[1], len(expected), timeout=3)
    finally:
        if service.process.poll() is None:
            service.stop()

    ids = [r['id'] for _, r in received]
    assert sorted(set(ids)) == expected
    assert ready(rabbitmq_channel, queues[0]) == 0
    # Prefetch bounds what was in flight at the kill: a few batches of 50, 45 kept each.
    assert len(ids) - len(set(ids)) <= 8 * 45, f'{len(ids) - len(set(ids))} repeated'


def test_a_sink_queue_that_is_missing_stops_the_stream_and_acknowledges_nothing(
    mage_project, stream_pipeline, queues, rabbitmq_channel, rabbitmq_settings, tmp_path,
):
    """
    Messages are published as mandatory: when no queue takes them the broker returns them,
    the sink fails, and the input stays in its queue instead of being acknowledged.
    """
    rabbitmq_channel.queue_delete(queues[1])
    service = service_for(mage_project, stream_pipeline, tmp_path, rabbitmq_settings)
    publish(rabbitmq_channel, queues[0], range(1, 6))
    service.start()
    try:
        # The stream can fail before it is seen running.
        deadline = time.time() + 60
        status = None
        while time.time() < deadline:
            try:
                status = service.streams()[0]
            except OSError:  # not listening yet
                status = None
            if status and status['state'] == 'failed':
                break
            time.sleep(0.5)
    finally:
        service.stop()

    assert status['state'] == 'failed', status
    assert 'no queue is bound' in status['last_error'], status
    assert ready(rabbitmq_channel, queues[0]) == 5
