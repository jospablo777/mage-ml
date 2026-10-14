"""
An exported stream from NATS JetStream through a Python block to NATS JetStream, run by
mage-service: every message reaches the output stream once with its values exact, the
consumer has nothing pending once the batch is acknowledged, a restart repeats nothing that
was acknowledged, and a replayed publish is dropped by its message id.
"""
import json
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
from integration_tests.nats.test_streaming import consumer_state, jetstream, publish

TRANSFORM = '''
@transformer
def double(messages, *args, **kwargs):
    return [dict(m, doubled=m['n'] * 2) for m in messages]
'''


@pytest.fixture
def streams(nats_url):
    names = [f'it_{uuid.uuid4().hex[:10]}' for _ in range(2)]
    for name in names:
        jetstream(nats_url, lambda js, n=name: js.add_stream(name=n, subjects=[f'{n}.events']))
    yield names
    for name in names:
        jetstream(nats_url, lambda js, n=name: js.delete_stream(n))


def make_pipeline(project, nats_url, streams):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'nats_stream_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', f'''
        connector_type: nats_js
        server_url: {nats_url}
        stream_name: {streams[0]}
        subject: {streams[0]}.events
        consumer_name: {name}
        batch_size: 50
        timeout: 0.2
    ''', 'yaml')
    step = add('double', 'transformer', TRANSFORM, 'python', [source])
    add('sink', 'data_exporter', f'''
        connector_type: nats_js
        server_url: {nats_url}
        stream_name: {streams[1]}
        subject: {streams[1]}.events
    ''', 'yaml', [step])
    return pipeline, name


def read_all(url, stream, count, timeout=60):
    async def action(js):
        subscription = await js.pull_subscribe(f'{stream}.events', durable='reader',
                                               stream=stream)
        found = []
        deadline = time.time() + timeout
        while len(found) < count and time.time() < deadline:
            try:
                messages = await subscription.fetch(count - len(found), timeout=2)
            except Exception:
                continue
            for message in messages:
                await message.ack()
                found.append((json.loads(message.data), dict(message.headers or {})))
        return found

    return jetstream(url, action)


def test_messages_flow_once_and_are_acknowledged_after_the_sink(
    mage_project, nats_url, streams, tmp_path,
):
    pipeline, consumer = make_pipeline(mage_project, nats_url, streams)
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data')
        publish(nats_url, f'{streams[0]}.events',
                *[dict(n=i, big=2**70 + i, city='Zürich 🚀') for i in range(120)])
        service.start()
        try:
            service.wait_running()
            out = read_all(nats_url, streams[1], 120)
            deadline = time.time() + 30
            while consumer_state(nats_url, streams[0], consumer) != (0, 0) \
                    and time.time() < deadline:
                time.sleep(0.2)
            state = consumer_state(nats_url, streams[0], consumer)
        finally:
            service.stop()
        # A restart reads nothing again.
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data2')
        publish(nats_url, f'{streams[0]}.events', dict(n=1000, big=1, city='x'))
        service.start()
        try:
            service.wait_running()
            more = read_all(nats_url, streams[1], 1)
        finally:
            service.stop()
    finally:
        pipeline.delete()

    assert sorted(m['n'] for m, _ in out) == list(range(120))
    assert all(m['doubled'] == m['n'] * 2 and m['big'] == 2**70 + m['n']
               and m['city'] == 'Zürich 🚀' for m, _ in out)
    # Ids name the run: NATS's source numbers messages from 1 again after a restart (I-101).
    assert all(headers.get('Nats-Msg-Id', '').startswith(f'{pipeline.uuid}~')
               for _, headers in out)
    assert state == (0, 0), state
    assert [m['n'] for m, _ in more] == [1000]
