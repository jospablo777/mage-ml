"""
A network partition between an exported stream and its NATS server (NEXT_RELEASE S3): the
connection is cut while messages keep arriving, and once it heals every message reaches the
sink exactly once.
"""
import json
import os
import textwrap
import time
import urllib.request
import uuid
from pathlib import Path

import pytest

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.nats.test_streaming import jetstream, publish

PROXY_PORT = int(os.getenv('MAGE_TEST_TOXIPROXY_PROXY_PORT', '18666'))


def toxiproxy(url, method, path, body=None):
    request = urllib.request.Request(
        f'{url}{path}', method=method, data=None if body is None else json.dumps(body).encode(),
        headers={'Content-Type': 'application/json'},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.read()


@pytest.fixture
def proxy(toxiproxy_url):
    """A proxy to NATS on the published proxy port, removed after the test."""
    name = f'nats_{uuid.uuid4().hex[:8]}'
    toxiproxy(toxiproxy_url, 'POST', '/proxies', dict(
        name=name, listen='0.0.0.0:8666', upstream='nats:4222', enabled=True,
    ))
    yield name
    toxiproxy(toxiproxy_url, 'DELETE', f'/proxies/{name}')


@pytest.fixture
def streams(nats_url):
    names = {kind: f'it_{kind}_{uuid.uuid4().hex[:8]}' for kind in ('in', 'out')}
    for name in names.values():
        jetstream(nats_url, lambda js, n=name: js.add_stream(name=n, subjects=[f'{n}.events']))
    yield names
    for name in names.values():
        jetstream(nats_url, lambda js, n=name: js.delete_stream(n))


def out_ids(nats_url, stream):
    async def read(js):
        state = (await js.stream_info(stream)).state
        if not state.messages:
            return []
        subscription = await js.pull_subscribe(f'{stream}.events', stream=stream)
        ids = []
        try:
            while len(ids) < state.messages:
                for message in await subscription.fetch(min(500, state.messages - len(ids)),
                                                        timeout=5):
                    ids.append(json.loads(message.data)['id'])
                    await message.ack()
        finally:
            await subscription.unsubscribe()
        return ids

    return jetstream(nats_url, read)


def count(nats_url, stream):
    return jetstream(nats_url, lambda js: js.stream_info(stream)).state.messages


def wait_for(condition, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.5)
    return False


def test_a_partition_from_the_server_loses_and_repeats_nothing(
    mage_project, nats_url, toxiproxy_url, proxy, streams, tmp_path,
):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'partition_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING,
                               repo_path=mage_project)
    source = Block.create(f'{name}_source', 'data_loader', mage_project, language='yaml')
    Path(source.file_path).write_text(textwrap.dedent(f'''
        connector_type: nats
        server_url: nats://127.0.0.1:{PROXY_PORT}
        stream_name: {streams['in']}
        subject: {streams['in']}.events
        consumer_name: partition
        batch_size: 20
    '''))
    pipeline.add_block(source)
    sink = Block.create(f'{name}_sink', 'data_exporter', mage_project, language='yaml')
    Path(sink.file_path).write_text(textwrap.dedent(f'''
        connector_type: nats
        server_url: {nats_url}
        subject: {streams['out']}.events
    '''))
    pipeline.add_block(sink, upstream_block_uuids=[source.uuid])
    service = None
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        publish(nats_url, f"{streams['in']}.events", *[dict(id=i) for i in range(100)])
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data')
        service.start()
        assert wait_for(lambda: count(nats_url, streams['out']) == 100)

        # The partition: the stream cannot reach its server while messages keep coming.
        toxiproxy(toxiproxy_url, 'POST', f'/proxies/{proxy}', dict(enabled=False))
        publish(nats_url, f"{streams['in']}.events", *[dict(id=i) for i in range(100, 300)])
        time.sleep(8)
        assert count(nats_url, streams['out']) == 100
        toxiproxy(toxiproxy_url, 'POST', f'/proxies/{proxy}', dict(enabled=True))

        assert wait_for(lambda: count(nats_url, streams['out']) >= 300, timeout=180), \
            service.log.read_text()[-3000:]
        time.sleep(3)
        ids = out_ids(nats_url, streams['out'])
        assert sorted(ids) == list(range(300)), 'a message was lost or written twice'
    finally:
        if service:
            service.stop()
        pipeline.delete()
