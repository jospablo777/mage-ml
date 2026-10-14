"""
An exported stream from an ActiveMQ queue through a Python block to another queue, over
STOMP, run by mage-service: blocks get each body as text, as in MageML; every message is
sent once, persistent, with its metadata as headers and its values exact; nothing is left
unacknowledged; and a restart reads only what came after.
"""
import json
import textwrap
import time
import uuid
from pathlib import Path

from integration_tests.activemq.test_streaming import drain, publish
from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)

TRANSFORM = '''
import json


@transformer
def double(messages, *args, **kwargs):
    assert all(isinstance(m, str) for m in messages), [type(m) for m in messages]
    rows = [json.loads(m) for m in messages]
    return [dict(data=dict(r, doubled=r['n'] * 2), metadata=dict(source='shop'))
            for r in rows]
'''


def make_pipeline(project, port, queues):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'activemq_stream_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', f'''
        connector_type: activemq
        connection_host: 127.0.0.1
        connection_port: {port}
        queue_name: {queues[0]}
        batch_size: 50
        batch_timeout: 0.2
    ''', 'yaml')
    step = add('step', 'transformer', TRANSFORM, 'python', [source])
    add('sink', 'data_exporter', f'''
        connector_type: activemq
        connection_host: 127.0.0.1
        connection_port: {port}
        queue_name: {queues[1]}
    ''', 'yaml', [step])
    return pipeline


def collect(port, queue, count, timeout=60):
    found = []
    deadline = time.time() + timeout
    while len(found) < count and time.time() < deadline:
        found += drain(port, queue, wait=1.0)
    return found


def test_messages_flow_once_persistent_with_their_headers(
    mage_project, activemq_port, tmp_path,
):
    queues = [f'it_{uuid.uuid4().hex[:12]}' for _ in range(2)]
    pipeline = make_pipeline(mage_project, activemq_port, queues)
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        publish(activemq_port, queues[0],
                *[dict(n=i, big=2**70 + i, city='Zürich 🚀') for i in range(120)])
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data')
        service.start()
        try:
            service.wait_running()
            out = collect(activemq_port, queues[1], 120)
            time.sleep(1)
            stream = service.streams()[0]
        finally:
            service.stop()
        left = drain(activemq_port, queues[0], wait=1.0)
        publish(activemq_port, queues[0], dict(n=1000, big=1, city='x'))
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data2')
        service.start()
        try:
            service.wait_running()
            more = collect(activemq_port, queues[1], 1)
            time.sleep(1)
            more += drain(activemq_port, queues[1], wait=1.0)
        finally:
            service.stop()
    finally:
        pipeline.delete()

    assert stream['last_error'] is None, stream
    assert left == []
    rows = [json.loads(frame.body) for frame in out]
    assert sorted(r['n'] for r in rows) == list(range(120))
    assert all(r['doubled'] == r['n'] * 2 and r['big'] == 2**70 + r['n']
               and r['city'] == 'Zürich 🚀' for r in rows)
    assert all(frame.headers.get('persistent') == 'true'
               and frame.headers.get('source') == 'shop' for frame in out)
    assert [json.loads(frame.body)['n'] for frame in more] == [1000]
