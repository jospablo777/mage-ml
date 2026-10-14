"""
An exported stream from a Pub/Sub subscription through a Python block to a Pub/Sub topic
(Google's emulator), run by mage-service: blocks get messages as MageML hands them over,
every message is published once with its values exact, including a batch that runs longer
than the subscription's ack deadline, nothing is left unacknowledged, and a restart reads
only what came after.
"""
import base64
import json
import textwrap
import time
import uuid
from pathlib import Path

import pytest
import requests

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)

PROJECT = 'mage-test'
# The input subscription's ack deadline is 10 seconds: the batch with `slow` runs past it.
TRANSFORM = '''
import json
import time


@transformer
def double(messages, *args, **kwargs):
    rows = [dict(json.loads(m['data']), source=m['metadata']['attributes'].get('source'))
            for m in messages]
    if any(r.get('slow') for r in rows):
        time.sleep(12)
    return [dict(r, doubled=r['n'] * 2) for r in rows]
'''


class Emulator:
    def __init__(self, host):
        self.base = f'http://{host}/v1/projects/{PROJECT}'

    def call(self, method, path, body=None):
        response = requests.request(method, f'{self.base}/{path}', json=body, timeout=30)
        assert response.ok, response.text
        return response.json() if response.content else {}

    def publish(self, topic, *messages, attributes=None):
        for start in range(0, len(messages), 500):
            self.call('POST', f'topics/{topic}:publish', dict(messages=[
                dict(data=base64.b64encode(json.dumps(m).encode()).decode(),
                     attributes=attributes or {})
                for m in messages[start:start + 500]
            ]))

    def pull(self, subscription, count, timeout=60):
        found = []
        deadline = time.time() + timeout
        while len(found) < count and time.time() < deadline:
            answer = self.call('POST', f'subscriptions/{subscription}:pull',
                               dict(maxMessages=1000, returnImmediately=True))
            received = answer.get('receivedMessages', [])
            if received:
                self.call('POST', f'subscriptions/{subscription}:acknowledge',
                          dict(ackIds=[r['ackId'] for r in received]))
            found += [json.loads(base64.b64decode(r['message']['data'])) for r in received]
            if not received:
                time.sleep(0.2)
        return found


@pytest.fixture
def emulator(pubsub_host):
    return Emulator(pubsub_host)


@pytest.fixture
def names(emulator):
    suffix = uuid.uuid4().hex[:8]
    names = dict(source=f'in-{suffix}', sink=f'out-{suffix}', subscription=f'scorer-{suffix}',
                 reader=f'reader-{suffix}')
    emulator.call('PUT', f'topics/{names["source"]}')
    emulator.call('PUT', f'topics/{names["sink"]}')
    emulator.call('PUT', f'subscriptions/{names["subscription"]}', dict(
        topic=f'projects/{PROJECT}/topics/{names["source"]}', ackDeadlineSeconds=10,
    ))
    emulator.call('PUT', f'subscriptions/{names["reader"]}', dict(
        topic=f'projects/{PROJECT}/topics/{names["sink"]}',
    ))
    yield names
    for kind, name in (('subscriptions', names['subscription']),
                       ('subscriptions', names['reader']),
                       ('topics', names['source']), ('topics', names['sink'])):
        requests.delete(f'{emulator.base}/{kind}/{name}', timeout=30)


def make_pipeline(project, host, names):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'pubsub_stream_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', f'''
        connector_type: google_cloud_pubsub
        project_id: {PROJECT}
        topic_id: {names['source']}
        subscription_id: {names['subscription']}
        pubsub_emulator_host: "{host}"
        batch_size: 50
    ''', 'yaml')
    step = add('step', 'transformer', TRANSFORM, 'python', [source])
    add('sink', 'data_exporter', f'''
        connector_type: google_cloud_pubsub
        project_id: {PROJECT}
        topic_id: {names['sink']}
        pubsub_emulator_host: "{host}"
    ''', 'yaml', [step])
    return pipeline


def test_messages_flow_once_with_their_attributes(
    mage_project, pubsub_host, emulator, names, tmp_path,
):
    pipeline = make_pipeline(mage_project, pubsub_host, names)
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        emulator.publish(names['source'], dict(n=-1, slow=True))
        emulator.publish(names['source'],
                         *[dict(n=i, big=2**70 + i, city='Zürich 🚀') for i in range(120)],
                         attributes=dict(source='shop'))
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data')
        service.start()
        try:
            service.wait_running()
            out = emulator.pull(names['reader'], 121, timeout=90)
            # Long enough for a message the slow batch let go to come back.
            time.sleep(12)
            out += emulator.pull(names['reader'], 1, timeout=2)
            stream = service.streams()[0]
        finally:
            service.stop()
        emulator.publish(names['source'], dict(n=1000, big=1, city='x'))
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data2')
        service.start()
        try:
            service.wait_running()
            more = emulator.pull(names['reader'], 1)
            time.sleep(2)
            more += emulator.pull(names['reader'], 1, timeout=2)
        finally:
            service.stop()
    finally:
        pipeline.delete()

    assert stream['last_error'] is None, stream
    assert sorted(m['n'] for m in out) == [-1] + list(range(120))
    assert all(m['doubled'] == m['n'] * 2 for m in out)
    assert all(m['big'] == 2**70 + m['n'] and m['city'] == 'Zürich 🚀'
               and m['source'] == 'shop' for m in out if m['n'] >= 0)
    assert [m['n'] for m in more] == [1000]
