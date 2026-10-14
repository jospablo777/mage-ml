"""
An exported stream from an SQS queue (ElasticMQ) through a Python block to JSON lines files,
run by mage-service: every message is written once with its values exact, including a batch
that runs longer than the queue's visibility timeout, the queue is empty once the sinks took
the messages, a restart reads only what came after, and RAW_VALUE bodies reach blocks as
text, as in MageML.
"""
import json
import textwrap
import time
import uuid
from pathlib import Path

from integration_tests.exported_services import (
    RunningService,
    export_service,
    stream_service_binary,
)

# The queue's visibility timeout is 5 seconds: the batch with `slow` runs past it.
TRANSFORM = '''
import time


@transformer
def double(messages, *args, **kwargs):
    if any(m.get('slow') for m in messages):
        time.sleep(8)
    return [dict(m, doubled=m['n'] * 2) for m in messages]
'''
RAW = '''
@transformer
def wrap(messages, *args, **kwargs):
    assert all(isinstance(m, str) for m in messages), [type(m) for m in messages]
    return [dict(text=m) for m in messages]
'''
ENV = dict(AWS_ACCESS_KEY_ID='test', AWS_SECRET_ACCESS_KEY='test', AWS_REGION='us-east-1')


def make_pipeline(project, sqs_url, queue, folder, code, extra=''):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'sqs_stream_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', textwrap.dedent(f'''
        connector_type: amazon_sqs
        queue_name: {queue}
        endpoint_url: {sqs_url}
        region: us-east-1
        batch_size: 40
        wait_time_seconds: 1
    ''') + extra, 'yaml')
    step = add('step', 'transformer', code, 'python', [source])
    add('sink', 'data_exporter', f'''
        connector_type: object_storage
        url: "file://{folder}"
        format: jsonl
    ''', 'yaml', [step])
    return pipeline


def written(folder):
    return [json.loads(line) for path in sorted(Path(folder).rglob('*.jsonl'))
            for line in path.read_text().splitlines() if line]


def wait_for(predicate, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.2)
    return predicate()


def send(client, url, *messages):
    for start in range(0, len(messages), 10):
        client.send_message_batch(QueueUrl=url, Entries=[
            dict(Id=str(i), MessageBody=m if isinstance(m, str) else json.dumps(m))
            for i, m in enumerate(messages[start:start + 10])
        ])


def left(client, url):
    attributes = client.get_queue_attributes(QueueUrl=url, AttributeNames=[
        'ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible',
    ])['Attributes']
    return sum(int(v) for v in attributes.values())


def test_messages_are_written_once_and_deleted_after_the_sink(
    mage_project, sqs_url, sqs_queue, tmp_path,
):
    client, name, url = sqs_queue
    folder = tmp_path / 'lake'
    pipeline = make_pipeline(mage_project, sqs_url, name, folder, TRANSFORM)
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        send(client, url, dict(n=-1, slow=True))
        send(client, url, *[dict(n=i, big=2**70 + i, city='Zürich 🚀') for i in range(120)])
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data', **ENV)
        service.start()
        try:
            service.wait_running()
            first = wait_for(lambda: len(written(folder)) >= 121, timeout=90)
            # Long enough for a message the slow batch let go to come back.
            time.sleep(7)
            emptied = wait_for(lambda: left(client, url) == 0, timeout=30)
        finally:
            service.stop()
        send(client, url, dict(n=1000, big=1, city='x'))
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data2', **ENV)
        service.start()
        try:
            service.wait_running()
            second = wait_for(lambda: len(written(folder)) >= 122)
            time.sleep(2)
        finally:
            service.stop()
    finally:
        pipeline.delete()

    assert first and second
    assert emptied, left(client, url)
    rows = written(folder)
    assert sorted(r['n'] for r in rows) == [-1] + list(range(120)) + [1000]
    assert all(r['doubled'] == r['n'] * 2 for r in rows)
    assert all(r['big'] == 2**70 + r['n'] and r['city'] == 'Zürich 🚀'
               for r in rows if 0 <= r['n'] < 120)


def test_raw_value_bodies_reach_blocks_as_text(mage_project, sqs_url, sqs_queue, tmp_path):
    client, name, url = sqs_queue
    folder = tmp_path / 'lake'
    pipeline = make_pipeline(mage_project, sqs_url, name, folder, RAW,
                             'serde_config:\n  serialization_method: RAW_VALUE\n')
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        send(client, url, 'plain text', '{"looks": "like json"}')
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data', **ENV)
        service.start()
        try:
            service.wait_running()
            done = wait_for(lambda: len(written(folder)) >= 2)
            stream = service.streams()[0]
        finally:
            service.stop()
    finally:
        pipeline.delete()

    assert done, stream
    assert sorted(r['text'] for r in written(folder)) == [
        'plain text', '{"looks": "like json"}',
    ]
