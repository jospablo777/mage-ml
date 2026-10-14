"""
An exported stream that writes each batch as one object to S3 (MinIO): a webhook's messages,
transformed, land as Parquet with every value exact (integers past 64 bits, Unicode) or as
JSON lines byte for byte, and the caller is answered only once the object is written. Mage's
amazon_s3 sink settings export the same way, endpoint from AWS_ENDPOINT_URL as boto3 reads it.
"""
import io
import json
import textwrap
import urllib.error
import urllib.request
import uuid
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from integration_tests.exported_services import (
    TOKEN,
    RunningService,
    export_service,
    stream_service_binary,
)

TRANSFORM = '''
from typing import Dict, List


@transformer
def tag(messages: List[Dict], *args, **kwargs):
    return [dict(m, tagged=True) for m in messages]
'''


def make_pipeline(project, sink_yaml):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'object_sink_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', 'connector_type: webhook\nanswer_within_seconds: 30\n',
                 'yaml')
    tag = add('tag', 'transformer', TRANSFORM, 'python', [source])
    add('sink', 'data_exporter', sink_yaml, 'yaml', [tag])
    return pipeline


def post(service, pipeline, messages):
    request = urllib.request.Request(
        f'http://127.0.0.1:{service.port}/v1/streams/{pipeline}/messages',
        data=json.dumps(messages).encode(), method='POST',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.status, json.loads(response.read())


def objects(s3, bucket):
    listing = s3.list_objects_v2(Bucket=bucket)
    return [item['Key'] for item in listing.get('Contents', [])]


def run(project, tmp_path, s3_settings, sink_yaml, messages):
    pipeline = make_pipeline(project, sink_yaml)
    try:
        directory = export_service(project, pipeline.uuid, tmp_path)
        service = RunningService(
            stream_service_binary(), directory, tmp_path / 'data',
            AWS_ACCESS_KEY_ID=s3_settings['aws_access_key_id'],
            AWS_SECRET_ACCESS_KEY=s3_settings['aws_secret_access_key'],
            AWS_REGION='us-east-1', AWS_ENDPOINT_URL=s3_settings['endpoint_url'],
            AWS_ALLOW_HTTP='true',
        )
        service.start()
        try:
            service.wait_running()
            return post(service, pipeline.uuid, messages)
        finally:
            service.stop()
    finally:
        pipeline.delete()


MESSAGES = [
    dict(id=1, big=2**70 + 1, city='Zürich 🚀', price=0.1),
    dict(id=2, big=-(2**65), city='São Paulo', price=None),
]


def test_parquet_objects_keep_every_value(mage_project, tmp_path, s3, s3_settings, bucket):
    sink = f'''
        connector_type: object_storage
        url: s3://{bucket}/orders
        format: parquet
        date_partition_format: "dt=%Y-%m-%d"
    '''
    status, body = run(mage_project, tmp_path, s3_settings, sink, MESSAGES)

    assert (status, body) == (200, {'accepted': 2})
    keys = objects(s3, bucket)
    assert len(keys) == 1 and keys[0].startswith('orders/dt=') and keys[0].endswith('.parquet')
    data = s3.get_object(Bucket=bucket, Key=keys[0])['Body'].read()
    rows = sorted(pq.read_table(io.BytesIO(data)).to_pylist(), key=lambda r: r['id'])
    assert [r['big'] for r in rows] == [Decimal(2**70 + 1), Decimal(-(2**65))]
    assert [r['city'] for r in rows] == ['Zürich 🚀', 'São Paulo']
    assert rows[0]['price'] == 0.1 and rows[1]['price'] is None
    assert all(r['tagged'] for r in rows)


def test_mage_s3_sink_settings_write_json_lines(mage_project, tmp_path, s3, s3_settings, bucket):
    sink = f'''
        connector_type: amazon_s3
        bucket: {bucket}
        prefix: raw/orders
        file_type: json
        buffer_size_mb: 1
        buffer_timeout_seconds: 2
    '''
    status, _ = run(mage_project, tmp_path, s3_settings, sink, MESSAGES)

    assert status == 200
    keys = objects(s3, bucket)
    assert len(keys) == 1 and keys[0].startswith('raw/orders/'), keys
    lines = s3.get_object(Bucket=bucket, Key=keys[0])['Body'].read().decode().splitlines()
    assert sorted(json.loads(line)['big'] for line in lines) == [-(2**65), 2**70 + 1]


def test_a_store_that_refuses_the_write_answers_the_caller_with_an_error(
    mage_project, tmp_path, s3_settings,
):
    sink = '''
        connector_type: object_storage
        url: s3://no-such-bucket-mage/orders
    '''
    with pytest.raises(urllib.error.HTTPError) as caught:
        run(mage_project, tmp_path, s3_settings, sink, MESSAGES[:1])
    assert caught.value.code in (503, 504)
