"""
Audit records of an exported service copied to S3 (MinIO): a model served with audit, two
requests, and the records in the bucket, joined in object order, the instance's hash-chained
file byte for byte, the last ones copied when the service stops.
"""
import hashlib
import json
import time

import pytest

from integration_tests.exported_services import (
    RunningService,
    object_store_service_binary,
)
from integration_tests.models.test_challengers import SCORE, register, request
from integration_tests.postgres.test_sql_block_matrix import block_uuid, build


def objects(s3, bucket, prefix):
    keys = sorted(
        item['Key']
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix)
        for item in page.get('Contents', [])
    )
    return keys, b''.join(s3.get_object(Bucket=bucket, Key=k)['Body'].read() for k in keys)


@pytest.mark.timeout(1800)
def test_audit_records_reach_the_bucket_and_keep_their_chain(
    mage_project, mlflow_url, s3, s3_settings, bucket, tmp_path, monkeypatch,
):
    from mage_ai.pipeline_services import export

    registered = register(mlflow_url, monkeypatch)
    pipeline = build(mage_project, [dict(uuid='score', type='data_loader', code=SCORE)])
    captured = export.capture(
        mage_project, [pipeline], name='scoring', models=[f'churn=models:/{registered}/1'],
        audits=dict(churn=dict(features='values', mask=[], hash=[], reasons=0)),
    )
    assert 'object-store' in captured.engine_features
    directory = export.write(captured, str(tmp_path / 'service'), force=True)
    data = tmp_path / 'data'
    service = RunningService(
        object_store_service_binary(), directory, data,
        MAGE_SERVICE_AUDIT_URL=f's3://{bucket}/audit',
        MAGE_SERVICE_AUDIT_SHIP_SECONDS='1',
        AWS_ACCESS_KEY_ID=s3_settings['aws_access_key_id'],
        AWS_SECRET_ACCESS_KEY=s3_settings['aws_secret_access_key'],
        AWS_REGION='us-east-1', AWS_ENDPOINT_URL=s3_settings['endpoint_url'],
        AWS_ALLOW_HTTP='true',
    )
    service.start()
    try:
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                service.get('/healthz')
                break
            except OSError:
                time.sleep(0.25)
        block = block_uuid(pipeline, 'score')
        assert request(service.port, pipeline, block, [1, 2, 3])['status'] == 'completed'

        deadline = time.time() + 60
        while time.time() < deadline:
            keys, text = objects(s3, bucket, 'audit/scoring/')
            if text.count(b'\n') >= 3:
                break
            time.sleep(0.5)
        assert text.count(b'\n') == 3, keys
        assert all('/churn/' in k for k in keys), keys

        assert request(service.port, pipeline, block, [4, 5])['status'] == 'completed'
    finally:
        service.stop()

    keys, text = objects(s3, bucket, 'audit/scoring/')
    [local] = list((data / 'audit' / 'churn').glob('*.jsonl'))
    assert text == local.read_bytes(), keys
    lines = text.decode().splitlines()
    assert [json.loads(line)['key'] for line in lines] == [1, 2, 3, 4, 5]
    for before, line in zip(lines, lines[1:]):
        assert json.loads(line)['previous'] == hashlib.sha256(before.encode()).hexdigest()
