"""
A stream that turns webhook events into feature rows and pushes them to Feast's feature
server, exported with `mage export service` and run by mage-service: a webhook's sender is
answered 200 only once Feast took the rows, the online store serves them with every value
exact, and a row Feast rejects stops the stream instead of being acknowledged.
"""
import json
import textwrap
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import pytest
import requests

from integration_tests.exported_services import (
    TOKEN,
    RunningService,
    export_service,
    service_binary,
)

SOURCE = '''
connector_type: webhook
answer_within_seconds: 10
'''
FEATURES = '''
from typing import Dict, List


@transformer
def features(messages: List[Dict], *args, **kwargs):
    return [
        dict(
            driver_id=m['driver_id'],
            event_timestamp=m['at'],
            created=m['at'],
            conv_rate=m['trips'] / 100,
            acc_rate=0.25,
            avg_daily_trips=m['trips'],
            city=m.get('city', 'Zürich'),
            active=True,
        )
        for m in messages
    ]
'''
SINK = '''
connector_type: feast
url: "{{ env_var('FEAST_URL') }}"
push_source_name: driver_stats_push
to: online
allow_private: true
'''


@pytest.fixture
def stream_pipeline(mage_project):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'feast_stream_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING,
                               repo_path=mage_project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, mage_project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', SOURCE, 'yaml')
    features = add('features', 'transformer', FEATURES, 'python', [source])
    add('sink', 'data_exporter', SINK, 'yaml', [features])
    yield pipeline
    pipeline.delete()


def post(service: RunningService, pipeline: str, messages, key: str):
    request = urllib.request.Request(
        f'http://127.0.0.1:{service.port}/v1/streams/{pipeline}/messages',
        data=json.dumps(messages).encode(), method='POST',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json',
                 'Idempotency-Key': key},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b'{}')


def online(feast_url, drivers):
    response = requests.post(f'{feast_url}/get-online-features', json=dict(
        entities=dict(driver_id=drivers),
        features=['driver_hourly_stats:conv_rate', 'driver_hourly_stats:avg_daily_trips',
                  'driver_hourly_stats:city'],
    ), timeout=30)
    response.raise_for_status()
    payload = response.json()
    names = payload['metadata']['feature_names']
    return {name: result['values'] for name, result in zip(names, payload['results'])}


def test_webhook_events_become_online_features_in_feast(
    mage_project, stream_pipeline, feast_url, tmp_path,
):
    directory = export_service(mage_project, stream_pipeline.uuid, tmp_path)
    service = RunningService(service_binary(), directory, tmp_path / 'data', FEAST_URL=feast_url)
    drivers = [int(time.time() * 1000) % 1_000_000 + 9_000_000 + i for i in range(2)]
    now = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    events = [
        dict(driver_id=drivers[0], at=now, trips=2**53 + 1, city='Zürich'),
        dict(driver_id=drivers[1], at=now, trips=40, city='São Paulo'),
    ]
    service.start()
    try:
        service.wait_running()
        status, body = post(service, stream_pipeline.uuid, events, 'drivers-1')
        again = post(service, stream_pipeline.uuid, events, 'drivers-1')
    finally:
        service.stop()

    assert (status, body) == (200, {'accepted': 2})
    assert again == (200, {'accepted': 2, 'duplicate': True})
    values = online(feast_url, drivers)
    assert values['avg_daily_trips'] == [2**53 + 1, 40]
    assert values['city'] == ['Zürich', 'São Paulo']


def test_a_row_feast_rejects_stops_the_stream_and_is_not_acknowledged(
    mage_project, stream_pipeline, feast_url, tmp_path,
):
    # A feature row whose driver_id is text: Feast refuses the push.
    directory = export_service(mage_project, stream_pipeline.uuid, tmp_path)
    service = RunningService(service_binary(), directory, tmp_path / 'data', FEAST_URL=feast_url)
    now = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    service.start()
    try:
        service.wait_running()
        status, _ = post(service, stream_pipeline.uuid,
                         [dict(driver_id='not a number', at=now, trips=1)], 'bad-1')
        stream = service.streams()[0]
    finally:
        service.stop()

    assert status in (503, 504)
    assert stream['state'] == 'failed', stream
    assert 'Feast sink' in stream['last_error'], stream
