"""
A stream that turns webhook orders into Amplitude events, exported with
`mage export service` and run by mage-service against a local server that answers as
Amplitude's Batch Event Upload API does: the sender is answered 200 only once Amplitude
took the events, an order sent again is kept once by its insert_id, and a row Amplitude
refuses stops the stream with Amplitude's reason instead of being acknowledged.
"""
import json
import textwrap
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import pytest

from integration_tests.exported_services import (
    TOKEN,
    RunningService,
    export_service,
    service_binary,
)
from mage_ai.tests.io.fixtures_amplitude import API_KEY, FakeAmplitude

SOURCE = '''
connector_type: webhook
answer_within_seconds: 10
'''
ORDERS = '''
from typing import Dict, List


@transformer
def orders(messages: List[Dict], *args, **kwargs):
    return [
        dict(order_id=m['id'], customer_id=m['customer'], ordered_at=m['at'],
             amount=m['amount'], status=m['status'])
        for m in messages
    ]
'''
SINK = '''
connector_type: amplitude
api_key: "{{ env_var('AMPLITUDE_API_KEY') }}"
url: "{{ env_var('AMPLITUDE_URL') }}"
allow_private: true
event_type: Order Completed
columns:
  user_id: customer_id
  time: ordered_at
key_columns: [order_id]
'''


@pytest.fixture
def stream_pipeline(mage_project):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'amplitude_sink_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING,
                               repo_path=mage_project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, mage_project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', SOURCE, 'yaml')
    orders = add('orders', 'transformer', ORDERS, 'python', [source])
    add('amplitude', 'data_exporter', SINK, 'yaml', [orders])
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
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b'{}')


def order(number: int, status: str = 'paid'):
    return dict(id=number, customer=f'customer-{number}', at='2024-06-01T10:00:00Z',
                amount=19.5, status=status)


def test_webhook_orders_become_amplitude_events_once(mage_project, stream_pipeline, tmp_path):
    directory = export_service(mage_project, stream_pipeline.uuid, tmp_path)
    manifest = json.loads((directory / 'service.json').read_text())
    [sink] = manifest['pipelines'][0]['stream']['sinks']
    assert sink['kind'] == 'amplitude'
    with FakeAmplitude() as fake:
        service = RunningService(service_binary(), directory, tmp_path / 'data',
                                 AMPLITUDE_API_KEY=API_KEY, AMPLITUDE_URL=fake.url)
        service.start()
        try:
            service.wait_running()
            first = post(service, stream_pipeline.uuid, [order(1), order(2)], 'orders-1')
            # The same order changed, under another request: the same Amplitude event.
            refund = post(service, stream_pipeline.uuid, [order(2, 'refunded')], 'orders-2')
        finally:
            service.stop()

        assert first == (200, {'accepted': 2}), first
        assert refund == (200, {'accepted': 1}), refund
        assert sorted(e['user_id'] for e in fake.uploaded) == ['customer-1', 'customer-2']
        event = next(e for e in fake.uploaded if e['user_id'] == 'customer-1')
        assert event['event_type'] == 'Order Completed'
        assert event['time'] == 1717236000000
        assert event['event_properties'] == {'order_id': 1, 'amount': 19.5, 'status': 'paid'}


def test_a_row_amplitude_refuses_stops_the_stream(mage_project, stream_pipeline, tmp_path):
    directory = export_service(mage_project, stream_pipeline.uuid, tmp_path)
    with FakeAmplitude() as fake:
        service = RunningService(service_binary(), directory, tmp_path / 'data',
                                 AMPLITUDE_API_KEY=API_KEY, AMPLITUDE_URL=fake.url)
        service.start()
        try:
            service.wait_running()
            # A customer id under Amplitude's 5 characters.
            status, _ = post(service, stream_pipeline.uuid,
                             [dict(order(3), customer='c3')], 'short-1')
            stream = service.streams()[0]
        finally:
            service.stop()

        assert status in (503, 504)
        assert stream['state'] == 'failed', stream
        assert 'min_id_length' in stream['last_error'], stream
        assert fake.uploaded == []
