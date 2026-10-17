"""
An exported stream from a webhook to InfluxDB, run by mage-service: each message is a point
with its integers exact, messages without a time do not overwrite each other, and a write
InfluxDB refuses stops the stream with the reason.
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
    stream_service_binary,
)


@pytest.fixture
def measurement():
    return f'it_{uuid.uuid4().hex[:10]}'


def make_pipeline(project, influxdb, measurement, bucket=None):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'influx_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', 'connector_type: webhook\n', 'yaml')
    step = add('step', 'transformer', '''
        @transformer
        def keep(messages, *args, **kwargs):
            return messages
    ''', 'python', [source])
    add('sink', 'data_exporter', f'''
        connector_type: influxdb
        url: {influxdb['url']}
        token: "{{{{ env_var('INFLUX_TOKEN') }}}}"
        org: {influxdb['org']}
        bucket: {bucket or influxdb['bucket']}
        measurement: {measurement}
    ''', 'yaml', [step])
    return pipeline


def post(service, pipeline, messages):
    request = urllib.request.Request(
        f'http://127.0.0.1:{service.port}/v1/streams/{pipeline}/messages',
        data=json.dumps(messages).encode(), method='POST',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def run(project, influxdb, pipeline, tmp_path, *batches):
    try:
        directory = export_service(project, pipeline.uuid, tmp_path)
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 INFLUX_TOKEN=influxdb['token'])
        service.start()
        try:
            service.wait_running()
            return [post(service, pipeline.uuid, batch) for batch in batches]
        finally:
            service.stop()
    finally:
        pipeline.delete()


def fields(influxdb, measurement):
    from influxdb_client import InfluxDBClient

    with InfluxDBClient(url=influxdb['url'], token=influxdb['token'], org=influxdb['org']) as c:
        tables = c.query_api().query(
            f'from(bucket: "{influxdb["bucket"]}") |> range(start: -1h, stop: 1h) '
            f'|> filter(fn: (r) => r._measurement == "{measurement}") '
            '|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'
        )
        return [
            {k: v for k, v in record.values.items() if k in ('n', 'big', 'city', 'site')}
            for table in tables for record in table.records
        ]


def test_messages_become_points_with_exact_integers(
    mage_project, influxdb, measurement, tmp_path,
):
    pipeline = make_pipeline(mage_project, influxdb, measurement)
    statuses = run(mage_project, influxdb, pipeline, tmp_path, [
        dict(n=i, big=2**63 - 1 - i, city='Zürich "x"') for i in range(4)
    ] + [dict(data=dict(n=9), metadata=dict(tags=dict(site='a,b')))])

    assert statuses == [200]
    rows = sorted(fields(influxdb, measurement), key=lambda r: r['n'])
    assert [r['n'] for r in rows] == [0, 1, 2, 3, 9]
    assert rows[1]['big'] == 2**63 - 2
    assert rows[0]['city'] == 'Zürich "x"'
    assert rows[-1]['site'] == 'a,b'


def test_a_refused_write_stops_the_stream_with_the_reason(
    mage_project, influxdb, measurement, tmp_path,
):
    pipeline = make_pipeline(mage_project, influxdb, measurement, bucket='no_such_bucket')
    statuses = run(mage_project, influxdb, pipeline, tmp_path, [dict(n=1)])

    status, body = statuses[0]
    assert status == 503
    assert 'no_such_bucket' in body
