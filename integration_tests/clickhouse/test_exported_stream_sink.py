"""
An exported stream that writes to ClickHouse with MageML's ClickHouse sink settings (an
io_config.yaml profile and config.table_name): the table is made from the messages, values
keep every digit (Int256 for integers past 64 bits) and their Unicode, objects become JSON
text, a new field adds a column, and a value that does not fit its column is refused.
"""
import json
import textwrap
import urllib.error
import urllib.request
import uuid
from pathlib import Path

from integration_tests.exported_services import (
    TOKEN,
    RunningService,
    export_service,
    stream_service_binary,
)

SINK = '''
connector_type: clickhouse
profile: clickhouse
config:
  table_name: events
'''
TRANSFORM = '''
from typing import Dict, List


@transformer
def passthrough(messages: List[Dict], *args, **kwargs):
    return messages
'''


def make_pipeline(project):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'ch_sink_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', 'connector_type: webhook\nanswer_within_seconds: 20\n',
                 'yaml')
    step = add('pass', 'transformer', TRANSFORM, 'python', [source])
    add('sink', 'data_exporter', SINK, 'yaml', [step])
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
        return error.code


def test_a_stream_writes_exact_values_to_clickhouse(mage_project, tmp_path, ch):
    pipeline = make_pipeline(mage_project)
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data')
        service.start()
        try:
            service.wait_running()
            first = post(service, pipeline.uuid, [
                {'Event ID': 1, 'big': 2**70 + 1, 'price': 0.1, 'city': 'Zürich 🚀',
                 'attrs': {'n': 2**65}, 'ok': True},
                {'Event ID': 2, 'big': None, 'price': 2, 'city': None, 'attrs': None,
                 'ok': False},
            ])
            second = post(service, pipeline.uuid, [{'Event ID': 3, 'note': 'new column'}])
            refused = post(service, pipeline.uuid, [{'Event ID': 'not a number'}])
            stream = service.streams()[0]
        finally:
            service.stop()
    finally:
        pipeline.delete()

    assert (first, second) == (200, 200)
    assert refused in (503, 504)
    assert 'ClickHouse' in (stream['last_error'] or ''), stream
    types = dict(ch.query(
        "SELECT name, type FROM system.columns WHERE database = currentDatabase() "
        "AND table = 'events'",
    ).result_rows)
    assert types['event_id'] == 'Nullable(Int64)' and types['big'] == 'Nullable(Int256)'
    assert types['note'] == 'Nullable(String)'
    rows = ch.query(
        'SELECT event_id, toString(big), price, city, attrs, ok, note FROM events '
        'ORDER BY event_id',
    ).result_rows
    assert rows[0][:4] == (1, str(2**70 + 1), 0.1, 'Zürich 🚀')
    assert json.loads(rows[0][4]) == {'n': 2**65}
    assert rows[1][3] is None and rows[1][5] is False
    assert rows[2][0] == 3 and rows[2][6] == 'new column'
    assert len(rows) == 3
