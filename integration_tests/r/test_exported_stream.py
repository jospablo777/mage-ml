"""
A stream whose transformers are R blocks, exported and run by mage-service: webhook
messages reach an R block as a tibble (integers past 2^53 kept exactly as integer64), its
rows reach PostgreSQL, a Python block after it runs in turn, and an R error is the batch's
error with the block's name.
"""
import json
import textwrap
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import psycopg2
import pytest

from integration_tests.exported_services import (
    TOKEN,
    RunningService,
    export_service,
    stream_service_binary,
)

PRICE = '''
#* @transformer
transform <- function(messages, ...) {
  rate <- variable("rate", 1)
  cat("pricing", nrow(messages), "messages\\n")
  if (any(messages$amount < 0)) stop("negative amount")
  messages$total <- messages$amount * rate
  messages[messages$total > 2, ]
}
'''
LABEL = '''
@transformer
def label(messages, *args, **kwargs):
    return [dict(m, label='python after r') for m in messages]
'''


@pytest.fixture
def schema(postgres_settings):
    name = f'it_rstream_{uuid.uuid4().hex[:8]}'
    yield name
    connection = psycopg2.connect(**postgres_settings)
    with connection, connection.cursor() as cursor:
        cursor.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
    connection.close()


def make_pipeline(project, postgres_settings, schema):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'r_stream_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)
    pipeline.variables = dict(rate=2)
    pipeline.save()

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', 'connector_type: webhook\nanswer_within_seconds: 60\n',
                 'yaml')
    price = add('price', 'transformer', PRICE, 'r', [source])
    label = add('label', 'transformer', LABEL, 'python', [price])
    sink = '\n'.join(f'{k}: {json.dumps(v)}' for k, v in dict(
        connector_type='postgres', host=postgres_settings['host'],
        port=int(postgres_settings['port']), database=postgres_settings['dbname'],
        username=postgres_settings['user'], password="{{ env_var('PG_PASSWORD') }}",
        schema=schema, table='orders', sslmode='disable',
    ).items())
    add('sink', 'data_exporter', sink, 'yaml', [label])
    return pipeline


def post(service, pipeline, messages):
    request = urllib.request.Request(
        f'http://127.0.0.1:{service.port}/v1/streams/{pipeline}/messages',
        data=json.dumps(messages).encode(), method='POST',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def test_r_blocks_transform_a_stream_with_python_after_them(
    mage_project, postgres_settings, schema, tmp_path,
):
    pipeline = make_pipeline(mage_project, postgres_settings, schema)
    try:
        directory = export_service(mage_project, pipeline.uuid, tmp_path)
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 PG_PASSWORD=postgres_settings['password'])
        service.start()
        try:
            service.wait_running()
            ok = post(service, pipeline.uuid, [
                dict(id=1, amount=1, big=2**60 + 1), dict(id=2, amount=3, big=2**62 + 3),
                dict(id=3, amount=5, city='Zürich 🚀'),
            ])
            refused = post(service, pipeline.uuid, [dict(id=4, amount=-1)])
            stream = service.streams()[0]
            log = service.log.read_text()
        finally:
            service.stop()
    finally:
        pipeline.delete()

    assert ok == 200
    assert refused in (503, 504)
    assert 'negative amount' in (stream['last_error'] or ''), stream
    assert 'pricing 3 messages' in log
    connection = psycopg2.connect(**postgres_settings)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'SELECT id, big, total, city, label FROM {schema}.orders ORDER BY id')
            rows = cursor.fetchall()
    finally:
        connection.close()
    assert rows == [
        (2, 2**62 + 3, 6, None, 'python after r'),
        (3, None, 10, 'Zürich 🚀', 'python after r'),
    ]
