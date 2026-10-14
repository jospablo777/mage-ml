"""
An exported stream that writes to PostgreSQL (TLS, private CA): the table is made from the
messages with MageML's column names, values keep every digit (NUMERIC, JSONB, Unicode),
upserts keep the last message of a key, a new field adds a column, and a value that does
not fit its column reaches the webhook's caller as an error, not a cut value.
"""
import json
import textwrap
import urllib.error
import urllib.request
import uuid
from decimal import Decimal
from pathlib import Path

import psycopg2
import pytest

from integration_tests.exported_services import (
    TOKEN,
    RunningService,
    export_service,
    stream_service_binary,
)
from integration_tests.tls_certs import CERTS

TRANSFORM = '''
from typing import Dict, List


@transformer
def passthrough(messages: List[Dict], *args, **kwargs):
    return messages
'''


@pytest.fixture
def schema(postgres_settings):
    name = f'it_stream_{uuid.uuid4().hex[:8]}'
    yield name
    connection = psycopg2.connect(**postgres_settings)
    with connection, connection.cursor() as cursor:
        cursor.execute(f'DROP SCHEMA IF EXISTS {name} CASCADE')
    connection.close()


def sink_yaml(postgres_settings, schema, **extra):
    settings = dict(
        connector_type='postgres', host=postgres_settings['host'],
        port=int(postgres_settings['port']), database=postgres_settings['dbname'],
        username=postgres_settings['user'], password="{{ env_var('PG_PASSWORD') }}",
        schema=schema, table='orders', sslmode='verify-ca',
        sslrootcert=str(CERTS / 'ca.pem'), **extra,
    )
    return '\n'.join(f'{k}: {json.dumps(v)}' for k, v in settings.items())


def make_pipeline(project, sink):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'pg_sink_{uuid.uuid4().hex[:8]}'
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
    add('sink', 'data_exporter', sink, 'yaml', [step])
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


def running(project, tmp_path, postgres_settings, sink):
    pipeline = make_pipeline(project, sink)
    directory = export_service(project, pipeline.uuid, tmp_path)
    service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                             PG_PASSWORD=postgres_settings['password'])
    return pipeline, service


def rows(postgres_settings, sql):
    connection = psycopg2.connect(**postgres_settings)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()
    finally:
        connection.close()


def test_a_table_is_made_and_values_keep_every_digit(
    mage_project, tmp_path, postgres_settings, schema,
):
    pipeline, service = running(mage_project, tmp_path, postgres_settings,
                                sink_yaml(postgres_settings, schema))
    try:
        service.start()
        service.wait_running()
        status = post(service, pipeline.uuid, [
            {'Order ID': 1, 'big': 2**70 + 1, 'price': 0.1, 'city': 'Zürich 🚀',
             'attrs': {'n': 2**65, 'tags': ['a']}, 'paid': True, 'select': 'x'},
            {'Order ID': 2, 'big': None, 'price': 2, 'city': None, 'attrs': None,
             'paid': False, 'select': 'y'},
        ])
        # A field the table does not have adds a column.
        status_2 = post(service, pipeline.uuid, [{'Order ID': 3, 'note': 'new column'}])
    finally:
        service.stop()
        pipeline.delete()

    assert (status, status_2) == (200, 200)
    columns = dict(rows(postgres_settings, (
        "SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = 'orders'"
    )))
    assert columns['order_id'] == 'bigint' and columns['_select'] == 'text'
    assert columns['big'] == 'numeric' and columns['attrs'] == 'jsonb'
    assert columns['note'] == 'text'
    found = rows(postgres_settings, (
        f'SELECT order_id, big, price, city, attrs, paid, note FROM {schema}.orders '
        'ORDER BY order_id'
    ))
    assert found[0][:4] == (1, Decimal(2**70 + 1), 0.1, 'Zürich 🚀')
    assert found[0][4] == {'n': 2**65, 'tags': ['a']}
    assert found[1][1] is None and found[1][5] is False
    assert found[2][0] == 3 and found[2][6] == 'new column'


def test_upserts_keep_the_last_message_of_each_key(
    mage_project, tmp_path, postgres_settings, schema,
):
    sink = sink_yaml(postgres_settings, schema, unique_constraints=['id'],
                     unique_conflict_method='UPDATE')
    pipeline, service = running(mage_project, tmp_path, postgres_settings, sink)
    try:
        service.start()
        service.wait_running()
        first = post(service, pipeline.uuid, [
            {'id': 1, 'state': 'new'}, {'id': 2, 'state': 'new'}, {'id': 1, 'state': 'paid'},
        ])
        # Sent again, as after a restart: nothing is written twice.
        again = post(service, pipeline.uuid, [{'id': 2, 'state': 'shipped'}, {'id': 3,
                                                                            'state': 'new'}])
    finally:
        service.stop()
        pipeline.delete()

    assert (first, again) == (200, 200)
    # state is on MageML's reserved word list: the column is _state, as MageML names it.
    assert rows(postgres_settings, f'SELECT id, _state FROM {schema}.orders ORDER BY id') == [
        (1, 'paid'), (2, 'shipped'), (3, 'new'),
    ]


def test_a_value_that_does_not_fit_its_column_is_refused(
    mage_project, tmp_path, postgres_settings, schema,
):
    pipeline, service = running(mage_project, tmp_path, postgres_settings,
                                sink_yaml(postgres_settings, schema))
    try:
        service.start()
        service.wait_running()
        ok = post(service, pipeline.uuid, [{'id': 1}])
        refused = post(service, pipeline.uuid, [{'id': 'not a number'}])
        stream = service.streams()[0]
    finally:
        service.stop()
        pipeline.delete()

    assert ok == 200 and refused in (503, 504)
    assert 'invalid input syntax for type bigint' in (stream['last_error'] or ''), stream
    assert rows(postgres_settings, f'SELECT id FROM {schema}.orders') == [(1,)]


def test_verify_full_checks_the_server_name(mage_project, tmp_path, postgres_settings, schema):
    # The test server's certificate names db.test, not the address the sink connects to.
    sink = sink_yaml(postgres_settings, schema).replace('"verify-ca"', '"verify-full"')
    pipeline, service = running(mage_project, tmp_path, postgres_settings, sink)
    try:
        service.start()
        service.wait_running()
        status = post(service, pipeline.uuid, [{'id': 1}])
        stream = service.streams()[0]
    finally:
        service.stop()
        pipeline.delete()

    assert status in (503, 504)
    assert 'certificate' in (stream['last_error'] or '').lower(), stream
