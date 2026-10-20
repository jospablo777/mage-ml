"""
Apache Pinot read by a Mage pipeline: a Python data loader queries the broker through the
project's io_config profile, and the block's stored output keeps Pinot's types and values
exactly (LONG digits with nulls, UTC timestamps, exact decimals, bytes, JSON, arrays). A
table that stores nulls returns them, and a query naming a column like credit_limit still
returns every row, not the broker's default 10.
"""
import datetime as dt
import json
import os
import random
import textwrap
import time
import uuid
from decimal import Decimal

import pytest
import requests

from integration_tests.mage_runner import run_pipeline

ROWS = 2500


@pytest.fixture
def controller():
    url = os.getenv('MAGE_TEST_PINOT_CONTROLLER_URL')
    if not url or not os.getenv('MAGE_TEST_PINOT_BROKER_PORT'):
        pytest.skip('Pinot is not configured: MAGE_TEST_PINOT_CONTROLLER_URL unset')
    return url.rstrip('/')


def edge_rows():
    return [
        dict(id=1, big=2 ** 53 + 1, name='Zürich ✓', flag=True, tags=['a', 'b'],
             amount='12345678901234567890.123456789', doc={'k': [1, 2]}, raw='00ff',
             ratio=0.1, score=1.5, credit_limit=100, at='2024-06-01 10:00:00.123'),
        dict(id=2, big=-(2 ** 63) + 1, name='', flag=False, tags=['c'], amount='0.000000001',
             doc={}, raw='', ratio=-1e-300, score=0.0, credit_limit=0,
             at='2000-01-01 00:00:00.000'),
        # Nulls in every column but the key and the time.
        dict(id=3, big=None, name=None, flag=None, tags=None, amount=None, doc=None, raw=None,
             ratio=None, score=None, credit_limit=None, at='2024-06-01 10:00:00.000'),
    ]


def faker_rows(count: int):
    rng = random.Random(7)
    start = dt.datetime(2024, 1, 1)
    return [
        dict(id=index, big=rng.randint(-(2 ** 62), 2 ** 62), name=f'customer {index}',
             flag=rng.random() < 0.5, tags=[f't{rng.randint(1, 9)}'],
             amount=f'{rng.randint(0, 10 ** 12)}.{rng.randint(0, 99):02d}',
             doc={'n': index}, raw=f'{index % 256:02x}', ratio=rng.random(),
             score=float(rng.randint(0, 100)), credit_limit=rng.randint(0, 5000),
             at=(start + dt.timedelta(minutes=index)).strftime('%Y-%m-%d %H:%M:%S.000'))
        for index in range(4, count + 1)
    ]


@pytest.fixture
def pinot_table(controller):
    """A table with every Pinot type and null handling, dropped after the test."""
    name = f'it_{uuid.uuid4().hex[:10]}'
    schema = {
        'schemaName': name,
        'enableColumnBasedNullHandling': True,
        'dimensionFieldSpecs': [
            {'name': 'id', 'dataType': 'INT'},
            {'name': 'big', 'dataType': 'LONG'},
            {'name': 'name', 'dataType': 'STRING'},
            {'name': 'flag', 'dataType': 'BOOLEAN'},
            {'name': 'tags', 'dataType': 'STRING', 'singleValueField': False},
            {'name': 'amount', 'dataType': 'BIG_DECIMAL'},
            {'name': 'doc', 'dataType': 'JSON'},
            {'name': 'raw', 'dataType': 'BYTES'},
            {'name': 'credit_limit', 'dataType': 'INT'},
        ],
        'metricFieldSpecs': [
            {'name': 'ratio', 'dataType': 'DOUBLE'},
            {'name': 'score', 'dataType': 'FLOAT'},
        ],
        'dateTimeFieldSpecs': [{
            'name': 'at', 'dataType': 'TIMESTAMP',
            'format': '1:MILLISECONDS:TIMESTAMP', 'granularity': '1:MILLISECONDS',
        }],
    }
    table = {
        'tableName': name, 'tableType': 'OFFLINE',
        'segmentsConfig': {'replication': '1', 'timeColumnName': 'at'},
        'tableIndexConfig': {'loadMode': 'MMAP'}, 'tenants': {}, 'metadata': {},
    }
    requests.post(f'{controller}/schemas', json=schema, timeout=30).raise_for_status()
    requests.post(f'{controller}/tables', json=table, timeout=30).raise_for_status()
    rows = edge_rows() + faker_rows(ROWS)
    try:
        response = requests.post(
            f'{controller}/ingestFromFile',
            params={
                'tableNameWithType': f'{name}_OFFLINE',
                'batchConfigMapStr': json.dumps({'inputFormat': 'json'}),
            },
            files={'file': (f'{name}.json', '\n'.join(json.dumps(row) for row in rows))},
            timeout=120,
        )
        response.raise_for_status()
        broker = f"http://127.0.0.1:{os.environ['MAGE_TEST_PINOT_BROKER_PORT']}"
        deadline = time.monotonic() + 60
        while True:
            answer = requests.post(
                f'{broker}/query/sql', json={'sql': f'SELECT COUNT(*) FROM {name}'}, timeout=30,
            ).json()
            count = (answer.get('resultTable') or {}).get('rows', [[0]])[0][0]
            if count == len(rows) or time.monotonic() > deadline:
                break
            time.sleep(1)
        assert count == len(rows), answer
        yield name, rows
    finally:
        requests.delete(f'{controller}/tables/{name}', params={'type': 'offline'}, timeout=30)
        requests.delete(f'{controller}/schemas/{name}', timeout=30)


def loader_pipeline(project, query: str):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'pinot_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, repo_path=project)
    block = Block.create(f'{name}_load', 'data_loader', project, pipeline=pipeline)
    with open(block.file_path, 'w') as file:
        file.write(textwrap.dedent(f'''\
            from os import path

            from mage_ai.io.config import ConfigFileLoader
            from mage_ai.io.pinot import Pinot
            from mage_ai.settings.repo import get_repo_path


            @data_loader
            def load(*args, **kwargs):
                config = ConfigFileLoader(path.join(get_repo_path(), 'io_config.yaml'), 'pinot')
                return Pinot.with_config(config).load({query!r})
        '''))
    return pipeline, block


def output(pipeline, block, run):
    from mage_ai.data_preparation.variable_manager import get_variable

    return get_variable(pipeline.uuid, block.uuid, 'output_0',
                        partition=run.execution_partition)


def test_a_pipeline_reads_every_pinot_type_exactly(mage_project, pinot_table):
    name, rows = pinot_table
    pipeline, block = loader_pipeline(mage_project, f'SELECT * FROM {name} ORDER BY id')
    frame = output(pipeline, block, run_pipeline(pipeline.uuid))

    assert len(frame) == len(rows)
    assert str(frame['big'].dtype) == 'Int64', frame.dtypes
    assert str(frame['id'].dtype) == 'Int32', frame.dtypes
    assert str(frame['at'].dtype) == 'datetime64[ms, UTC]', frame.dtypes
    first, second, nulls = (frame.iloc[i] for i in range(3))
    assert first['big'] == 2 ** 53 + 1
    assert second['big'] == -(2 ** 63) + 1
    assert first['name'] == 'Zürich ✓' and second['name'] == ''
    assert first['amount'] == Decimal('12345678901234567890.123456789')
    assert second['amount'] == Decimal('0.000000001')
    assert first['doc'] == {'k': [1, 2]}
    assert first['raw'] == b'\x00\xff' and second['raw'] == b''
    assert first['tags'] == ['a', 'b']
    assert first['ratio'] == 0.1 and second['ratio'] == -1e-300
    assert first['at'] == dt.datetime(2024, 6, 1, 10, 0, 0, 123000, tzinfo=dt.timezone.utc)
    for column in ('big', 'name', 'flag', 'amount', 'credit_limit'):
        assert nulls[column] is None or str(nulls[column]) in ('<NA>', 'None'), \
            (column, nulls[column])
    # Every generated row, value by value.
    by_id = {int(row['id']): row for row in frame.to_dict('records')}
    for expected in rows[3:]:
        got = by_id[expected['id']]
        assert got['big'] == expected['big']
        assert got['amount'] == Decimal(expected['amount'])
        assert got['name'] == expected['name']
        assert bool(got['flag']) == expected['flag']


def test_a_column_named_like_limit_does_not_cut_the_rows(mage_project, pinot_table):
    # The broker answers 10 rows to a query without LIMIT; a 'limit' anywhere in the query
    # used to keep MageML from adding one.
    name, rows = pinot_table
    pipeline, block = loader_pipeline(mage_project, f'SELECT id, credit_limit FROM {name}')
    frame = output(pipeline, block, run_pipeline(pipeline.uuid))
    assert len(frame) == len(rows)
