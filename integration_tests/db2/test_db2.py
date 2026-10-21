"""
IBM Db2 Community 12.1: frames written and read back keep their values (integers past 64
bits, decimals, Unicode, long text, timestamps, JSON), the write policies and MERGE upserts
behave, and SQL blocks run on Db2 with their upstream outputs as tables.
"""
import json
from datetime import datetime
from decimal import Decimal

import pandas as pd
import pytest

from integration_tests.mage_runner import execute_pipeline
from integration_tests.postgres.test_sql_block_matrix import build, error_of, output

DB2 = dict(data_provider='db2', data_provider_profile='db2')


@pytest.fixture
def client(db2_settings, db2_schema):
    from mage_ai.io.db2 import Db2

    schema, _ = db2_schema
    with Db2(**db2_settings, schema=schema, verbose=False) as loader:
        yield loader


def rows(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return [tuple(r) for r in cursor.fetchall()]


def test_a_frame_round_trips_with_exact_values(client, db2_schema):
    schema, connection = db2_schema
    df = pd.DataFrame(dict(
        id=[1, 2, 3],
        huge=[2**70, -(2**65), 0],
        price=[Decimal('0.10'), Decimal('12345678901234567890.123456789'), None],
        ratio=[0.1, 1e-300, None],
        city=['Zürich', 'São Paulo 漢字 🚀', None],
        note=['x' * 40000, '', 'y'],
        at=pd.to_datetime(['2026-01-01 12:00:00.123456', '2026-06-30 23:59:59', None],
                          format='ISO8601'),
        flag=[True, False, None],
        attrs=[{'a': [1, 'ñ']}, {}, None],
    ))
    client.export(df, schema, 'orders', if_exists='replace')
    loaded = client.load(f'SELECT * FROM {schema}.orders ORDER BY id')

    assert list(loaded.columns) == list(df.columns)
    assert [int(v) for v in loaded['huge']] == [2**70, -(2**65), 0]
    assert Decimal(str(loaded['price'][1])) == Decimal('12345678901234567890.123456789')
    assert loaded['ratio'][1] == 1e-300
    assert list(loaded['city'][:2]) == ['Zürich', 'São Paulo 漢字 🚀']
    assert len(loaded['note'][0]) == 40000
    assert loaded['at'][0] == datetime(2026, 1, 1, 12, 0, 0, 123456)
    assert list(loaded['flag'][:2]) == [True, False]
    assert json.loads(loaded['attrs'][0]) == {'a': [1, 'ñ']}
    assert pd.isna(loaded['city'][2]) and pd.isna(loaded['price'][2])
    types = dict(rows(connection, (
        f"SELECT COLNAME, TYPENAME FROM SYSCAT.COLUMNS WHERE TABSCHEMA = '{schema}' "
        "AND TABNAME = 'ORDERS'"
    )))
    assert types['HUGE'].strip() == 'DECFLOAT' and types['NOTE'].strip() == 'CLOB'


def test_append_replace_and_fail(client, db2_schema):
    schema, connection = db2_schema
    first = pd.DataFrame(dict(id=[1, 2], label=['a', 'b']))
    client.export(first, schema, 'items', if_exists='replace')
    client.export(pd.DataFrame(dict(id=[3], label=['c'])), schema, 'items', if_exists='append')
    assert rows(connection, f'SELECT id, label FROM {schema}.items ORDER BY id') == [
        (1, 'a'), (2, 'b'), (3, 'c'),
    ]
    client.export(pd.DataFrame(dict(id=[9], label=['z'])), schema, 'items',
                  if_exists='replace')
    assert rows(connection, f'SELECT id, label FROM {schema}.items') == [(9, 'z')]
    with pytest.raises(ValueError, match='already exists'):
        client.export(first, schema, 'items', if_exists='fail')


def test_upserts_update_or_keep_rows_by_their_unique_columns(client, db2_schema):
    schema, connection = db2_schema
    client.export(pd.DataFrame(dict(id=[1, 2], label=['a', 'b'])), schema, 'stock',
                  if_exists='replace', unique_constraints=['id'])
    client.export(pd.DataFrame(dict(id=[2, 3], label=['B', 'c'])), schema, 'stock',
                  if_exists='append', unique_constraints=['id'],
                  unique_conflict_method='UPDATE')
    assert rows(connection, f'SELECT id, label FROM {schema}.stock ORDER BY id') == [
        (1, 'a'), (2, 'B'), (3, 'c'),
    ]
    client.export(pd.DataFrame(dict(id=[1, 4], label=['ignored', 'd'])), schema, 'stock',
                  if_exists='append', unique_constraints=['id'],
                  unique_conflict_method='IGNORE')
    assert rows(connection, f'SELECT id, label FROM {schema}.stock ORDER BY id') == [
        (1, 'a'), (2, 'B'), (3, 'c'), (4, 'd'),
    ]


def test_a_missing_schema_is_created(db2_settings, db2_schema):
    from mage_ai.io.db2 import Db2

    schema, connection = db2_schema
    with Db2(**db2_settings, verbose=False) as loader:
        loader.export(pd.DataFrame(dict(a=[1])), schema, 'fresh')
    assert rows(connection, f'SELECT a FROM {schema}.fresh') == [(1,)]


def test_sql_blocks_run_on_db2_with_their_upstream_tables(mage_project, db2_schema):
    schema, connection = db2_schema
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code='''
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'id': [1, 2, 3], 'amount': [10, 20, 30],
                                     'city': ['Zürich', 'Bern', 'Basel']})
        '''),
        dict(
            uuid='big', type='transformer', language='sql', upstream=['load'],
            config=dict(DB2, export_write_policy='replace'),
            code='SELECT id, amount * 2 AS doubled, city FROM {{ df_1 }} WHERE amount > 10',
        ),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['big'],
            config=dict(DB2, data_provider_table='doubled', export_write_policy='append'),
            code='SELECT id, doubled, city FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)

    frame = output(pipeline, run, 'big')
    assert sorted(frame['doubled'].tolist()) == [40, 60]
    assert rows(connection, f'SELECT id, doubled, city FROM {schema}.doubled ORDER BY id') == [
        (2, 40, 'Bern'), (3, 60, 'Basel'),
    ]


def test_an_exported_service_runs_the_db2_sql_blocks_as_mageml(mage_project, db2_schema,
                                                                tmp_path):
    from integration_tests.exported_services import run_exported

    schema, connection = db2_schema
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code='''
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'id': [1, 2, 3], 'amount': [10, 20, 30],
                                     'city': ['Zürich', 'Bern', 'Basel']})
        '''),
        dict(
            uuid='big', type='transformer', language='sql', upstream=['load'],
            config=dict(DB2, export_write_policy='replace'),
            code='SELECT id, amount * 2 AS doubled, city FROM {{ df_1 }} WHERE amount > 10',
        ),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['big'],
            config=dict(DB2, data_provider_table='service_doubled',
                        export_write_policy='replace'),
            code='SELECT id, doubled, city FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    query = f'SELECT id, doubled, city FROM {schema}.service_doubled ORDER BY id'
    expected = rows(connection, query)
    with connection.cursor() as cursor:
        cursor.execute(f'DROP TABLE {schema}.service_doubled')
    # The connection does not commit on its own: the drop held the table from the service.
    connection.commit()

    run_exported(mage_project, pipeline, tmp_path)

    assert rows(connection, query) == expected == [(2, 40, 'Bern'), (3, 60, 'Basel')]
