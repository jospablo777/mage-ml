"""
PostgreSQL SQL blocks across their configurations: write policies, table and schema
names, raw SQL, templates, limits, row counts, column names and values that break
naive SQL, in pipelines with pandas and Polars blocks. Each test builds its pipeline in
the session's project and checks what Mage stored in PostgreSQL and what the Python
blocks received.
"""
import json
import textwrap
import uuid
from pathlib import Path
from typing import Dict, List, Optional

import pytest

from integration_tests.mage_runner import execute_pipeline, run_pipeline

PG = dict(data_provider='postgres', data_provider_profile='test_schema')


def build(project: str, blocks: List[Dict]) -> str:
    """
    A pipeline of blocks, each {uuid, type, code, language (python), config, upstream};
    returns its uuid. Block uuids get the pipeline's suffix, so tests do not share files.
    """
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    suffix = uuid.uuid4().hex[:8]
    pipeline = Pipeline.create(f'matrix_{suffix}', repo_path=project)
    names = {spec['uuid']: f'{spec["uuid"]}_{suffix}' for spec in blocks}
    for spec in blocks:
        block = Block.create(
            names[spec['uuid']],
            spec['type'],
            project,
            pipeline=pipeline,
            language=spec.get('language', 'python'),
            configuration=spec.get('config'),
            upstream_block_uuids=[names[u] for u in spec.get('upstream', [])],
        )
        code = textwrap.dedent(spec['code']).strip() + '\n'
        for original, renamed in names.items():
            code = code.replace(f'@{original}@', renamed)
        Path(block.file_path).write_text(code)
    return pipeline.uuid


def block_uuid(pipeline_uuid: str, name: str) -> str:
    return f'{name}_{pipeline_uuid.split("_")[-1]}'


def query(pg, sql: str, params=None):
    with pg.cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall() if cursor.description else None
    pg.rollback()
    return rows


def execute(pg, sql: str) -> None:
    with pg.cursor() as cursor:
        cursor.execute(sql)
    pg.commit()


def output(pipeline_uuid: str, run, name: str):
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.get(pipeline_uuid)
    return pipeline.variable_manager.get_variable(
        pipeline_uuid, block_uuid(pipeline_uuid, name), 'output_0',
        partition=run.execution_partition,
    )


def error_of(run, name: Optional[str] = None) -> str:
    for block_run in run.block_runs:
        if name and not block_run.block_uuid.startswith(name):
            continue
        error = (block_run.metrics or {}).get('error') or {}
        if error:
            return json.dumps(error)
    return str(run.executor_error or '')


PANDAS_ROWS = '''
import pandas as pd


@data_loader
def load(*args, **kwargs):
    rows = int(kwargs.get('rows', 3))
    return pd.DataFrame({
        'id': pd.Series(range(1, rows + 1), dtype='Int64'),
        'name': pd.Series([f'n{i}' for i in range(1, rows + 1)], dtype='string'),
    })
'''


# ---- write policies -------------------------------------------------------------------


@pytest.mark.parametrize('policy', ['replace', 'append', 'fail'])
def test_export_write_policies_over_two_runs(mage_project, pg, profile_schema, policy):
    table = f'policy_{policy}'
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(PG, data_provider_table=table, export_write_policy=policy),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    run_pipeline(pipeline, rows=3)
    second = execute_pipeline(pipeline, rows=4)

    count = query(pg, f'SELECT count(*) FROM {profile_schema}.{table}')[0][0]
    if policy == 'replace':
        assert second.status.value == 'completed', error_of(second)
        assert count == 4
    elif policy == 'append':
        assert second.status.value == 'completed', error_of(second)
        assert count == 7
    else:
        assert second.status.value == 'failed'
        assert 'exists' in error_of(second, 'export').lower()
        assert count == 3


# ---- table and schema names ------------------------------------------------------------


@pytest.mark.parametrize('table, stored', [
    # Plain names fold to lower case, as SQL that writes them unquoted reads them.
    ('MixedCase', 'mixedcase'),
    ('with space', 'with space'),
    ('2024-sales', '2024-sales'),
    ('select', 'select'),
    ('ñandú', 'ñandú'),
])
def test_tables_whose_names_need_quotes(mage_project, pg, profile_schema, table, stored):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(PG, data_provider_table=table, export_write_policy='replace'),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline, rows=3)
    assert run.status.value == 'completed', error_of(run)
    # A second run replaces the table it made.
    run = execute_pipeline(pipeline, rows=4)
    assert run.status.value == 'completed', error_of(run)
    assert query(pg, f'SELECT count(*) FROM {profile_schema}."{stored}"')[0][0] == 4


def test_long_block_names_do_not_share_a_table(mage_project, pg, profile_schema):
    """Postgres cuts identifiers at 63 bytes; two blocks must not write one table."""
    long_a = 'transform_' + 'a' * 60
    long_b = 'transform_' + 'a' * 59 + 'b'
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
        dict(
            uuid=long_a, type='transformer', language='sql', upstream=['load'],
            config=dict(PG, export_write_policy='replace'),
            code='SELECT id, 1 AS which FROM {{ df_1 }}',
        ),
        dict(
            uuid=long_b, type='transformer', language='sql', upstream=['load'],
            config=dict(PG, export_write_policy='replace'),
            code='SELECT id, 2 AS which FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline, rows=3)
    assert run.status.value == 'completed', error_of(run)
    first = output(pipeline, run, long_a)
    second = output(pipeline, run, long_b)
    assert first['which'].tolist() == [1, 1, 1]
    assert second['which'].tolist() == [2, 2, 2]


def test_a_schema_other_than_the_profiles(mage_project, pg, profile_schema):
    other = f'{profile_schema}_other'
    execute(pg, f'CREATE SCHEMA {other}')
    try:
        pipeline = build(mage_project, [
            dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
            dict(
                uuid='export', type='data_exporter', language='sql', upstream=['load'],
                config=dict(
                    PG, data_provider_schema=other, data_provider_table='dst',
                    export_write_policy='replace',
                ),
                code='SELECT * FROM {{ df_1 }}',
            ),
        ])
        run = execute_pipeline(pipeline, rows=2)
        assert run.status.value == 'completed', error_of(run)
        assert query(pg, f'SELECT count(*) FROM {other}.dst')[0][0] == 2
    finally:
        execute(pg, f'DROP SCHEMA {other} CASCADE')


# ---- row counts --------------------------------------------------------------------------


def test_a_sql_loader_passes_every_row_to_pandas(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(
            uuid='load', type='data_loader', language='sql', config=dict(PG),
            code='SELECT g AS id, md5(g::text) AS digest FROM generate_series(1, 25000) g',
        ),
        dict(uuid='count', type='transformer', upstream=['load'], code='''
            @transformer
            def count(frame, *args, **kwargs):
                assert len(frame) == 25000, len(frame)
                assert frame['id'].is_unique
                return frame.head(1)
        '''),
    ])
    run_pipeline(pipeline)


def test_raw_sql_that_creates_a_table_passes_every_row_on(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(
            uuid='create', type='transformer', language='sql',
            config=dict(PG, use_raw_sql=True),
            code=f'''
                DROP TABLE IF EXISTS {profile_schema}.raw_created;
                CREATE TABLE {profile_schema}.raw_created AS
                SELECT g AS id FROM generate_series(1, 5000) g;
            ''',
        ),
        dict(uuid='count', type='transformer', upstream=['create'], code='''
            @transformer
            def count(frame, *args, **kwargs):
                assert len(frame) == 5000, len(frame)
                return frame.head(1)
        '''),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)


def test_an_empty_frame_becomes_an_empty_table(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code='''
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({
                    'id': pd.Series([], dtype='Int64'),
                    'name': pd.Series([], dtype='string'),
                    'at': pd.Series([], dtype='datetime64[us, UTC]'),
                })
        '''),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(PG, data_provider_table='empty_dst', export_write_policy='replace'),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    columns = dict(query(
        pg,
        'SELECT column_name, data_type FROM information_schema.columns '
        'WHERE table_schema = %s AND table_name = %s',
        (profile_schema, 'empty_dst'),
    ))
    assert columns == {
        'id': 'bigint', 'name': 'text', 'at': 'timestamp with time zone',
    }
    assert query(pg, f'SELECT count(*) FROM {profile_schema}.empty_dst')[0][0] == 0


def test_the_pipeline_run_limit_caps_what_downstream_blocks_get(
    mage_project, pg, profile_schema,
):
    pipeline = build(mage_project, [
        dict(
            uuid='load', type='data_loader', language='sql',
            config=dict(PG, limit_in_pipeline_run=7),
            code='SELECT g AS id FROM generate_series(1, 100) g ORDER BY g',
        ),
        dict(uuid='count', type='transformer', upstream=['load'], code='''
            @transformer
            def count(frame, *args, **kwargs):
                assert frame['id'].tolist() == list(range(1, 8)), frame
                return frame
        '''),
    ])
    run_pipeline(pipeline)


# ---- templates and statements ------------------------------------------------------------


def test_templates_read_variables_dates_and_several_upstream_blocks(
    mage_project, pg, profile_schema,
):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
        dict(uuid='other', type='data_loader', code='''
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'id': [1, 2], 'score': [0.5, 1.5]})
        '''),
        dict(
            uuid='join', type='transformer', language='sql', upstream=['load', 'other'],
            config=dict(PG, export_write_policy='replace'),
            code='''
                -- A comment; with a semicolon.
                WITH scored AS (
                    SELECT a.id, a.name, b.score
                    FROM {{ df_1 }} a JOIN {{ df_2 }} b ON a.id = b.id
                )
                SELECT
                    id, name, score,
                    '{{ variables("label") }}' AS label,
                    '{{ ds }}'::date AS day,
                    $$it's; quoted$$ AS quoted
                FROM scored
                WHERE score > {{ variables("threshold") }}
            ''',
        ),
    ])
    # Templates insert values as they are: a quote in a value is the SQL's to escape.
    run = execute_pipeline(pipeline, rows=3, label='obrien', threshold=1)
    assert run.status.value == 'completed', error_of(run)
    frame = output(pipeline, run, 'join')
    assert frame['id'].tolist() == [2]
    assert frame['label'].tolist() == ['obrien']
    assert frame['quoted'].tolist() == ["it's; quoted"]
    assert str(frame['day'].iloc[0])[:10] == run.execution_date.strftime('%Y-%m-%d')


def test_raw_sql_runs_every_statement_in_order(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(
            uuid='statements', type='data_exporter', language='sql',
            config=dict(PG, use_raw_sql=True),
            code=f'''
                CREATE TABLE IF NOT EXISTS {profile_schema}.log (id int, note text);
                INSERT INTO {profile_schema}.log VALUES (1, 'semi;colon');
                INSERT INTO {profile_schema}.log VALUES (2, $$dollar; quoted$$);
                -- INSERT INTO {profile_schema}.log VALUES (3, 'commented out');
                UPDATE {profile_schema}.log SET note = note || '!' WHERE id = 1;
            ''',
        ),
    ])
    run_pipeline(pipeline)
    assert query(pg, f'SELECT id, note FROM {profile_schema}.log ORDER BY id') == [
        (1, 'semi;colon!'), (2, 'dollar; quoted'),
    ]


def test_a_sql_block_reads_the_table_of_an_upstream_sql_block(
    mage_project, pg, profile_schema,
):
    pipeline = build(mage_project, [
        dict(
            uuid='first', type='data_loader', language='sql',
            config=dict(PG, export_write_policy='replace'),
            code='SELECT g AS id, g * 10 AS value FROM generate_series(1, 4) g',
        ),
        dict(
            uuid='second', type='transformer', language='sql', upstream=['first'],
            config=dict(PG, export_write_policy='replace'),
            code='SELECT id, value + 1 AS value FROM {{ df_1 }} ORDER BY id',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    assert output(pipeline, run, 'second')['value'].tolist() == [11, 21, 31, 41]


# ---- column names and values -------------------------------------------------------------


def test_column_names_that_need_quotes_reach_sql_and_back(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code='''
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({
                    'id': [1, 2],
                    'MixedCase': [1, 2],
                    'with space': ['a', 'b'],
                    'select': [True, False],
                    'ünïcode': [1.5, 2.5],
                })
        '''),
        dict(
            uuid='pick', type='transformer', language='sql', upstream=['load'],
            config=dict(PG, export_write_policy='replace'),
            code='SELECT "MixedCase", "with space", "select", "ünïcode" FROM {{ df_1 }} '
                 'ORDER BY id',
        ),
        dict(uuid='check', type='transformer', upstream=['pick'], code='''
            @transformer
            def check(frame, *args, **kwargs):
                assert list(frame.columns) == ['MixedCase', 'with space', 'select', 'ünïcode']
                assert frame['with space'].tolist() == ['a', 'b']
                return frame
        '''),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)


def test_values_that_break_naive_sql(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=r'''
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({
                    'id': [1, 2, 3, 4, 5],
                    'text': ["o'brien", 'back\\slash', 'line\nbreak', 'tab\tand "quotes"',
                             "'; DROP TABLE x; --"],
                    'emptyish': ['', ' ', None, 'NULL', '\\N'],
                })
        '''),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(PG, data_provider_table='tricky', export_write_policy='replace'),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    assert query(pg, f'SELECT id, text, emptyish FROM {profile_schema}.tricky ORDER BY id') == [
        (1, "o'brien", ''),
        (2, 'back\\slash', ' '),
        (3, 'line\nbreak', None),
        (4, 'tab\tand "quotes"', 'NULL'),
        (5, "'; DROP TABLE x; --", '\\N'),
    ]


def test_pandas_types_become_postgres_types(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code='''
            import datetime as dt
            import decimal
            import uuid

            import numpy as np
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({
                    'id': [1, 2, 3],
                    'i64': pd.array([9223372036854775807, None, -9223372036854775808],
                                    dtype='Int64'),
                    'f64': [1.5, np.nan, float('inf')],
                    'flag': pd.array([True, None, False], dtype='boolean'),
                    'text': pd.Series(['a', None, 'ü'], dtype='string'),
                    'naive': pd.to_datetime(
                        ['2024-01-01 10:00', None, '1900-01-01 00:00'], format='ISO8601',
                    ),
                    'aware': pd.to_datetime(
                        ['2024-03-31 01:30', None, '2024-10-27 02:30'], format='ISO8601',
                    ).tz_localize('UTC'),
                    'day': [dt.date(2024, 2, 29), None, dt.date(1, 1, 1)],
                    'amount': [decimal.Decimal('12345678901234567890.123456789'), None,
                               decimal.Decimal('-0.000000001')],
                    'key': [uuid.UUID(int=1), None, uuid.UUID(int=2**128 - 1)],
                    'tags': [['a', 'b'], None, []],
                    'doc': [{'k': [1, None]}, None, {}],
                    'blob': [b'\\x00\\xff', None, b''],
                })
        '''),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(PG, data_provider_table='typed', export_write_policy='replace'),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    rows = query(pg, f'''
        SELECT id, i64, f64::text, flag, text, naive::text, aware AT TIME ZONE 'UTC',
               day::text, amount::text, key::text, tags::text, doc::text, blob
        FROM {profile_schema}.typed ORDER BY id
    ''')
    assert rows[0][:5] == (1, 9223372036854775807, '1.5', True, 'a')
    assert rows[1][1:5] == (None, None, None, None)
    assert rows[2][1:5] == (-9223372036854775808, 'Infinity', False, 'ü')
    assert rows[0][5] == '2024-01-01 10:00:00'
    assert rows[2][5] == '1900-01-01 00:00:00'
    assert str(rows[2][6]) == '2024-10-27 02:30:00'
    assert rows[0][7] == '2024-02-29' and rows[2][7] == '0001-01-01'
    assert rows[0][8] == '12345678901234567890.123456789'
    assert rows[2][8] == '-0.000000001'
    assert rows[2][9] == 'ffffffff-ffff-ffff-ffff-ffffffffffff'
    assert json.loads(rows[0][11]) == {'k': [1, None]}
    assert bytes(rows[0][12]) == b'\x00\xff' and bytes(rows[2][12]) == b''
    assert rows[1][7:13] == (None, None, None, None, None, None)
