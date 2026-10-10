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


# ---- more configurations -----------------------------------------------------------------


def test_polars_frames_reach_sql_with_their_types(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code='''
            import datetime as dt

            import polars as pl


            @data_loader
            def load(*args, **kwargs):
                return pl.DataFrame({
                    'id': [1, 2, 3],
                    'big': pl.Series([2**63 - 1, None, -(2**63)], dtype=pl.Int64),
                    'u': pl.Series([0, 2**32, None], dtype=pl.UInt64),
                    'f': [1.25, None, float('nan')],
                    'when': [dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc), None,
                             dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)],
                    'day': [dt.date(2024, 2, 29), None, dt.date(1999, 12, 31)],
                    'tags': [['a'], None, []],
                    'cat': pl.Series(['x', None, 'y'], dtype=pl.Categorical),
                })
        '''),
        dict(
            uuid='pick', type='transformer', language='sql', upstream=['load'],
            config=dict(PG, export_write_policy='replace'),
            code='SELECT id, big, u, f, "when", day, cardinality(tags) AS n, cat '
                 'FROM {{ df_1 }} ORDER BY id',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    frame = output(pipeline, run, 'pick')
    assert frame['big'].tolist()[0] == 2**63 - 1 and frame['big'].tolist()[2] == -(2**63)
    assert frame['u'].tolist()[1] == 2**32
    assert frame['n'].tolist()[0] == 1 and frame['n'].tolist()[2] == 0
    assert frame['cat'].tolist()[0] == 'x' and frame['cat'].isna().tolist()[1]
    assert str(frame['day'].iloc[0])[:10] == '2024-02-29'


def test_the_profile_can_come_from_a_variable(mage_project, pg, profile_schema):
    """The profile names the schema exports go to (it does not set the search path)."""
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(
                data_provider='postgres',
                data_provider_profile="{{ variables('profile') }}",
                data_provider_table='from_variable_profile',
                export_write_policy='replace',
            ),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline, profile='test_schema', rows=2)
    assert run.status.value == 'completed', error_of(run)
    assert query(
        pg, f'SELECT count(*) FROM {profile_schema}.from_variable_profile',
    )[0][0] == 2


def test_raw_sql_without_preprocessing_runs_as_one_script(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(
            uuid='script', type='data_exporter', language='sql',
            config=dict(PG, use_raw_sql=True, disable_query_preprocessing=True),
            code=f'''
                CREATE TABLE {profile_schema}.scripted (id int);
                INSERT INTO {profile_schema}.scripted SELECT generate_series(1, 3);
                DO $$ BEGIN
                    INSERT INTO {profile_schema}.scripted VALUES (4);
                END $$;
            ''',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    assert query(pg, f'SELECT count(*) FROM {profile_schema}.scripted')[0][0] == 4


def test_null_only_and_wide_frames(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code='''
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                frame = pd.DataFrame({'id': [1, 2], 'nothing': [None, None]})
                for index in range(300):
                    frame[f'c{index}'] = [index, index + 1]
                return frame
        '''),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(PG, data_provider_table='wide', export_write_policy='replace'),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    assert query(pg, f'SELECT nothing, c299 FROM {profile_schema}.wide ORDER BY id') == [
        (None, 299), (None, 300),
    ]


def test_duplicate_column_names_in_a_result_are_explained(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(
            uuid='load', type='data_loader', language='sql',
            config=dict(PG, export_write_policy='replace'),
            code='SELECT 1 AS a, 2 AS a',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'failed'
    assert 'a' in error_of(run, 'load')
    assert 'specified more than once' in error_of(run, 'load')


def test_a_nul_character_fails_with_the_column_named(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code='''
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'id': [1], 'text': ['a\\x00b']})
        '''),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(PG, data_provider_table='nul', export_write_policy='replace'),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'failed'
    message = error_of(run, 'export')
    assert 'NUL' in message or '0x00' in message, message


def test_replacing_a_table_a_view_depends_on_says_so(mage_project, pg, profile_schema):
    table = 'viewed'
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(PG, data_provider_table=table, export_write_policy='replace'),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    run_pipeline(pipeline, rows=2)
    execute(pg, f'CREATE VIEW {profile_schema}.v AS SELECT * FROM {profile_schema}.{table}')
    run = execute_pipeline(pipeline, rows=3)
    assert run.status.value == 'failed'
    assert 'depend' in error_of(run, 'export')
    assert query(pg, f'SELECT count(*) FROM {profile_schema}.v')[0][0] == 2


def test_an_upstream_dict_becomes_a_one_row_table(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code="""
            @data_loader
            def load(*args, **kwargs):
                return {'name': 'a table', 'size': 3}
        """),
        dict(
            uuid='use', type='transformer', language='sql', upstream=['load'],
            config=dict(PG, export_write_policy='replace'),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    frame = output(pipeline, run, 'use')
    assert frame.to_dict('records') == [{'name': 'a table', 'size': 3}]


def test_sql_loader_types_reach_pandas(mage_project, pg, profile_schema, source_table):
    pipeline = build(mage_project, [
        dict(
            uuid='load', type='data_loader', language='sql', config=dict(PG),
            code=f'SELECT * FROM {profile_schema}.{source_table} ORDER BY id',
        ),
        dict(uuid='check', type='transformer', upstream=['load'], code='''
            import datetime as dt
            import uuid

            import pandas as pd


            @transformer
            def check(frame, *args, **kwargs):
                problems = []
                expect = {
                    'c_smallint': 'Int64', 'c_integer': 'Int64', 'c_bigint': 'Int64',
                    'c_bool': ('boolean', 'bool', 'object'),
                }
                for column, dtypes in expect.items():
                    if str(frame[column].dtype) not in (
                        dtypes if isinstance(dtypes, tuple) else (dtypes,)
                    ):
                        problems.append(f'{column}: {frame[column].dtype}')
                value = frame['c_timestamptz'].dropna().iloc[0]
                if getattr(value, 'tzinfo', None) is None:
                    problems.append(f'c_timestamptz has no time zone: {value!r}')
                value = frame['c_jsonb'].dropna().iloc[0]
                if not isinstance(value, (dict, list, str, int, float, bool)):
                    problems.append(f'c_jsonb: {type(value)}')
                value = frame['c_int_array'].dropna().iloc[0]
                if not hasattr(value, '__len__'):
                    problems.append(f'c_int_array: {type(value)}')
                assert not problems, problems
                return frame.head(1)
        '''),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)


# ---- round trips of every type ----------------------------------------------------------

POLARS_MIDDLE = '''
import json

import polars as pl


@transformer
def middle(frame, *args, **kwargs):
    assert len(frame) == int(kwargs['expected_rows']), len(frame)
    # Polars columns have one type: JSON of mixed shapes goes as text, as do numbers past
    # the 76 digits of its decimals.
    frame = frame.assign(
        c_json=frame['c_json'].map(lambda v: None if v is None else json.dumps(v)),
        c_jsonb=frame['c_jsonb'].map(lambda v: None if v is None else json.dumps(v)),
        c_numeric_free=frame['c_numeric_free'].map(lambda v: None if v is None else str(v)),
    )
    return pl.from_pandas(frame)
'''

PANDAS_MIDDLE = '''
@transformer
def middle(frame, *args, **kwargs):
    assert len(frame) == int(kwargs['expected_rows']), len(frame)
    return frame
'''


def raw_insert(schema: str) -> str:
    """INSERT ... SELECT with each column cast to the source table's type."""
    from integration_tests.data import postgres_dataset

    values = []
    for name, sql_type in postgres_dataset.COLUMNS:
        sql_type = sql_type.format(schema=schema)
        if name == 'c_int_matrix':
            # Nested lists reach PostgreSQL as jsonb.
            values.append(f"translate({name}::text, '[]', '{{}}')::{sql_type}")
        else:
            values.append(f'{name}::text::{sql_type}' if name in (
                'c_uuid', 'c_enum', 'c_json', 'c_jsonb', 'c_uuid_array', 'c_numeric_free',
            ) else f'{name}::{sql_type}')
    return (
        f'INSERT INTO {schema}.dst\nSELECT {", ".join(values)}\n'
        'FROM {{ df_1 }}'
    )


def round_trip_problems(
    pg, schema: str, exact_numbers: bool, skip: tuple = (), nan_is_null: bool = False,
) -> Dict[str, list]:
    """
    Each source column against the stored one. In a table of the source's types values
    compare as they are; in a table Mage made, by the types it chose (round trips of
    test_sql_blocks.ROUND_TRIP). Without exact_numbers numeric goes through float.
    """
    from integration_tests.data import postgres_dataset
    from integration_tests.postgres.test_sql_blocks import ROUND_TRIP

    source_types = dict(postgres_dataset._format_types(pg, schema, 'src'))
    target_types = dict(postgres_dataset._format_types(pg, schema, 'dst'))
    problems = {}
    for name in postgres_dataset.COLUMN_NAMES:
        if name in skip:
            continue
        if name not in target_types:
            problems[name] = ['<missing column>']
            continue
        same_type = target_types[name] == source_types[name]
        if name in ('c_json', 'c_jsonb'):
            comparison = f'd.{name}::jsonb = s.{name}::jsonb'
        elif name in ('c_numeric', 'c_numeric_free') and not exact_numbers:
            comparison = f'd.{name}::double precision = s.{name}::double precision'
        elif name == 'c_real':
            comparison = f'd.{name}::text::double precision = s.{name}::text::double precision'
        elif same_type:
            comparison = f'd.{name} = s.{name}'
        elif name in ('c_numeric', 'c_numeric_free'):
            comparison = f'd.{name}::numeric = s.{name}'
        else:
            comparison = ROUND_TRIP.get(name, f'd.{name} = s.{name}')
        nan_as_null = (
            f"AND NOT (s.{name}::text = 'NaN' AND d.{name} IS NULL)"
            if nan_is_null and name in ('c_real', 'c_double', 'c_numeric', 'c_numeric_free')
            else ''
        )
        rows = query(pg, f'''
            SELECT s.id, s.{name}::text, d.{name}::text
            FROM {schema}.src s JOIN {schema}.dst d USING (id)
            WHERE ((s.{name} IS NULL) <> (d.{name} IS NULL)
               OR NOT coalesce({comparison}, s.{name} IS NULL)) {nan_as_null}
            ORDER BY s.id LIMIT 3
        ''')
        if rows:
            problems[name] = rows
    return problems


@pytest.mark.parametrize('exporter', ['create', 'append_typed', 'raw_insert'])
@pytest.mark.parametrize('middle', ['pandas', 'polars'])
@pytest.mark.parametrize('loader', ['sql', 'raw_sql', 'sql_exact', 'raw_sql_exact'])
def test_every_type_round_trips_through_python(
    mage_project, pg, profile_schema, source_table, source_rows, loader, middle, exporter,
):
    from integration_tests.data import postgres_dataset

    if exporter != 'create':
        postgres_dataset.create_like_table(pg, profile_schema, 'src', 'dst')
    exact = loader.endswith('_exact')
    loader_config = dict(PG, use_raw_sql=loader.startswith('raw_sql'))
    if exact:
        loader_config['exact_numbers'] = True
    if exporter == 'raw_insert':
        export = dict(
            uuid='export', type='data_exporter', language='sql', upstream=['middle'],
            config=dict(PG, use_raw_sql=True),
            # Raw SQL is the author's: uuid and enum values arrive as text and need casts.
            code=raw_insert(profile_schema),
        )
    else:
        export = dict(
            uuid='export', type='data_exporter', language='sql', upstream=['middle'],
            config=dict(
                PG, data_provider_table='dst',
                export_write_policy='replace' if exporter == 'create' else 'append',
            ),
            code='SELECT * FROM {{ df_1 }}',
        )
    pipeline = build(mage_project, [
        dict(
            uuid='load', type='data_loader', language='sql', config=loader_config,
            code=f'SELECT * FROM {profile_schema}.{source_table} ORDER BY id',
        ),
        dict(
            uuid='middle', type='transformer', upstream=['load'],
            code=POLARS_MIDDLE if middle == 'polars' else PANDAS_MIDDLE,
        ),
        export,
    ])
    run = execute_pipeline(pipeline, expected_rows=len(source_rows))
    if not exact and exporter != 'create':
        # Read as float, the largest numeric(38, 10) value rounds up to 10^28, which the
        # column cannot hold: exact_numbers is how numeric values round trip.
        assert run.status.value == 'failed'
        assert 'numeric field overflow' in error_of(run, 'export')
        return
    assert run.status.value == 'completed', error_of(run)
    assert query(pg, f'SELECT count(*) FROM {profile_schema}.dst')[0][0] == len(source_rows)
    # In the Polars block: a Polars time has no time zone, so timetz loses its offset;
    # pandas holds a NULL float as NaN, so from_pandas cannot keep NULL and NaN apart.
    polars = middle == 'polars'
    assert round_trip_problems(
        pg, profile_schema, exact, skip=('c_timetz',) if polars else (), nan_is_null=polars,
    ) == {}


def test_append_writes_columns_by_name_not_position(mage_project, pg, profile_schema):
    execute(pg, f'CREATE TABLE {profile_schema}.ordered (id bigint, label text, n bigint)')
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code='''
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'n': [10, 20], 'id': [1, 2], 'label': ['a', 'b']})
        '''),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(PG, data_provider_table='ordered', export_write_policy='append'),
            code='SELECT n, label, id FROM {{ df_1 }}',
        ),
    ])
    run_pipeline(pipeline)
    assert query(pg, f'SELECT id, label, n FROM {profile_schema}.ordered ORDER BY id') == [
        (1, 'a', 10), (2, 'b', 20),
    ]

    extra = build(mage_project, [
        dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(PG, data_provider_table='ordered', export_write_policy='append'),
            code='SELECT id, name AS surprise FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(extra)
    assert run.status.value == 'failed'
    assert 'surprise' in error_of(run, 'export')


# ---- failures ------------------------------------------------------------------------------


@pytest.mark.parametrize('raw', [False, True], ids=['sql', 'raw_sql'])
def test_a_failing_replace_keeps_the_previous_table(mage_project, pg, profile_schema, raw):
    table = f'kept_{"raw" if raw else "sql"}'
    if raw:
        code = f'''
            DROP TABLE IF EXISTS {profile_schema}.{table};
            CREATE TABLE {profile_schema}.{table} AS
            SELECT id, 1 / (id - {{{{ variables("zero_at") }}}}) AS x FROM {{{{ df_1 }}}};
        '''
        config = dict(PG, use_raw_sql=True)
    else:
        code = 'SELECT id, 1 / (id - {{ variables("zero_at") }}) AS x FROM {{ df_1 }}'
        config = dict(PG, data_provider_table=table, export_write_policy='replace')
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
        dict(uuid='export', type='data_exporter', language='sql', upstream=['load'],
             config=config, code=code),
    ])
    run_pipeline(pipeline, rows=3, zero_at=100)
    failed = execute_pipeline(pipeline, rows=5, zero_at=4)
    assert failed.status.value == 'failed'
    assert 'division by zero' in error_of(failed, 'export')
    # The statements ran in one transaction, rolled back: the earlier rows remain.
    assert query(pg, f'SELECT count(*) FROM {profile_schema}.{table}')[0][0] == 3


def test_a_missing_column_names_the_column(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
        dict(
            uuid='use', type='transformer', language='sql', upstream=['load'],
            config=dict(PG, export_write_policy='replace'),
            code='SELECT id, nmae FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'failed'
    assert 'nmae' in error_of(run, 'use')


def test_large_text_and_long_arrays(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code='''
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({
                    'id': [1],
                    'big': ['x' * 5_000_000],
                    'many': [list(range(100_000))],
                })
        '''),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(PG, data_provider_table='large', export_write_policy='replace'),
            code='SELECT id, length(big) AS n, cardinality(many) AS m FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    assert query(pg, f'SELECT n, m FROM {profile_schema}.large') == [(5_000_000, 100_000)]


def test_the_same_pipeline_run_twice_in_a_row_is_idempotent(mage_project, pg, profile_schema):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
        dict(
            uuid='middle', type='transformer', language='sql', upstream=['load'],
            config=dict(PG, export_write_policy='replace'),
            code='SELECT id * 2 AS id, name FROM {{ df_1 }}',
        ),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['middle'],
            config=dict(PG, data_provider_table='twice', export_write_policy='replace'),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    for rows in (3, 3, 2):
        run_pipeline(pipeline, rows=rows)
    assert query(pg, f'SELECT id FROM {profile_schema}.twice ORDER BY id') == [(2,), (4,)]
