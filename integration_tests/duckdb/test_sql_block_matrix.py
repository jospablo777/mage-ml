"""
DuckDB SQL blocks across their configurations, as the PostgreSQL matrix tests them:
write policies, raw SQL scripts, row counts, empty frames, column names and appends.
"""
import duckdb
import pytest

from integration_tests.mage_runner import execute_pipeline, run_pipeline
from integration_tests.postgres.test_sql_block_matrix import (
    PANDAS_ROWS,
    build,
    error_of,
    output,
)

DUCK = dict(data_provider='duckdb', data_provider_profile='duckdb', data_provider_schema='main')


@pytest.fixture
def duck(duckdb_path, monkeypatch):
    monkeypatch.setenv('MAGE_TEST_DUCKDB_DATABASE', duckdb_path)
    return duckdb_path


def query(path: str, sql: str):
    connection = duckdb.connect(path, read_only=True)
    try:
        return connection.execute(sql).fetchall()
    finally:
        connection.close()


@pytest.mark.parametrize('policy', ['replace', 'append', 'fail'])
def test_export_write_policies_over_two_runs(mage_project, duck, policy):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(DUCK, data_provider_table='dst', export_write_policy=policy),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    run_pipeline(pipeline, rows=3)
    second = execute_pipeline(pipeline, rows=4)
    count = query(duck, 'SELECT count(*) FROM main.dst')[0][0]
    expected = {'replace': 4, 'append': 7, 'fail': 3}[policy]
    assert count == expected
    assert second.status.value == ('failed' if policy == 'fail' else 'completed')


def test_raw_sql_scripts_and_their_rows_downstream(mage_project, duck):
    pipeline = build(mage_project, [
        dict(
            uuid='create', type='transformer', language='sql',
            config=dict(DUCK, use_raw_sql=True),
            code='''
                -- A comment; with a semicolon.
                CREATE OR REPLACE TABLE main.made AS
                SELECT range AS id, 'semi;colon' AS note, $$dollar; quoted$$ AS dollar
                FROM range(5000);
            ''',
        ),
        dict(uuid='count', type='transformer', upstream=['create'], code='''
            @transformer
            def count(frame, *args, **kwargs):
                assert len(frame) == 5000, len(frame)
                assert frame['note'].iloc[0] == 'semi;colon'
                assert frame['dollar'].iloc[0] == 'dollar; quoted'
                return frame.head(1)
        '''),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)


def test_empty_frames_and_column_names(mage_project, duck):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code='''
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({
                    'MixedCase': pd.Series([], dtype='Int64'),
                    'with space': pd.Series([], dtype='string'),
                })
        '''),
        dict(
            uuid='pick', type='transformer', language='sql', upstream=['load'],
            config=dict(DUCK, export_write_policy='replace'),
            code='SELECT "MixedCase", "with space" FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    frame = output(pipeline, run, 'pick')
    assert list(frame.columns) == ['MixedCase', 'with space'] and len(frame) == 0


def test_append_writes_columns_by_name(mage_project, duck):
    connection = duckdb.connect(duck)
    connection.execute('CREATE TABLE main.ordered (id BIGINT, label VARCHAR, n BIGINT)')
    connection.close()
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code='''
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'n': [10, 20], 'id': [1, 2], 'label': ['a', 'b']})
        '''),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(DUCK, data_provider_table='ordered', export_write_policy='append'),
            code='SELECT n, label, id FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    assert query(duck, 'SELECT id, label, n FROM main.ordered ORDER BY id') == [
        (1, 'a', 10), (2, 'b', 20),
    ]


def test_types_round_trip_through_pandas(mage_project, duck):
    pipeline = build(mage_project, [
        dict(
            uuid='load', type='data_loader', language='sql',
            config=dict(DUCK, exact_numbers=True),
            code='''
                SELECT
                    1 AS id,
                    9223372036854775807::BIGINT AS big,
                    NULL::BIGINT AS missing,
                    12345678901234567.89::DECIMAL(38, 2) AS amount,
                    'ü'::VARCHAR AS text,
                    TIMESTAMPTZ '2024-03-31 01:30:00+00' AS at,
                    DATE '2024-02-29' AS day,
                    [1, 2, NULL] AS ints,
                    {'a': 1, 'b': 'x'} AS record
            ''',
        ),
        dict(uuid='middle', type='transformer', upstream=['load'], code='''
            @transformer
            def middle(frame, *args, **kwargs):
                assert frame['big'].iloc[0] == 9223372036854775807
                return frame
        '''),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['middle'],
            config=dict(DUCK, data_provider_table='typed', export_write_policy='replace'),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    row = query(duck, '''
        SELECT big, missing, amount::VARCHAR, text, day::VARCHAR, ints, record
        FROM main.typed
    ''')[0]
    assert row[:5] == (9223372036854775807, None, '12345678901234567.89', 'ü', '2024-02-29')
    assert list(row[5]) == [1, 2, None]
    assert dict(row[6]) == {'a': 1, 'b': 'x'}


def test_the_sql_templates_run_on_duckdb(mage_project, duck):
    from integration_tests.sql_templates import check_sql_templates

    check_sql_templates(mage_project, DUCK)


def test_the_files_template_reads_parquet_with_duckdb(mage_project, duck):
    from pathlib import Path

    import pandas as pd

    from integration_tests.sql_templates import sql_template

    data = Path(mage_project) / 'data'
    data.mkdir(exist_ok=True)
    pd.DataFrame({'id': [1, 2]}).to_parquet(data / 'files_template_a.parquet')
    pd.DataFrame({'id': [3]}).to_parquet(data / 'files_template_b.parquet')
    code = sql_template('data_loaders/sql/files.sql').replace(
        "'data/*.parquet'", f"'{data}/files_template_*.parquet'",
    )
    pipeline = build(mage_project, [
        dict(uuid='files', type='data_loader', language='sql', code=code,
             config=dict(DUCK, export_write_policy='replace')),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    frame = output(pipeline, run, 'files')
    ids = frame['id'].to_list() if hasattr(frame, 'to_list') else list(frame['id'])
    assert sorted(ids) == [1, 2, 3]
