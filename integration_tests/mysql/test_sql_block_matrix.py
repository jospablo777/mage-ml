"""
MySQL SQL blocks across their configurations, as the PostgreSQL matrix tests them.
"""
import pytest

from integration_tests.mage_runner import execute_pipeline, run_pipeline
from integration_tests.postgres.test_sql_block_matrix import (
    PANDAS_ROWS,
    build,
    error_of,
    output,
)

MY = dict(data_provider='mysql', data_provider_profile='mysql')


@pytest.fixture
def database(mysql_database, monkeypatch):
    monkeypatch.setenv('MAGE_TEST_MYSQL_DATABASE', mysql_database)
    return mysql_database


def query(my, sql: str):
    with my.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def test_append_writes_columns_by_name(mage_project, my, database):
    query(my, 'CREATE TABLE ordered (id BIGINT, label TEXT, n BIGINT)')
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code='''
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'n': [10, 20], 'id': [1, 2], 'label': ['a', 'b']})
        '''),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(MY, data_provider_table='ordered', export_write_policy='append'),
            code='SELECT n, label, id FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    assert query(my, 'SELECT id, label, n FROM ordered ORDER BY id') == [
        (1, 'a', 10), (2, 'b', 20),
    ]


def test_empty_frames_and_column_names(mage_project, my, database):
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
            config=dict(MY, export_write_policy='replace'),
            code='SELECT `MixedCase`, `with space` FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    frame = output(pipeline, run, 'pick')
    assert list(frame.columns) == ['MixedCase', 'with space'] and len(frame) == 0


def test_raw_sql_passes_every_row_of_the_table_it_makes(mage_project, my, database):
    pipeline = build(mage_project, [
        dict(
            uuid='create', type='transformer', language='sql',
            config=dict(MY, use_raw_sql=True),
            code='''
                SET SESSION cte_max_recursion_depth = 10000;
                DROP TABLE IF EXISTS made;
                CREATE TABLE made AS
                WITH RECURSIVE n AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM n WHERE id < 3000)
                SELECT id, 'semi;colon' AS note FROM n;
            ''',
        ),
        dict(uuid='count', type='transformer', upstream=['create'], code='''
            @transformer
            def count(frame, *args, **kwargs):
                assert len(frame) == 3000, len(frame)
                assert frame['note'].iloc[0] == 'semi;colon'
                return frame.head(1)
        '''),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)


@pytest.mark.parametrize('policy', ['replace', 'append', 'fail'])
def test_export_write_policies_over_two_runs(mage_project, my, database, policy):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(MY, data_provider_table='dst', export_write_policy=policy),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    run_pipeline(pipeline, rows=3)
    second = execute_pipeline(pipeline, rows=4)
    assert query(my, 'SELECT count(*) FROM dst')[0][0] == {
        'replace': 4, 'append': 7, 'fail': 3,
    }[policy]
    assert second.status.value == ('failed' if policy == 'fail' else 'completed')
