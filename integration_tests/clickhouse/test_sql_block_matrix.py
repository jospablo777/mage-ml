"""
ClickHouse SQL blocks across configurations, as the PostgreSQL matrix tests them.
"""
import pytest

from integration_tests.mage_runner import execute_pipeline, run_pipeline
from integration_tests.postgres.test_sql_block_matrix import (
    PANDAS_ROWS,
    build,
    error_of,
    output,
)

CH = dict(data_provider='clickhouse', data_provider_profile='clickhouse')


def test_append_writes_columns_by_name(mage_project, ch):
    ch.command(
        'CREATE TABLE ordered (id Int64, label String, n Int64) ENGINE = MergeTree ORDER BY id',
    )
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code='''
            import pandas as pd


            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'n': [10, 20], 'id': [1, 2], 'label': ['a', 'b']})
        '''),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(CH, data_provider_table='ordered', export_write_policy='append'),
            code='SELECT n, label, id FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    assert ch.query('SELECT id, label, n FROM ordered ORDER BY id').result_rows == [
        (1, 'a', 10), (2, 'b', 20),
    ]


@pytest.mark.parametrize('policy', ['replace', 'append'])
def test_export_write_policies_over_two_runs(mage_project, ch, policy):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
        dict(
            uuid='export', type='data_exporter', language='sql', upstream=['load'],
            config=dict(CH, data_provider_table='dst', export_write_policy=policy),
            code='SELECT * FROM {{ df_1 }}',
        ),
    ])
    run_pipeline(pipeline, rows=3)
    run_pipeline(pipeline, rows=4)
    assert ch.query('SELECT count() FROM dst').result_rows[0][0] == (
        4 if policy == 'replace' else 7
    )


def test_empty_frames_and_column_names(mage_project, ch):
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
            config=dict(CH, export_write_policy='replace'),
            code='SELECT `MixedCase`, `with space` FROM {{ df_1 }}',
        ),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    frame = output(pipeline, run, 'pick')
    assert list(frame.columns) == ['MixedCase', 'with space'] and len(frame) == 0
