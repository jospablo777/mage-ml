"""
Trino SQL blocks in an exported service: the pipeline run by mage-service must leave the
tables that the pipeline run by Mage leaves, column by column, in each catalog.

Also, in Mage and in the service:
- an empty upstream frame is written as a table with no rows. Mage skipped it, and the block
  read the rows an earlier run left;
- the table of a SQL upstream on the same catalog and schema is read as it is. Mage copied
  it through pandas into a new table;
- with unique_upstream_table_name, only the tables written for the run get its suffix, and
  they are dropped when the block ends. Every run left its tables behind.
"""
import re

import pytest

from integration_tests.exported_services import differences, run_exported
from integration_tests.mage_runner import execute_pipeline, run_pipeline
from integration_tests.postgres.test_sql_block_matrix import (
    PANDAS_ROWS,
    build,
    error_of,
    output,
)

TRINO = dict(data_provider='trino', data_provider_profile='trino')


def snapshot(tr) -> dict:
    result = {}
    for (name,) in tr('SHOW TABLES'):
        summary = {'rows': tr(f'SELECT count(*) FROM "{name}"')[0][0]}
        for column, column_type, *_ in tr(f'DESCRIBE "{name}"'):
            # An order-independent hash of the column's values.
            digest = tr(f'SELECT checksum("{column}") FROM "{name}"')[0][0]
            summary[column] = (column_type, digest)
        result[name] = summary
    return result


def drop_all(tr) -> None:
    for (name,) in tr('SHOW TABLES'):
        tr(f'DROP TABLE "{name}"')


def test_an_exported_trino_sql_pipeline_leaves_the_tables_mage_leaves(
    mage_project, trino_schema, tr, tmp_path,
):
    run_pipeline('trino_sql', rows=1000)
    expected = snapshot(tr)
    assert len(expected) == 2, expected
    drop_all(tr)

    run_exported(mage_project, 'trino_sql', tmp_path, dict(rows=1000))

    assert differences(expected, snapshot(tr)) == {}


ROWS_OR_EMPTY = '''
import pandas as pd


@data_loader
def load(*args, **kwargs):
    rows = int(kwargs.get('rows', 3))
    return pd.DataFrame({
        'id': pd.Series(range(1, rows + 1), dtype='Int64'),
        'name': pd.Series([f'n{i}' for i in range(1, rows + 1)], dtype='string'),
    })
'''


@pytest.mark.parametrize('trino_catalog', ['memory'])
def test_an_empty_upstream_frame_is_read_as_no_rows(
    mage_project, trino_schema, tr, tmp_path,
):
    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=ROWS_OR_EMPTY),
        dict(
            uuid='pick', type='transformer', language='sql', upstream=['load'],
            config=dict(TRINO, export_write_policy='replace'),
            code='SELECT id, name FROM {{ df_1 }}',
        ),
    ])
    run_pipeline(pipeline, rows=3)
    run = run_pipeline(pipeline, rows=0)
    assert len(output(pipeline, run, 'pick')) == 0

    run_exported(mage_project, pipeline, tmp_path / 'three', dict(rows=3))
    run_exported(mage_project, pipeline, tmp_path / 'none', dict(rows=0))
    pick = next(name for (name,) in tr('SHOW TABLES') if '_pick_' in name)
    assert tr(f'SELECT count(*) FROM "{pick}"') == [[0]]


@pytest.mark.parametrize('trino_catalog', ['memory'])
def test_unique_upstream_tables_are_named_for_the_run_and_dropped(
    mage_project, trino_schema, tr, tmp_path, monkeypatch,
):
    from mage_ai.io.trino import Trino

    written = []
    export = Trino.export

    def recording_export(self, df, schema_name=None, table_name=None, **kwargs):
        if df is not None:
            written.append(table_name)
        return export(self, df, schema_name, table_name, **kwargs)

    monkeypatch.setattr(Trino, 'export', recording_export)

    pipeline = build(mage_project, [
        dict(uuid='load', type='data_loader', code=PANDAS_ROWS),
        dict(
            uuid='names', type='transformer', language='sql', upstream=['load'],
            config=dict(TRINO, export_write_policy='replace'),
            code='SELECT id, upper(name) AS name FROM {{ df_1 }}',
        ),
        dict(
            uuid='joined', type='transformer', language='sql', upstream=['names', 'load'],
            config=dict(TRINO, export_write_policy='replace', unique_upstream_table_name=True),
            code=(
                'SELECT l.id, n.name FROM {{ df_2 }} l JOIN {{ df_1 }} n ON l.id = n.id '
                'ORDER BY l.id'
            ),
        ),
    ])

    def tables():
        return sorted(name for (name,) in tr('SHOW TABLES'))

    run = execute_pipeline(pipeline, rows=3)
    assert run.status.value == 'completed', error_of(run)
    assert output(pipeline, run, 'joined')['name'].tolist() == ['N1', 'N2', 'N3']
    # The load frame, once for names and once for joined: the names table is read as it is.
    assert len(written) == 2 and all('_load_' in name for name in written), written
    # The load table written for names, and the tables of names and joined: the load table
    # written for joined, with the run's suffix, is gone.
    after_mage = tables()
    assert len(after_mage) == 3, after_mage
    assert not [t for t in after_mage if re.search(r'_\d{10}_[0-9a-f]{8}$', t)], after_mage

    drop_all(tr)
    run_exported(mage_project, pipeline, tmp_path, dict(rows=3))
    assert tables() == after_mage
    joined = next(name for name in after_mage if '_joined_' in name)
    assert tr(f'SELECT name FROM "{joined}" ORDER BY id') == [['N1'], ['N2'], ['N3']]
