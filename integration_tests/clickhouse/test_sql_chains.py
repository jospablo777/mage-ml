"""
Compiled chains of ClickHouse SQL blocks: in a production run, a block that replaces its
table and is read only by the next SQL block of its profile becomes a view, marked as a
chain view, and the block that needs its rows writes its table from one query ClickHouse
plans across the views.
"""
from pathlib import Path

from integration_tests.clickhouse.test_sql_block_matrix import CH
from integration_tests.postgres.test_sql_block_matrix import block_uuid, build
from integration_tests.postgres.test_sql_chains import compiled_run, completed, table

REPLACE = dict(CH, export_write_policy='replace')


def kinds(ch, database):
    from mage_ai.io.clickhouse import CHAIN_VIEW_COMMENT

    rows = ch.query(
        'SELECT name, engine, comment FROM system.tables WHERE database = {d:String}',
        parameters=dict(d=database),
    ).result_rows
    return {name: ('chain view' if comment == CHAIN_VIEW_COMMENT else 'view')
            if engine == 'View' else 'table' for name, engine, comment in rows}


def chain(project):
    return build(project, [
        dict(uuid='load', type='data_loader', language='sql', config=REPLACE,
             code='SELECT number AS n FROM numbers(10)'),
        dict(uuid='double', type='transformer', language='sql', config=REPLACE,
             upstream=['load'], code='SELECT n * 2 AS d FROM {{ df_1 }} WHERE n > 2'),
        dict(uuid='total', type='data_exporter', language='sql', config=REPLACE,
             upstream=['double'], code='SELECT sum(d) AS total FROM {{ df_1 }}'),
    ])


def total(ch, pipeline):
    return ch.query(f'SELECT total FROM {table(pipeline, "total")}').result_rows[0][0]


def test_a_chain_becomes_marked_views_and_the_exporter_writes_its_table(
    mage_project, ch, clickhouse_database,
):
    pipeline = chain(mage_project)
    completed(compiled_run(pipeline))

    assert total(ch, pipeline) == 84
    found = kinds(ch, clickhouse_database)
    assert found[table(pipeline, 'load')] == 'chain view'
    assert found[table(pipeline, 'double')] == 'chain view'
    assert found[table(pipeline, 'total')] == 'table'

    # A view of the user's over a chain view keeps working across runs.
    ch.command(f'CREATE VIEW mine AS SELECT d FROM {table(pipeline, "double")}')
    completed(compiled_run(pipeline))
    assert total(ch, pipeline) == 84
    assert ch.query('SELECT count() FROM mine').result_rows[0][0] == 7


def test_a_view_whose_columns_change_and_an_editor_run(mage_project, ch, clickhouse_database):
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = chain(mage_project)
    completed(compiled_run(pipeline))

    double = Pipeline.get(pipeline).get_block(block_uuid(pipeline, 'double'))
    Path(double.file_path).write_text(
        'SELECT n AS original, n * 3 AS d FROM {{ df_1 }} WHERE n > 2\n',
    )
    completed(compiled_run(pipeline))
    assert total(ch, pipeline) == 3 * 42

    # Run in the editor, the block writes its table, and compiled again it is a view.
    load = table(pipeline, 'load')
    block = Pipeline.get(pipeline).get_block(block_uuid(pipeline, 'load'))
    block.execute_sync(from_notebook=True, run_all_blocks=True)
    assert kinds(ch, clickhouse_database)[load] == 'table'
    assert ch.query(f'SELECT count() FROM {load}').result_rows[0][0] == 10
    completed(compiled_run(pipeline))
    assert kinds(ch, clickhouse_database)[load] == 'chain view'
    assert total(ch, pipeline) == 3 * 42
