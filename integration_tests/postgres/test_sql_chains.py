"""
Compiled chains of PostgreSQL SQL blocks: in a production run, a block that replaces its
table and is read only by the next SQL block of its profile becomes a view, marked as a
chain view, and the block that needs its rows writes its table from one query PostgreSQL
plans across the views. PostgreSQL keeps a table that a view reads from being dropped, so
a block that replaces such a table drops the chain views that read it, and never a view
of the user's.
"""
from pathlib import Path

from integration_tests.postgres.test_sql_block_matrix import (
    PG,
    block_uuid,
    build,
    error_of,
    execute,
    query,
)

REPLACE = dict(PG, export_write_policy='replace')


def compiled_run(pipeline_uuid: str):
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.orchestration import fusion_verify
    from mage_ai.shared.dates import utc_now

    pipeline = Pipeline.get(pipeline_uuid)
    return fusion_verify.run_in_process(
        fusion_verify._create_run(pipeline, 'chains', {}, utc_now()), log=lambda text: None,
    )


def completed(run):
    statuses = {b.block_uuid: getattr(b.status, 'value', b.status) for b in run.block_runs}
    assert set(statuses.values()) == {'completed'}, (statuses, error_of(run))


def relations(pg, schema):
    """Each relation of the schema: its kind and whether it is marked as a chain view."""
    from mage_ai.io.postgres import CHAIN_VIEW_COMMENT

    rows = query(pg, """
        SELECT c.relname, c.relkind, obj_description(c.oid, 'pg_class') = %s
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind IN ('r', 'v')
    """, (CHAIN_VIEW_COMMENT, schema))
    return {name: ('chain view' if marked else 'view') if kind == 'v' else 'table'
            for name, kind, marked in rows}


def chain(project, double_code='SELECT n * 2 AS d FROM {{ df_1 }} WHERE n > 2'):
    return build(project, [
        dict(uuid='load', type='data_loader', language='sql', config=REPLACE,
             code='SELECT generate_series(0, 9) AS n'),
        dict(uuid='double', type='transformer', language='sql', config=REPLACE,
             upstream=['load'], code=double_code),
        dict(uuid='total', type='data_exporter', language='sql', config=REPLACE,
             upstream=['double'], code='SELECT sum(d) AS total FROM {{ df_1 }}'),
    ])


def table(pipeline_uuid: str, name: str) -> str:
    """The table, or view, a SQL block of the pipeline writes."""
    from mage_ai.data_preparation.models.pipeline import Pipeline

    return Pipeline.get(pipeline_uuid).get_block(block_uuid(pipeline_uuid, name)).table_name


def total(pg, schema, pipeline):
    return query(pg, f'SELECT total FROM {schema}.{table(pipeline, "total")}')[0][0]


def test_a_chain_becomes_marked_views_and_the_exporter_writes_its_table(
    mage_project, pg, profile_schema,
):
    pipeline = chain(mage_project)
    completed(compiled_run(pipeline))

    assert total(pg, profile_schema, pipeline) == 84
    kinds = relations(pg, profile_schema)
    assert kinds[table(pipeline, 'load')] == 'chain view'
    assert kinds[table(pipeline, 'double')] == 'chain view'
    assert kinds[table(pipeline, 'total')] == 'table'

    # Again, compiled: each view is created again over the one it reads.
    completed(compiled_run(pipeline))
    assert total(pg, profile_schema, pipeline) == 84


def test_a_view_whose_columns_change_is_created_again(mage_project, pg, profile_schema):
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = chain(mage_project)
    completed(compiled_run(pipeline))

    # CREATE OR REPLACE VIEW cannot rename or drop a column; the chain drops and creates.
    double = Pipeline.get(pipeline).get_block(block_uuid(pipeline, 'double'))
    Path(double.file_path).write_text(
        'SELECT n AS original, n * 3 AS d FROM {{ df_1 }} WHERE n > 2\n',
    )
    completed(compiled_run(pipeline))
    assert total(pg, profile_schema, pipeline) == 3 * 42


def test_replacing_a_table_that_chain_views_read_drops_and_recreates_them(
    mage_project, pg, profile_schema,
):
    # base has two readers, so it keeps its table; the chain after it reads it as a view.
    pipeline = build(mage_project, [
        dict(uuid='base', type='data_loader', language='sql', config=REPLACE,
             code='SELECT generate_series(1, 4) AS n'),
        dict(uuid='side', type='data_exporter', language='sql', config=REPLACE,
             upstream=['base'], code='SELECT count(*) AS c FROM {{ df_1 }}'),
        dict(uuid='double', type='transformer', language='sql', config=REPLACE,
             upstream=['base'], code='SELECT n * 2 AS d FROM {{ df_1 }}'),
        dict(uuid='total', type='data_exporter', language='sql', config=REPLACE,
             upstream=['double'], code='SELECT sum(d) AS total FROM {{ df_1 }}'),
    ])
    completed(compiled_run(pipeline))
    kinds = relations(pg, profile_schema)
    assert kinds[table(pipeline, 'base')] == 'table'
    assert kinds[table(pipeline, 'double')] == 'chain view'

    # base drops its table to replace it, which the chain view of double reads.
    completed(compiled_run(pipeline))
    assert total(pg, profile_schema, pipeline) == 20
    assert relations(pg, profile_schema)[table(pipeline, 'double')] == 'chain view'


def test_a_view_of_the_users_is_never_dropped(mage_project, pg, profile_schema):
    pipeline = chain(mage_project)
    completed(compiled_run(pipeline))
    double = table(pipeline, 'double')
    execute(pg, f'CREATE VIEW {profile_schema}.mine AS SELECT d FROM {profile_schema}.{double}')

    run = compiled_run(pipeline)

    # Replacing load would drop the user's view through double: the run fails as it did
    # before chains, and says what depends on it.
    assert 'mine' in error_of(run)
    assert relations(pg, profile_schema)['mine'] == 'view'


def test_an_editor_run_writes_the_table_and_the_next_compiled_run_a_view_again(
    mage_project, pg, profile_schema,
):
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = chain(mage_project)
    completed(compiled_run(pipeline))
    load = table(pipeline, 'load')

    block = Pipeline.get(pipeline).get_block(block_uuid(pipeline, 'load'))
    block.execute_sync(from_notebook=True, run_all_blocks=True)
    assert relations(pg, profile_schema)[load] == 'table'
    assert query(pg, f'SELECT count(*) FROM {profile_schema}.{load}')[0][0] == 10

    completed(compiled_run(pipeline))
    assert relations(pg, profile_schema)[load] == 'chain view'
    assert total(pg, profile_schema, pipeline) == 84
