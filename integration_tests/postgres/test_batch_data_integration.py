"""
PostgreSQL to PostgreSQL data integration blocks in a batch pipeline, run through Mage's
scheduler: several streams, one of them in parallel, each read in several batches, and an
incremental stream that the second run continues from its bookmark.
"""
import textwrap
import uuid

import pytest

from integration_tests.mage_runner import run_scheduled_pipeline
from integration_tests.postgres.test_integrations import discover
from integration_tests.postgres.test_sql_block_matrix import query

# Each stream is read in batches of at most three rows.
BATCH_FETCH_LIMIT = 3


@pytest.fixture
def batch_integration(monkeypatch):
    from mage_ai.data_preparation.models.project import Project
    from mage_ai.data_preparation.models.project.constants import FeatureUUID

    enabled = Project.is_feature_enabled
    monkeypatch.setattr(Project, 'is_feature_enabled', lambda self, feature: (
        feature == FeatureUUID.DATA_INTEGRATION_IN_BATCH_PIPELINE or enabled(self, feature)
    ))


def create_table(pg, schema, table, rows, updated_at=False):
    with pg.cursor() as cursor:
        cursor.execute(
            f'CREATE TABLE {schema}.{table} (id integer PRIMARY KEY, name text'
            + (', updated_at timestamptz' if updated_at else '') + ')',
        )
        insert_rows(cursor, schema, table, 1, rows, updated_at)
    pg.commit()


def insert_rows(cursor, schema, table, first, last, updated_at=False):
    if updated_at:
        cursor.execute(
            f"INSERT INTO {schema}.{table} SELECT n, 'row ' || n, "
            "timestamptz '2024-01-01' + n * interval '1 hour' "
            'FROM generate_series(%s, %s) n',
            (first, last),
        )
    else:
        cursor.execute(
            f"INSERT INTO {schema}.{table} SELECT n, 'row ' || n FROM generate_series(%s, %s) n",
            (first, last),
        )


def build(project, postgres_settings, schema, tmp_path, streams):
    """streams: tap_stream_id to the catalog settings that differ from a FULL_TABLE
    stream."""
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'batch_di_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, repo_path=project)
    _, catalog = discover(postgres_settings, schema, tmp_path)
    catalog['streams'] = [s for s in catalog['streams'] if s['tap_stream_id'] in streams]
    for stream in catalog['streams']:
        stream['replication_method'] = 'FULL_TABLE'
        stream['destination_table'] = f"dst_{stream['tap_stream_id']}"
        stream.update(streams[stream['tap_stream_id']])
        for entry in stream['metadata']:
            entry['metadata']['selected'] = True

    connection = textwrap.indent(textwrap.dedent(f'''\
        database: {postgres_settings['dbname']}
        host: {postgres_settings['host']}
        port: {postgres_settings['port']}
        username: {postgres_settings['user']}
        password: {postgres_settings['password']}
        schema: {schema}
    '''), '  ')
    source = Block.create(
        f'{name}_source', 'data_loader', project, language='yaml', pipeline=pipeline,
    )
    with open(source.file_path, 'w') as file:
        file.write(
            f'source: postgresql\nconfig:\n{connection}'
            f'  batch_fetch_limit: {BATCH_FETCH_LIMIT}\n',
        )
    destination = Block.create(
        f'{name}_destination', 'data_exporter', project, language='yaml', pipeline=pipeline,
        upstream_block_uuids=[source.uuid],
    )
    with open(destination.file_path, 'w') as file:
        file.write(f'destination: postgresql\nconfig:\n{connection}')
    pipeline = Pipeline.get(pipeline.uuid, repo_path=project)
    pipeline.get_block(source.uuid).update_catalog_file(catalog)
    return pipeline.uuid


def test_streams_in_sequence_and_in_parallel(
    mage_project, pg, postgres_settings, schema, tmp_path, batch_integration,
):
    create_table(pg, schema, 'orders', 7)
    create_table(pg, schema, 'customers', 5)
    create_table(pg, schema, 'products', 4)
    pipeline = build(mage_project, postgres_settings, schema, tmp_path, dict(
        orders={},
        customers=dict(run_in_parallel=True),
        products={},
    ))

    run = run_scheduled_pipeline(pipeline)

    batches = sorted(
        b.block_uuid.split(':', 1)[1] for b in run.block_runs
        if (b.metrics or {}).get('child') and not (b.metrics or {}).get('controller')
        and '_destination' in b.block_uuid
    )
    assert batches == sorted(
        [f'postgresql:customers:{i}' for i in range(2)]
        + [f'postgresql:orders:{i}' for i in range(3)]
        + [f'postgresql:products:{i}' for i in range(2)]
    ), batches
    for table, rows in dict(orders=7, customers=5, products=4).items():
        assert query(
            pg, f'SELECT count(*), count(DISTINCT id), min(id), max(id) FROM {schema}.dst_{table}',
        ) == [(rows, rows, 1, rows)], table


def test_an_incremental_stream_continues_from_its_bookmark(
    mage_project, pg, postgres_settings, schema, tmp_path, batch_integration,
):
    create_table(pg, schema, 'events', 5, updated_at=True)
    pipeline = build(mage_project, postgres_settings, schema, tmp_path, dict(
        events=dict(replication_method='INCREMENTAL', bookmark_properties=['updated_at']),
    ))
    run_scheduled_pipeline(pipeline)
    assert query(pg, f'SELECT count(*) FROM {schema}.dst_events') == [(5,)]

    with pg.cursor() as cursor:
        insert_rows(cursor, schema, 'events', 6, 9, updated_at=True)
    pg.commit()
    run_scheduled_pipeline(pipeline)

    assert query(
        pg, f'SELECT count(*), count(DISTINCT id), max(id) FROM {schema}.dst_events',
    ) == [(9, 9, 9)]

    # Nothing new: the run reads nothing and the next one still starts at the bookmark.
    run_scheduled_pipeline(pipeline)
    with pg.cursor() as cursor:
        insert_rows(cursor, schema, 'events', 10, 10, updated_at=True)
    pg.commit()
    run_scheduled_pipeline(pipeline)

    assert query(
        pg, f'SELECT count(*), count(DISTINCT id), max(id) FROM {schema}.dst_events',
    ) == [(10, 10, 10)]


def test_a_python_blocks_output_exported_by_a_destination_block(
    mage_project, pg, postgres_settings, schema, batch_integration,
):
    """A DataFrame from an ordinary block, written by a data integration destination."""
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'batch_di_py_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, repo_path=mage_project)
    loader = Block.create(
        f'{name}_rows', 'data_loader', mage_project, language='python', pipeline=pipeline,
    )
    with open(loader.file_path, 'w') as file:
        file.write(textwrap.dedent('''\
            import pandas as pd


            @data_loader
            def rows(*args, **kwargs):
                return pd.DataFrame({
                    'id': [1, 2, 3],
                    'name': ['a', "it's", None],
                    'amount': [1.5, -2.0, 1e10],
                })
        '''))
    destination = Block.create(
        f'{name}_destination', 'data_exporter', mage_project, language='yaml',
        pipeline=pipeline, upstream_block_uuids=[loader.uuid],
    )
    with open(destination.file_path, 'w') as file:
        file.write(textwrap.dedent(f'''\
            destination: postgresql
            config:
              database: {postgres_settings['dbname']}
              host: {postgres_settings['host']}
              port: {postgres_settings['port']}
              username: {postgres_settings['user']}
              password: {postgres_settings['password']}
              schema: {schema}
              table: rows
        '''))

    run_scheduled_pipeline(pipeline.uuid)

    tables = [t for (t,) in query(
        pg, 'SELECT table_name FROM information_schema.tables WHERE table_schema = %s',
        (schema,),
    )]
    assert len(tables) == 1, tables
    assert query(pg, f'SELECT id, name, amount FROM {schema}.{tables[0]} ORDER BY id') == [
        (1, 'a', 1.5), (2, "it's", -2.0), (3, None, 1e10),
    ]
