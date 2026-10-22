"""
A PostgreSQL to PostgreSQL data integration pipeline whose destination truncates its
table before a FULL_TABLE sync (upstream #4708), run by Mage twice: rows deleted at the
source between the runs leave the destination too, and the stream's later batches do not
empty what its first one wrote.
"""
import json
import textwrap
import uuid

from integration_tests.mage_runner import (
    run_integration_pipeline,
    run_scheduled_pipeline,
)
from integration_tests.postgres.test_integrations import discover, select
from integration_tests.postgres.test_sql_block_matrix import query

ROWS = 7
# Three batches of at most three rows.
BATCH_FETCH_LIMIT = 3


def build(project, postgres_settings, schema, tmp_path):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'truncate_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.INTEGRATION, repo_path=project)
    _, catalog = discover(postgres_settings, schema, tmp_path)
    with open(select(catalog, tmp_path)) as file:
        catalog = json.load(file)
    for stream in catalog['streams']:
        for entry in stream['metadata']:
            entry['metadata']['selected'] = True
        stream['destination_table'] = 'dst'
    with open(pipeline.catalog_config_path, 'w') as file:
        json.dump(dict(catalog=catalog), file)

    connection = textwrap.indent(textwrap.dedent(f'''\
        database: {postgres_settings['dbname']}
        host: {postgres_settings['host']}
        port: {postgres_settings['port']}
        username: {postgres_settings['user']}
        password: {postgres_settings['password']}
        schema: {schema}
    '''), '  ')
    source = Block.create(f'{name}_source', 'data_loader', project, language='yaml')
    with open(source.file_path, 'w') as file:
        file.write(
            f'source: postgresql\nconfig:\n{connection}'
            f'  batch_fetch_limit: {BATCH_FETCH_LIMIT}\n',
        )
    destination = Block.create(f'{name}_destination', 'data_exporter', project, language='yaml')
    with open(destination.file_path, 'w') as file:
        file.write(
            f'destination: postgresql\nconfig:\n{connection}'
            '  truncate_before_full_table_sync: true\n',
        )
    pipeline.add_block(source)
    pipeline.add_block(destination, upstream_block_uuids=[source.uuid])
    return pipeline.uuid


def test_a_full_table_sync_replaces_the_table(
    mage_project, pg, postgres_settings, schema, tmp_path,
):
    with pg.cursor() as cursor:
        cursor.execute(f'CREATE TABLE {schema}.src (id integer PRIMARY KEY, name text)')
        cursor.execute(
            f'INSERT INTO {schema}.src SELECT n, %s || n FROM generate_series(1, %s) n',
            ('row ', ROWS),
        )
    pg.commit()
    pipeline = build(mage_project, postgres_settings, schema, tmp_path)

    run = run_integration_pipeline(pipeline)
    exports = [b for b in run.block_runs if '_destination:' in b.block_uuid]
    assert len(exports) == 3, [b.block_uuid for b in exports]
    assert query(pg, f'SELECT count(*), min(id), max(id) FROM {schema}.dst') == [(ROWS, 1, ROWS)]

    with pg.cursor() as cursor:
        cursor.execute(f'DELETE FROM {schema}.src WHERE id IN (1, 5)')
        cursor.execute(f"UPDATE {schema}.src SET name = 'changed' WHERE id = 2")
    pg.commit()
    run_integration_pipeline(pipeline)

    assert query(pg, f'SELECT id, name FROM {schema}.dst ORDER BY id') == [
        (2, 'changed'), (3, 'row 3'), (4, 'row 4'), (6, 'row 6'), (7, 'row 7'),
    ]


def build_batch(project, postgres_settings, schema, tmp_path):
    """The same sync as blocks of a batch pipeline, where a stream's batches run in any
    order."""
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'truncate_batch_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, repo_path=project)
    _, catalog = discover(postgres_settings, schema, tmp_path)
    with open(select(catalog, tmp_path)) as file:
        catalog = json.load(file)
    for stream in catalog['streams']:
        for entry in stream['metadata']:
            entry['metadata']['selected'] = True
        stream['destination_table'] = 'dst'

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
        file.write(
            f'destination: postgresql\nconfig:\n{connection}'
            '  truncate_before_full_table_sync: true\n',
        )
    pipeline = Pipeline.get(pipeline.uuid, repo_path=project)
    pipeline.get_block(source.uuid).update_catalog_file(catalog)
    return pipeline.uuid


def test_a_batch_pipeline_full_table_sync_replaces_the_table(
    mage_project, pg, postgres_settings, schema, tmp_path, monkeypatch,
):
    from mage_ai.data_preparation.models.project import Project
    from mage_ai.data_preparation.models.project.constants import FeatureUUID

    enabled = Project.is_feature_enabled
    monkeypatch.setattr(Project, 'is_feature_enabled', lambda self, feature: (
        feature == FeatureUUID.DATA_INTEGRATION_IN_BATCH_PIPELINE or enabled(self, feature)
    ))
    with pg.cursor() as cursor:
        cursor.execute(f'CREATE TABLE {schema}.src (id integer PRIMARY KEY, name text)')
        cursor.execute(
            f'INSERT INTO {schema}.src SELECT n, %s || n FROM generate_series(1, %s) n',
            ('row ', ROWS),
        )
    pg.commit()
    pipeline = build_batch(mage_project, postgres_settings, schema, tmp_path)

    run = run_scheduled_pipeline(pipeline)
    batches = [b.block_uuid for b in run.block_runs if (b.metrics or {}).get('child')
               and '_destination' in b.block_uuid and not (b.metrics or {}).get('controller')]
    assert len(batches) == 3, [(b.block_uuid, b.metrics) for b in run.block_runs]
    assert query(pg, f'SELECT count(*), min(id), max(id) FROM {schema}.dst') == [(ROWS, 1, ROWS)]

    with pg.cursor() as cursor:
        cursor.execute(f'DELETE FROM {schema}.src WHERE id IN (1, 5)')
        cursor.execute(f"UPDATE {schema}.src SET name = 'changed' WHERE id = 2")
    pg.commit()
    run_scheduled_pipeline(pipeline)

    assert query(pg, f'SELECT id, name FROM {schema}.dst ORDER BY id') == [
        (2, 'changed'), (3, 'row 3'), (4, 'row 4'), (6, 'row 6'), (7, 'row 7'),
    ]
