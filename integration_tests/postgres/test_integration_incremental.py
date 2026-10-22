"""
An INCREMENTAL stream of a PostgreSQL to PostgreSQL data integration pipeline, read in
several batches, run by Mage twice: a run reads each row once whatever its batch
boundaries (each batch read the last row of the one before again), and the next run reads
the rows added since, from the row at the bookmark on.
"""
import json
import textwrap
import uuid

from integration_tests.mage_runner import run_integration_pipeline
from integration_tests.postgres.test_integrations import discover, select
from integration_tests.postgres.test_sql_block_matrix import query

# Batches of at most three rows.
BATCH_FETCH_LIMIT = 3


def insert(pg, schema, first, last):
    with pg.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {schema}.src SELECT n, 'row ' || n, "
            "timestamptz '2024-01-01' + n * interval '1 hour' FROM generate_series(%s, %s) n",
            (first, last),
        )
    pg.commit()


def build(project, postgres_settings, schema, tmp_path):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'incremental_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.INTEGRATION, repo_path=project)
    _, catalog = discover(postgres_settings, schema, tmp_path)
    path = select(catalog, tmp_path, replication_method='INCREMENTAL',
                  bookmark_properties=['updated_at'])
    with open(path) as file:
        catalog = json.load(file)
    for stream in catalog['streams']:
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
        file.write(f'source: postgresql\nconfig:\n{connection}'
                   f'  batch_fetch_limit: {BATCH_FETCH_LIMIT}\n')
    destination = Block.create(f'{name}_destination', 'data_exporter', project, language='yaml')
    with open(destination.file_path, 'w') as file:
        file.write(f'destination: postgresql\nconfig:\n{connection}')
    pipeline.add_block(source)
    pipeline.add_block(destination, upstream_block_uuids=[source.uuid])
    return pipeline.uuid


def test_a_run_reads_each_row_once_and_the_next_continues(
    mage_project, pg, postgres_settings, schema, tmp_path,
):
    with pg.cursor() as cursor:
        # No key: the destination keeps a row read twice twice, so the counts show it.
        cursor.execute(f'CREATE TABLE {schema}.src (id integer, name text, updated_at timestamptz)')
    pg.commit()
    insert(pg, schema, 1, 7)
    pipeline = build(mage_project, postgres_settings, schema, tmp_path)

    run_integration_pipeline(pipeline)
    assert query(pg, f'SELECT count(*), count(DISTINCT id), max(id) FROM {schema}.dst') == [
        (7, 7, 7)]

    insert(pg, schema, 8, 12)
    run_integration_pipeline(pipeline)
    # The row at the bookmark is read again: a bookmark that is not unique compares with
    # >=, as Singer sources do, so rows sharing its value are not left out. Unique
    # constraints on the stream make the second copy an update.
    assert query(pg, f'SELECT count(*), count(DISTINCT id), max(id) FROM {schema}.dst') == [
        (13, 12, 12)]
    assert query(pg, f'SELECT id FROM {schema}.dst GROUP BY id HAVING count(*) > 1') == [(7,)]
