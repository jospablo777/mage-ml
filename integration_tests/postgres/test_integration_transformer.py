"""
A Python transformer between a PostgreSQL source and destination in a data integration
pipeline, run by Mage: each record goes through it, and a stream with no rows still
writes nothing and completes.
"""
import json
import textwrap
import uuid
from pathlib import Path

from integration_tests.mage_runner import run_integration_pipeline
from integration_tests.postgres.test_integrations import discover, select
from integration_tests.postgres.test_sql_block_matrix import query

TRANSFORM = '''
@transformer
def shout(df, *args, **kwargs):
    df['name'] = df['name'].str.upper()
    return df[df['id'] != 2]
'''


def build(project, postgres_settings, schema, tmp_path):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'transformed_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.INTEGRATION, repo_path=project)
    _, catalog = discover(postgres_settings, schema, tmp_path)
    with open(select(catalog, tmp_path)) as file:
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
    Path(source.file_path).write_text(f'source: postgresql\nconfig:\n{connection}')
    transform = Block.create(f'{name}_shout', 'transformer', project, language='python')
    Path(transform.file_path).write_text(textwrap.dedent(TRANSFORM))
    destination = Block.create(f'{name}_destination', 'data_exporter', project, language='yaml')
    Path(destination.file_path).write_text(f'destination: postgresql\nconfig:\n{connection}')
    pipeline.add_block(source)
    pipeline.add_block(transform, upstream_block_uuids=[source.uuid])
    pipeline.add_block(destination, upstream_block_uuids=[transform.uuid])
    return pipeline.uuid


def test_records_go_through_the_transformer(
    mage_project, pg, postgres_settings, schema, tmp_path,
):
    with pg.cursor() as cursor:
        cursor.execute(f'CREATE TABLE {schema}.src (id integer PRIMARY KEY, name text)')
        cursor.execute(f"INSERT INTO {schema}.src VALUES (1, 'ana'), (2, 'bo'), (3, 'cy')")
    pg.commit()

    run_integration_pipeline(build(mage_project, postgres_settings, schema, tmp_path))

    assert query(pg, f'SELECT id, name FROM {schema}.dst ORDER BY id') == [
        (1, 'ANA'), (3, 'CY')]


def test_a_stream_without_rows_completes(mage_project, pg, postgres_settings, schema, tmp_path):
    with pg.cursor() as cursor:
        cursor.execute(f'CREATE TABLE {schema}.src (id integer PRIMARY KEY, name text)')
    pg.commit()

    run_integration_pipeline(build(mage_project, postgres_settings, schema, tmp_path))
