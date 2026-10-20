import json
from os import path
from typing import Dict, List, Optional, Tuple

# from mage_ai.data_preparation.models.block.content import template_render
from mage_ai.data_preparation.models.block.sql.utils.shared import (
    blocks_in_query,
    interpolate_input,
    should_cache_data_from_upstream,
    table_name_parts,
    upstream_has_no_data,
)
from mage_ai.data_preparation.models.constants import BlockType
from mage_ai.data_preparation.variable_manager import get_variable
from mage_ai.io.config import ConfigFileLoader, ConfigKey
from mage_ai.settings.repo import get_repo_path

# from sqlglot import exp, parse_one


def bigquery_project(profile: str) -> Tuple[Optional[str], Optional[str]]:
    """
    The project and location an io_config profile reaches BigQuery in: the project_id of
    its service account key (the key, or the file it names) and GOOGLE_LOCATION. The
    project is None for application default credentials, which the file does not name.
    """
    config = ConfigFileLoader(path.join(get_repo_path(), 'io_config.yaml'), profile)
    key = config[ConfigKey.GOOGLE_SERVICE_ACC_KEY]
    if not key and config[ConfigKey.GOOGLE_SERVICE_ACC_KEY_FILEPATH]:
        try:
            with open(config[ConfigKey.GOOGLE_SERVICE_ACC_KEY_FILEPATH]) as file:
                key = json.load(file)
        except (OSError, ValueError):
            key = None
    if isinstance(key, str):
        try:
            key = json.loads(key)
        except ValueError:
            key = None
    project = key.get('project_id') if isinstance(key, dict) else None
    return project, config[ConfigKey.GOOGLE_LOCATION]


def reads_upstream_table(block, upstream_block) -> bool:
    """
    Whether the block reads an upstream BigQuery SQL block's table as it is: both use the
    same io_config profile, or profiles of the same project and location. The check
    required GOOGLE_SERVICE_ACC_KEY and GOOGLE_SERVICE_ACC_KEY_FILEPATH, which profiles
    seldom both set, so every upstream table was read into pandas and written over itself
    (I-222).
    """
    profile = (block.configuration or {}).get('data_provider_profile')
    upstream_profile = (upstream_block.configuration or {}).get('data_provider_profile')
    if profile == upstream_profile:
        return True
    project, location = bigquery_project(profile)
    return project is not None and (project, location) == bigquery_project(upstream_profile)


def create_upstream_block_tables(
    loader,
    block,
    configuration: Dict = None,
    execution_partition: str = None,
    cache_upstream_dbt_models: bool = False,
    query: str = None,
    dynamic_block_index: int = None,
    dynamic_upstream_block_uuids: List[str] = None,
    variables: Dict = None,
):
    configuration = configuration if configuration else block.configuration
    database_default = configuration.get('data_provider_database') or loader.default_database()

    input_vars, kwargs_vars, upstream_block_uuids = block.fetch_input_variables(
        None,
        execution_partition=execution_partition,
        global_vars=None,
        dynamic_block_index=dynamic_block_index,
        dynamic_upstream_block_uuids=dynamic_upstream_block_uuids,
    )

    mapping = blocks_in_query(block, query)
    for idx, upstream_block in enumerate(block.upstream_blocks):
        if query and upstream_block.uuid not in mapping:
            continue

        copy = should_cache_data_from_upstream(block, upstream_block, ['data_provider'], [])
        if not copy and BlockType.SENSOR != upstream_block.type:
            copy = not reads_upstream_table(block, upstream_block)
        if copy:
            if BlockType.DBT == upstream_block.type and not cache_upstream_dbt_models:
                continue

            if input_vars and idx < len(input_vars):
                df = input_vars[idx]
            else:
                df = get_variable(
                    upstream_block.pipeline.uuid,
                    upstream_block.uuid,
                    'output_0',
                    partition=execution_partition,
                )

            if upstream_has_no_data(df, empty_frames_have_data=False):
                print(f'\n\nNo data in upstream block {upstream_block.uuid}.')
                continue

            database_custom, schema_name, table_name = table_name_parts(
                configuration,
                upstream_block,
                dynamic_block_index=dynamic_block_index,
            )
            database = database_custom or database_default

            # This breaks io/bigquery.py:
            # BadRequest: 400 Syntax error:
            # Expected keyword ALL or keyword DISTINCT but got keyword SELECT at [3:140]

            # if query:
            #     for text in query.split(';'):
            #         try:
            #             text = interpolate_input_data(
            #                 block,
            #                 text,
            #                 loader,
            #             )
            #             text = template_render(text)
            #             for table in parse_one(text, read='bigquery').find_all(exp.Table):
            #                 if table_name == table.name:
            #                     continue

            #                 database = database or table.catalog
            #                 schema_name = schema_name or table.db
            #         except Exception as err:
            #             print(f'\n{err}')

            full_table_name = f'{schema_name}.{table_name}'
            print(f'\n\nExporting data from upstream block {upstream_block.uuid} '
                  f'to {database}.{full_table_name}.')

            loader.export(
                df,
                full_table_name,
                database=database,
                if_exists='replace',
                verbose=False,
            )


def interpolate_input_data(
    block,
    query: str,
    loader,
    dynamic_block_index: int = None,
    dynamic_upstream_block_uuids: List[str] = None,
):
    return interpolate_input(
        block,
        query,
        get_database=lambda opts: loader.default_database(),
        dynamic_block_index=dynamic_block_index,
        dynamic_upstream_block_uuids=dynamic_upstream_block_uuids,
    )
