import time
import uuid
from typing import Dict, List

from mage_ai.data_preparation.models.block.sql.utils.shared import (
    blocks_in_query,
    interpolate_input,
    should_cache_data_from_upstream,
    table_name_parts,
    upstream_has_no_data,
)
from mage_ai.data_preparation.models.constants import BlockType
from mage_ai.data_preparation.variable_manager import get_variable
from mage_ai.io.config import ConfigKey
from mage_ai.io.trino_types import quote

# The profile settings that make two Trino SQL blocks use the same schema, whose tables they
# read as they are. A 'trino' key, which no profile has, made every SQL upstream's table go
# through pandas into a copy.
CACHE_KEYS = [
    ConfigKey.TRINO_CATALOG,
    ConfigKey.TRINO_HOST,
    ConfigKey.TRINO_PORT,
    ConfigKey.TRINO_SCHEMA,
]


def unique_table_name_suffix(configuration: Dict) -> str:
    """
    With unique_upstream_table_name, the suffix of this run's upstream tables. It was the
    time in seconds, which two runs started in the same second shared.
    """
    if not (configuration or {}).get('unique_upstream_table_name', False):
        return None
    return f'{int(time.time())}_{uuid.uuid4().hex[:8]}'


def create_upstream_block_tables(
    loader,
    block,
    cascade_on_drop: bool = False,
    configuration: Dict = None,
    execution_partition: str = None,
    cache_upstream_dbt_models: bool = False,
    query: str = None,
    unique_table_name_suffix: str = None,
    dynamic_block_index: int = None,
    dynamic_upstream_block_uuids: List[str] = None,
    variables: Dict = None,
) -> Dict[str, str]:
    """
    Writes the upstream outputs the query reads to tables. Returns the full name of each
    table written, by upstream block UUID.
    """
    configuration = configuration if configuration else block.configuration
    database_default = configuration.get('data_provider_database') or loader.default_database()
    schema_default = configuration.get('data_provider_schema') or loader.default_schema()

    input_vars, kwargs_vars, upstream_block_uuids = block.fetch_input_variables(
        None,
        execution_partition=execution_partition,
        global_vars=None,
        dynamic_block_index=dynamic_block_index,
        dynamic_upstream_block_uuids=dynamic_upstream_block_uuids,
    )

    written = {}
    mapping = blocks_in_query(block, query)
    for idx, upstream_block in enumerate(block.upstream_blocks):
        if query and upstream_block.uuid not in mapping:
            continue

        should_cache = should_cache_data_from_upstream(block, upstream_block, [
            'data_provider',
        ], CACHE_KEYS)

        if should_cache:
            if BlockType.DBT == upstream_block.type \
                    and not cache_upstream_dbt_models:
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

            if upstream_has_no_data(df):
                print(f'\n\nNo data in upstream block {upstream_block.uuid}.')
                continue

            database_custom, schema_custom, table_name = table_name_parts(
                configuration,
                upstream_block,
                dynamic_block_index=dynamic_block_index,
            )

            database = database_custom or database_default
            schema = schema_custom or schema_default
            if unique_table_name_suffix:
                table_name = f'{table_name}_{unique_table_name_suffix}'

            full_table_name = '.'.join(list(filter(lambda x: x, [database, schema, table_name])))

            print(f'\n\nExporting data from upstream block {upstream_block.uuid} '
                  f'to {full_table_name}.')

            loader.export(
                df,
                schema,
                table_name,
                cascade_on_drop=cascade_on_drop,
                drop_table_on_replace=True,
                if_exists='replace',
                index=False,
                verbose=True,
            )
            written[upstream_block.uuid] = '.'.join(
                quote(name) for name in (loader.default_database(), schema, table_name)
            )

    return written


def drop_tables(loader, full_table_names: List[str]) -> None:
    """Drops this run's uniquely named upstream tables once the block no longer reads them."""
    for full_table_name in full_table_names:
        try:
            cursor = loader.conn.cursor()
            # A Trino statement finishes when its results are read.
            cursor.execute(f'DROP TABLE IF EXISTS {full_table_name}')
            cursor.fetchall()
        except Exception as error:
            print(f'Could not drop the upstream table {full_table_name}: {error}')


def interpolate_input_data(
    block,
    query: str,
    loader,
    unique_table_name_suffix: str = None,
    dynamic_block_index: int = None,
    dynamic_upstream_block_uuids: List[str] = None,
    written: Dict[str, str] = None,
):
    def _get_table(opts):
        table_name = opts['table']
        # Only the tables written for this run have the suffix. The table of a SQL block on
        # the same database is read as it is.
        if written is not None and opts.get('upstream_block_uuid') not in written:
            return table_name
        return f'{table_name}_{unique_table_name_suffix}'

    return interpolate_input(
        block,
        query,
        get_database=lambda opts: loader.default_database(),
        get_schema=lambda opts: loader.default_schema(),
        get_table=_get_table if unique_table_name_suffix else None,
        copied=lambda upstream: should_cache_data_from_upstream(
            block, upstream, ['data_provider'], CACHE_KEYS,
        ),
        dynamic_block_index=dynamic_block_index,
        dynamic_upstream_block_uuids=dynamic_upstream_block_uuids,
    )
