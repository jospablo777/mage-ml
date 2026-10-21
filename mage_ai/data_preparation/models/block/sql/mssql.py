"""
SQL blocks on SQL Server. A name without a schema is the login's default schema (usually
dbo), not MSSQL_SCHEMA: upstream tables are named with the schema they were written to,
and the block's table is read back from its schema. Named alone, they were read from the
default schema, so a profile or block with another schema failed (I-271).
"""
from typing import Any, Dict, List

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.block.sql.utils.shared import (
    create_upstream_block_tables as create_upstream_block_tables_orig,
)
from mage_ai.data_preparation.models.block.sql.utils.shared import (
    has_create_or_insert_statement,
    interpolate_input,
    should_cache_data_from_upstream,
    split_query_string,
)
from mage_ai.io.config import ConfigKey

# The profile settings that make two SQL Server SQL blocks use the same database.
CACHE_KEYS = [
    ConfigKey.MSSQL_DATABASE,
    ConfigKey.MSSQL_SCHEMA,
    ConfigKey.MSSQL_HOST,
    ConfigKey.MSSQL_PORT,
]

def create_upstream_block_tables(
    loader,
    block,
    cascade_on_drop: bool = False,
    configuration: Dict = None,
    execution_partition: str = None,
    cache_upstream_dbt_models: bool = False,
    query: str = None,
    dynamic_block_index: int = None,
    dynamic_upstream_block_uuids: List[str] = None,
    variables: Dict = None,
):
    create_upstream_block_tables_orig(
        loader,
        block,
        cascade_on_drop,
        configuration,
        execution_partition,
        cache_upstream_dbt_models,
        cache_keys=CACHE_KEYS,
        no_schema=False,
        query=query,
        schema_name=loader.default_schema(),
        dynamic_block_index=dynamic_block_index,
        dynamic_upstream_block_uuids=dynamic_upstream_block_uuids,
        variables=variables,
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
        lambda db, schema, tn: f'{schema}.{tn}' if schema else tn,
        get_schema=lambda opts: loader.default_schema(),
        copied=lambda upstream: should_cache_data_from_upstream(
            block, upstream, ['data_provider'], CACHE_KEYS,
        ),
        dynamic_block_index=dynamic_block_index,
        dynamic_upstream_block_uuids=dynamic_upstream_block_uuids,
    )


def execute_raw_sql(
    loader,
    block: Block,
    query_string: str,
    configuration: Dict = None,
    should_query: bool = False,
    disable_query_preprocessing: bool = False,
    limit: int = 1000,
) -> List[Any]:
    if configuration is None:
        configuration = {}

    if disable_query_preprocessing:
        return loader.execute_query_raw(
            query_string,
            configuration=configuration,
        )

    queries = []
    fetch_query_at_indexes = []

    has_create_or_insert = has_create_or_insert_statement(query_string)

    for query in split_query_string(query_string):
        if has_create_or_insert:
            queries.append(query)
            fetch_query_at_indexes.append(False)
        else:
            queries.append(query)
            fetch_query_at_indexes.append(True)

    if should_query and has_create_or_insert:
        # Within the block's limit, as for the other databases; it was 1000 rows.
        queries.append(f'SELECT TOP({int(limit)}) * FROM {block.full_table_name}')
        fetch_query_at_indexes.append(block.full_table_name)

    results = loader.execute_queries(
        queries,
        commit=True,
        fetch_query_at_indexes=fetch_query_at_indexes,
    )

    if should_query:
        return [results[-1]]

    return []
