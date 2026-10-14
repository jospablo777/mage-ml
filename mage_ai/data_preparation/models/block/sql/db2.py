"""SQL blocks on IBM Db2. Upstream outputs become tables in the profile's schema, which the
connection sets as current, so blocks name them without a schema."""
from typing import Any, Dict, List

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.block.sql.utils.shared import (
    create_upstream_block_tables as create_upstream_block_tables_orig,
)
from mage_ai.data_preparation.models.block.sql.utils.shared import (
    has_create_or_insert_statement,
    interpolate_input,
    split_query_string,
)
from mage_ai.io.config import ConfigKey


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
        cache_keys=[
            ConfigKey.DB2_DATABASE,
            ConfigKey.DB2_SCHEMA,
            ConfigKey.DB2_HOST,
            ConfigKey.DB2_PORT,
        ],
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
    dynamic_block_index: int = None,
    dynamic_upstream_block_uuids: List[str] = None,
):
    return interpolate_input(
        block,
        query,
        lambda db, schema, tn: tn,
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
        queries.append(
            f'SELECT * FROM {block.full_table_name} FETCH FIRST {int(limit)} ROWS ONLY',
        )
        fetch_query_at_indexes.append(block.full_table_name)

    results = loader.execute_queries(
        queries,
        commit=True,
        fetch_query_at_indexes=fetch_query_at_indexes,
    )

    if should_query:
        return [results[-1]]

    return []
