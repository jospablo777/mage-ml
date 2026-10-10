"""
Upstream outputs that a Rust block reads from their stored Parquet files.

Mage loads every upstream output into Python before a block runs. A Rust block reads
tables itself, so for an upstream table stored as one local Parquet file, the block gets
the file: Polars then reads only the columns and rows the block's query needs, and the
table never passes through Python. The file must hold exactly what loading it would
give: Polars outputs always do; pandas outputs do when every column has a plain type.
Anything else is loaded as for any block.
"""
import json
import os
import re
from dataclasses import dataclass
from typing import List, Optional

from mage_ai.data_preparation.models.variables.constants import (
    DATAFRAME_COLUMN_TYPES_FILE,
    DATAFRAME_PANDAS_METADATA_FILE,
    DATAFRAME_PARQUET_FILE,
    VariableType,
)

# pandas column types that Parquet stores and reads back unchanged.
_PLAIN_PANDAS_TYPE = re.compile(
    r'^(u?int(8|16|32|64)|U?Int(8|16|32|64)|float(32|64)|Float(32|64)|bool|boolean|str|'
    r'string|datetime64\[(ns|us|ms|s)(, [^\]]+)?\])$',
)


@dataclass(frozen=True)
class StoredTable:
    """An upstream table in a Parquet file, passed to the block by path."""

    path: str


def _pandas_columns_are_plain(directory: str) -> bool:
    if os.path.exists(os.path.join(directory, DATAFRAME_PANDAS_METADATA_FILE)):
        return False
    try:
        with open(os.path.join(directory, DATAFRAME_COLUMN_TYPES_FILE), encoding='utf-8') as file:
            column_types = json.load(file)
    except (OSError, ValueError):
        return False
    if not isinstance(column_types, dict) or not column_types:
        return False
    return all(
        isinstance(kind, str) and _PLAIN_PANDAS_TYPE.match(kind)
        for kind in column_types.values()
    )


def stored_table(block, upstream_uuid: str, partition: Optional[str]) -> Optional[StoredTable]:
    from mage_ai.data_preparation.storage.local_storage import LocalStorage

    pipeline = block.pipeline
    upstream = pipeline.get_block(upstream_uuid) if pipeline else None
    if upstream is None:
        return None
    manager = pipeline.variable_manager
    names = manager.get_variables_by_block(pipeline.uuid, upstream_uuid, partition=partition)
    if list(names) != ['output_0']:
        return None
    variable = manager.get_variable_object(
        pipeline.uuid, upstream_uuid, 'output_0', partition=partition,
    )
    if not isinstance(variable.storage, LocalStorage):
        return None
    path = os.path.join(variable.variable_path, DATAFRAME_PARQUET_FILE)
    if not os.path.isfile(path):
        return None
    if variable.variable_type == VariableType.POLARS_DATAFRAME:
        return StoredTable(path)
    if variable.variable_type == VariableType.DATAFRAME:
        if _pandas_columns_are_plain(variable.variable_path):
            return StoredTable(path)
    return None


def stored_tables(block, upstream_uuids: List[str], partition: Optional[str]):
    """The stored table of every upstream block, or None to load them as usual."""
    from mage_ai.data_preparation.models.block.dynamic.utils import (
        is_dynamic_block,
        is_dynamic_block_child,
    )

    if os.getenv('MAGE_RUST_DIRECT_INPUTS', '1') == '0':
        return None
    if getattr(block, 'is_dynamic_v2', False) or any(
        is_dynamic_block(upstream) or is_dynamic_block_child(upstream)
        for upstream in block.upstream_blocks
    ):
        return None
    tables = []
    for uuid in upstream_uuids:
        table = stored_table(block, uuid, partition)
        if table is None:
            return None
        tables.append(table)
    return tables
