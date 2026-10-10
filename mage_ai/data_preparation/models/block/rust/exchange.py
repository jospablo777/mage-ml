"""
Values between Mage and a Rust block's process.

Tables cross as uncompressed Arrow IPC files: writing and reading them copies memory
without encoding. Other values cross as JSON. Outputs come back as Polars DataFrames,
Python values or a conditional decision.
"""
import json
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc

from mage_ai.data_preparation.models.block.rust.inputs import StoredTable


class RustExchangeError(Exception):
    pass


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, 'item'):
        return value.item()
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    return str(value)


def _write_arrow(table: pa.Table, path: str) -> None:
    with pa.OSFile(path, 'wb') as sink, ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)


def write_value(value: Any, directory: str, index: int) -> Dict:
    """Writes one upstream output and returns its entry in the job."""
    if value is None:
        return dict(kind='empty')
    if isinstance(value, StoredTable):
        return dict(kind='frame', format='parquet', path=value.path)
    frame_path = os.path.join(directory, f'input_{index}.arrow')
    if isinstance(value, pl.LazyFrame):
        value = value.collect()
    if isinstance(value, pl.DataFrame):
        value.write_ipc(frame_path, compression='uncompressed')
        return dict(kind='frame', format='ipc', path=os.path.basename(frame_path))
    if isinstance(value, pd.DataFrame):
        if hasattr(value, 'geometry') and type(value).__name__ == 'GeoDataFrame':
            value = pd.DataFrame(value.to_wkt())
        try:
            # Keeps a meaningful index as a column; a default range index is dropped.
            table = pa.Table.from_pandas(value, preserve_index=None)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as error:
            raise RustExchangeError(
                f'Upstream output {index + 1} cannot be passed to Rust as a table: {error}',
            ) from error
        _write_arrow(table, frame_path)
        return dict(kind='frame', format='ipc', path=os.path.basename(frame_path))
    if isinstance(value, pa.Table):
        _write_arrow(value, frame_path)
        return dict(kind='frame', format='ipc', path=os.path.basename(frame_path))
    if isinstance(value, pd.Series):
        value = value.to_list()
    if isinstance(value, (dict, list, str, int, float, bool)):
        path = os.path.join(directory, f'input_{index}.json')
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(value, file, default=_json_default, allow_nan=False)
        return dict(kind='json', path=os.path.basename(path))
    raise RustExchangeError(
        f'Upstream output {index + 1} is of type {type(value).__name__}; Rust blocks take tables '
        '(pandas or Polars DataFrames, Arrow tables) and JSON values.',
    )


def write_inputs(values: List[Any], directory: str) -> List[Dict]:
    return [write_value(value, directory, index) for index, value in enumerate(values)]


def variables_for_rust(global_vars: Dict) -> Tuple[Dict, List[str]]:
    """The variables that JSON can hold, and the names of the others."""
    values, skipped = {}, []
    for name, value in (global_vars or {}).items():
        try:
            values[name] = json.loads(json.dumps(value, default=_json_default, allow_nan=False))
        except (TypeError, ValueError):
            skipped.append(name)
    return values, skipped


def read_outputs(result: Dict, job_dir: str) -> Tuple[List[Any], List[Dict]]:
    """The outputs and test results that the binary reported."""
    outputs = []
    for record in result.get('outputs') or []:
        kind = record.get('kind')
        if kind == 'frame':
            path = os.path.join(job_dir, record['path'])
            # From bytes in memory, never a memory map: the job directory is removed
            # after the run.
            with open(path, 'rb') as file:
                outputs.append(pl.read_ipc(file.read()))
        elif kind == 'json':
            with open(os.path.join(job_dir, record['path']), encoding='utf-8') as file:
                outputs.append(json.load(file))
        elif kind == 'decision':
            outputs.append(bool(record['value']))
        else:
            raise RustExchangeError(f'The Rust block reported an unknown output: {record}')
    return outputs, result.get('tests') or []
