import json
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Dict, Iterator, List, Optional, Tuple, Union

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq


def _with_item_fields(arrow_type: pa.DataType) -> pa.DataType:
    """
    Parquet names the value field of a list "element"; Arrow and pandas name it "item".
    """
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        build = pa.list_ if pa.types.is_list(arrow_type) else pa.large_list
        value = arrow_type.value_field
        return build(pa.field('item', _with_item_fields(value.type), value.nullable))
    if pa.types.is_struct(arrow_type):
        return pa.struct([
            field.with_type(_with_item_fields(field.type)) for field in arrow_type
        ])
    return arrow_type


PYARROW_READS_NULL_FIXED_SIZE_LISTS = int(pa.__version__.split('.')[0]) >= 26

def _has_fixed_size_list(arrow_type: pa.DataType) -> bool:
    if pa.types.is_fixed_size_list(arrow_type):
        return True
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return _has_fixed_size_list(arrow_type.value_type)
    if pa.types.is_struct(arrow_type):
        return any(_has_fixed_size_list(field.type) for field in arrow_type)
    return False


def read_parquet_table(source, columns: Optional[List[str]] = None, **kwargs) -> pa.Table:
    """
    Read a Parquet file into an Arrow table.

    pyarrow before 26 fails on fixed-size list columns that hold a null ("Expected all
    lists to be of size=2 but index 1 had size=0"), apache/arrow#35692. Polars writes
    such files, and pyarrow 25 does too. Polars reads them, so they are read with Polars
    and cast back to the schema stored in the file.
    """
    if PYARROW_READS_NULL_FIXED_SIZE_LISTS or kwargs:
        return pq.read_table(source, columns=columns, **kwargs)
    schema = pq.read_schema(source)
    if hasattr(source, 'seek'):
        source.seek(0)
    if not any(_has_fixed_size_list(field.type) for field in schema):
        return pq.read_table(source, columns=columns, **kwargs)
    names = columns or schema.names
    table = pl.read_parquet(source, columns=names).to_arrow(
        compat_level=pl.CompatLevel.oldest(),
    )
    return table.select(names).cast(
        pa.schema([schema.field(name) for name in names], metadata=schema.metadata),
    )


def read_pandas_parquet(
    source,
    columns: Optional[List[str]] = None,
    arrow_columns: Optional[List[str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Read a Parquet file written from a pandas frame.

    pd.read_parquet fails on a list or struct column that pandas wrote from a
    pd.ArrowDtype column, because pyarrow cannot parse the dtype name pandas stores in the
    file, and it returns decimal columns as objects. Columns pandas recorded with a
    pyarrow dtype are converted here as pd.ArrowDtype columns; the rest of the table
    converts as in pd.read_parquet. pandas records a decimal pyarrow column as object,
    so callers that know which columns had a pyarrow dtype pass them in arrow_columns.
    Other keyword arguments, such as filters, go to pyarrow.parquet.read_table.
    """
    # pd.read_parquet options that pyarrow.parquet.read_table does not take.
    kwargs.pop('engine', None)
    table = read_parquet_table(source, columns=columns, **kwargs)
    metadata = table.schema.pandas_metadata
    if not metadata:
        return table.to_pandas()

    arrow_columns = {
        entry['field_name']
        for entry in metadata.get('columns', [])
        if (
            str(entry.get('numpy_type', '')).endswith('[pyarrow]')
            or entry['field_name'] in (arrow_columns or ())
        )
        and entry['field_name'] in table.column_names
    }
    if not arrow_columns:
        return table.to_pandas()

    names = table.column_names
    arrays = {
        name: table.column(name).cast(_with_item_fields(table.schema.field(name).type))
        for name in arrow_columns
    }
    metadata['columns'] = [
        entry for entry in metadata['columns'] if entry['field_name'] not in arrow_columns
    ]
    rest = table.drop_columns(list(arrow_columns))
    schema_metadata = dict(rest.schema.metadata or {})
    schema_metadata[b'pandas'] = json.dumps(metadata).encode()
    frame = rest.replace_schema_metadata(schema_metadata).to_pandas()
    if len(frame.index) != table.num_rows:
        # A table with no other columns converts to an empty frame. Its index can only
        # be a range index, since a stored index is a column of the table.
        index = next(
            (i for i in metadata.get('index_columns', []) if isinstance(i, dict)),
            {},
        )
        frame = pd.DataFrame(index=pd.RangeIndex(
            index.get('start', 0),
            index.get('stop', table.num_rows),
            index.get('step', 1),
            name=index.get('name'),
        ))
    for name in arrow_columns:
        frame[name] = pd.Series(
            pd.array(arrays[name], dtype=pd.ArrowDtype(arrays[name].type)),
            index=frame.index,
        )
    data_columns = [name for name in names if name in frame.columns]
    return frame[data_columns]


class BaseStorage(ABC):
    @abstractmethod
    def isdir(self, path: str) -> bool:
        """
        Check whether a path is a directory.
        """
        pass

    @abstractmethod
    def listdir(self, path: str, suffix: str = None) -> List[str]:
        """
        Get a list of files and directories in the specified directory.
        """
        pass

    @abstractmethod
    def makedirs(self, path: str, **kwargs) -> None:
        """
        Create new directories.
        """
        pass

    @abstractmethod
    def path_exists(self, path: str) -> bool:
        """
        Check whether a path exists.
        """
        pass

    @abstractmethod
    def remove(self, path: str) -> None:
        """
        Remove a file with file path.
        """
        pass

    @abstractmethod
    def remove_dir(self, path: str) -> None:
        """
        Remove a directory with directory path.
        """
        pass

    @abstractmethod
    def read_parquet(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Read parquet from a file with file path and return a pandas DataFrame.
        """
        pass

    def read_bytes(self, file_path: str) -> bytes:
        """The content of a file."""
        with open(file_path, 'rb') as file:
            return file.read()

    @abstractmethod
    def read_polars_parquet(self, file_path: str, **kwargs) -> pl.DataFrame:
        """
        Read parquet from a file with file path and return a polars DataFrame.
        """
        pass

    @abstractmethod
    def read_json_file(
        self,
        file_path: str,
        default_value: Optional[Union[Dict, List]] = None,
        raise_exception: bool = False,
    ) -> Dict:
        """
        Read json from a file with file path.
        """
        pass

    @abstractmethod
    async def read_json_file_async(
        self,
        file_path: str,
        default_value: Dict = None,
        raise_exception: bool = False,
    ) -> Dict:
        """
        Read json from a file with file path asynchronously.
        """
        pass

    @abstractmethod
    def write_json_file(self, file_path: str, data) -> None:
        """
        Write json to a file with file path.
        """
        pass

    @abstractmethod
    async def write_json_file_async(self, file_path: str, data) -> None:
        """
        Write json to a file with file path asynchronously.
        """
        pass

    @abstractmethod
    def write_parquet(self, df, file_path: str) -> None:
        """
        Write Pandas dataframe to a file in parquet format.
        """
        pass

    @abstractmethod
    def write_polars_dataframe(self, df, file_path: str) -> None:
        """
        Write Polars dataframe to a file in parquet format.
        """
        pass

    @abstractmethod
    @contextmanager
    def open_to_write(self, file_path: str) -> None:
        pass

    @abstractmethod
    async def read_async(self, file_path: str) -> str:
        pass

    def polars_location(self, path: str) -> Optional[Tuple[str, Optional[Dict]]]:
        """
        The path or URI Polars reads and writes a file at, with its storage options.
        None when Polars cannot reach the storage, so frames go through this class.
        """
        return None

    @contextmanager
    def writing(self, file_path: str) -> Iterator[str]:
        """
        The path to write file_path through. Objects in S3 and GCS appear whole when
        their upload completes, so the path itself is returned.
        """
        yield file_path
