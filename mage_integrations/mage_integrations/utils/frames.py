import codecs
import io
import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc


def records_from_frame(df: pd.DataFrame) -> List[Dict]:
    """
    Rows of df as dicts, with None for every missing value.

    pandas 3 stores missing text as NaN in the str dtype, and NA and NaT mark missing
    values in other dtypes. Singer records are JSON, where NaN is not a value: a
    destination would receive a float in a string field.

    pyarrow-backed columns are converted by pyarrow: as Python objects, their lists
    became NumPy arrays, with integers turned into floats and nulls into NaN.
    """
    columns = []
    for position in range(df.shape[1]):
        column = df.iloc[:, position]
        if isinstance(column.dtype, pd.ArrowDtype):
            values = pa.array(column.array)
            if pa.types.is_floating(values.type):
                values = pc.if_else(pc.is_nan(values), None, values)
            columns.append(values.to_pylist())
        else:
            columns.append(column.astype(object).where(column.notna(), None).tolist())
    return [dict(zip(df.columns, row)) for row in zip(*columns)]


def frame_from_records(records: List[Dict], properties: Dict = None) -> pd.DataFrame:
    """
    A frame of Singer records. Columns the schema types as integer, and columns without
    a schema type whose values are all integers, are nullable integers: pd.DataFrame
    made an integer column with a null float64, which rounds values above 2**53.
    """
    names = list(dict.fromkeys(name for record in records for name in record))
    columns = {}
    for name in names:
        values = [record.get(name) for record in records]
        types = ((properties or {}).get(name) or {}).get('type') or []
        if isinstance(types, str):
            types = [types]
        present = [value for value in values if value is not None]
        integers = bool(present) and all(
            isinstance(value, int) and not isinstance(value, bool) for value in present
        )
        if 'integer' in types or (not types and integers):
            try:
                columns[name] = pd.array(values, dtype='Int64')
                continue
            except (TypeError, ValueError, OverflowError):
                # Values that are not int64 integers keep pandas' inferred type.
                pass
        columns[name] = values
    return pd.DataFrame(columns, index=pd.RangeIndex(len(records)))


def nested_values_as_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    df with dicts and lists as JSON text, for CSV files. pandas writes them as Python
    reprs, such as {'a': None}, which JSON readers cannot parse.
    """
    def as_json(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value, default=str)
        return value

    result = df.copy(deep=False)
    for name in result.select_dtypes(include='object', exclude='str').columns:
        result[name] = result[name].map(as_json, na_action='ignore')
    return result


def batch_file_name(time: datetime, file_type: str) -> str:
    """
    A file name for one batch of records. Names had one-second resolution, so batches
    written within the same second replaced each other.
    """
    return f'{time:%Y%m%d-%H%M%S-%f}-{uuid.uuid4().hex[:8]}.{file_type}'


def read_file_frame(content: bytes, file_type: str, encoding: Optional[str] = None) -> pd.DataFrame:
    """
    A Parquet or CSV file as pyarrow-backed pandas, read by Polars.

    pd.read_parquet turned integer columns with nulls into floats, rounding values above
    2**53, and pyarrow before 26 cannot read fixed-size lists that hold a null. pandas'
    CSV parser read -2**63 as missing in a column with nulls, and dropped null rows of
    one-column files. CSV in another encoding is decoded first, since Polars reads UTF-8.
    """
    if file_type == 'parquet':
        frame = pl.read_parquet(io.BytesIO(content))
    elif file_type == 'csv':
        if encoding and codecs.lookup(encoding).name != 'utf-8':
            content = content.decode(encoding).encode('utf-8')
        frame = pl.read_csv(io.BytesIO(content), infer_schema_length=None)
    else:
        return pd.DataFrame()
    return frame.to_pandas(use_pyarrow_extension_array=True)


def nested_column_type(dtype) -> Optional[str]:
    """The JSON schema type of a pyarrow-backed list or struct column."""
    if not isinstance(dtype, pd.ArrowDtype):
        return None
    arrow_type = dtype.pyarrow_dtype
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return 'array'
    if pa.types.is_struct(arrow_type) or pa.types.is_map(arrow_type):
        return 'object'
    return None
