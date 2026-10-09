import json
import uuid
from datetime import datetime
from typing import Dict, List

import pandas as pd
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
    A frame of Singer records. Columns the schema types as integer are nullable
    integers: pd.DataFrame made an integer column with a null float64, which rounds
    values above 2**53.
    """
    names = list(dict.fromkeys(name for record in records for name in record))
    columns = {}
    for name in names:
        values = [record.get(name) for record in records]
        types = ((properties or {}).get(name) or {}).get('type') or []
        if isinstance(types, str):
            types = [types]
        if 'integer' in types:
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
        result[name] = result[name].map(as_json)
    return result


def batch_file_name(time: datetime, file_type: str) -> str:
    """
    A file name for one batch of records. Names had one-second resolution, so batches
    written within the same second replaced each other.
    """
    return f'{time:%Y%m%d-%H%M%S-%f}-{uuid.uuid4().hex[:8]}.{file_type}'
