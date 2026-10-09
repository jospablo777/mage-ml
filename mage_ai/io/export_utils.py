from typing import Any, Callable, Dict, List, Mapping, Optional

import pandas as pd
import polars as pl
import pyarrow as pa
from pandas import DataFrame, Series
from pandas.api.types import infer_dtype

from mage_ai.shared.enum import StrEnum
from mage_ai.shared.utils import clean_name

"""
Utilities for exporting Python data frames to external databases.
"""


class BadConversionError(Exception):
    """
    Unable to convert Python-based data type to SQL equivalent.
    """

    pass


class PandasTypes(StrEnum):
    """
    Internal datatypes defined by the pandas Public API
    """

    BOOLEAN = 'boolean'
    BYTES = 'bytes'
    CATEGORICAL = 'categorical'
    COMPLEX = 'complex'
    DATE = 'date'
    DATETIME = 'datetime'
    DATETIME64 = 'datetime64'
    DECIMAL = 'decimal'
    INTEGER = 'integer'
    INT64 = 'int64'
    EMPTY = 'empty'
    FLOATING = 'floating'
    MIXED = 'mixed'
    MIXED_INTEGER = 'mixed-integer'
    MIXED_INTEGER_FLOAT = 'mixed-integer-float'
    OBJECT = 'object'
    PERIOD = 'period'
    STRING = 'string'
    TIME = 'time'
    TIMEDELTA = 'timedelta'
    TIMEDELTA64 = 'timedelta64'
    UNKNOWN_ARRAY = 'unknown-array'


def infer_dtypes(df: DataFrame) -> Dict[str, str]:
    """
    Fetches the internal pandas datatypes for the columns in the data frame.

    Args:
        df (DataFrame): Data frame to fetch dtypes from.

    Returns:
        Dict[str, str]: Map of column names to inferred dtypes
    """
    columns = []
    if type(df) is DataFrame:
        columns = df.columns
        return {column: infer_dtype(df[column], skipna=True) for column in columns}
    elif type(df) is dict:
        columns = df.keys()
        return {column: type(df[column]) for column in columns}

    return {}


def clean_df_for_export(
    df: DataFrame,
    column_mapper: Callable[[Series, str], Series],
    dtypes: Mapping[str, str],
) -> DataFrame:
    """
    Cleans data frame with the appropriate steps to prepare loading the data frame
    to the target database.

    Args:
        df (DataFrame): Data frame to clean.
        column_mapper (Callable[[Series, str], str]): Function that cleans a column given the
        pandas data type.
        dtypes (Mapping[str, str]): Name of the new table to create

    Returns:
        str: Table creation query for this table.
    """
    # Columns are replaced, and copy-on-write keeps the caller's frame unchanged.
    copy_df = df.copy(deep=False)

    columns = []
    if type(df) is DataFrame:
        columns = df.columns
    elif type(df) is dict:
        columns = df.keys()

    for column in columns:
        copy_df[column] = column_mapper(copy_df[column], dtypes[column])
    return copy_df


def gen_table_creation_query(
    dtypes: Mapping[str, str],
    schema_name: str,
    table_name: str,
    auto_clean_name: bool = True,
    case_sensitive: bool = False,
    unique_constraints: List[str] = None,
    overwrite_types: Dict = None,
    skip_semicolon_at_end: bool = False,
) -> str:
    """
    Generates a database table creation query from a data frame.

    Args:
        dtypes (Mapping[str, str]): Database relative data types for each column of
        the data frame.
        schema_name (str): Name of schema to create new table in.
        table_name (str): Name of the new table to create.

    Returns:
        str: Table creation query for this table.
    """
    if unique_constraints is None:
        unique_constraints = []
    query = []
    for cname in dtypes:
        if overwrite_types is not None and cname in overwrite_types.keys():
            dtypes[cname] = overwrite_types[cname]

        if auto_clean_name:
            cleaned_col_name = clean_name(cname, case_sensitive=case_sensitive)
        else:
            cleaned_col_name = cname

        query.append(f'"{cleaned_col_name}" {dtypes[cname]}')

    if schema_name:
        full_table_name = f'{schema_name}.{table_name}'
    else:
        full_table_name = table_name

    if unique_constraints:
        unique_constraints_clean = []
        for col in unique_constraints:
            if auto_clean_name:
                cleaned_col_name = clean_name(col, case_sensitive=case_sensitive)
            else:
                cleaned_col_name = col
            unique_constraints_clean.append(cleaned_col_name)
        unique_constraints_escaped = [f'"{col}"'
                                      for col in unique_constraints_clean]
        index_name = '_'.join([
            clean_name(full_table_name, case_sensitive=case_sensitive),
        ] + unique_constraints_clean)
        index_name = f'unique{index_name}'[:64]
        query.append(
            f"CONSTRAINT {index_name} UNIQUE ({', '.join(unique_constraints_escaped)})",
        )
    if skip_semicolon_at_end:
        return f'CREATE TABLE {full_table_name} (' + ','.join(query) + ')'
    return f'CREATE TABLE {full_table_name} (' + ','.join(query) + ');'


def to_pandas_frame(df: Any) -> Any:
    """
    Return a Polars DataFrame or LazyFrame as a pandas DataFrame, for exporters that build
    their requests from pandas. Other values are returned unchanged.
    """
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    if isinstance(df, pl.DataFrame):
        return polars_to_pandas(df)
    return df


# pandas' nullable types for the Arrow integer and boolean types.
_NULLABLE_PANDAS_TYPES = {
    pa.int8(): pd.Int8Dtype(),
    pa.int16(): pd.Int16Dtype(),
    pa.int32(): pd.Int32Dtype(),
    pa.int64(): pd.Int64Dtype(),
    pa.uint8(): pd.UInt8Dtype(),
    pa.uint16(): pd.UInt16Dtype(),
    pa.uint32(): pd.UInt32Dtype(),
    pa.uint64(): pd.UInt64Dtype(),
    pa.bool_(): pd.BooleanDtype(),
    # to_pandas turned dates into datetime64.
    pa.date32(): pd.ArrowDtype(pa.date32()),
}


def polars_to_pandas(frame: pl.DataFrame) -> DataFrame:
    """
    A Polars frame as pandas for the exporters that build statements from pandas.
    Integer and boolean columns become pandas' nullable types: to_pandas made an integer
    column with a null float64, which rounds values above 2**53. Dates stay dates.
    """
    wide = [
        name for name, dtype in frame.schema.items() if dtype in (pl.Int128, pl.UInt128)
    ]
    # pyarrow cannot import Polars' 128-bit integers; they become Python ints.
    result = frame.drop(wide).to_pandas(types_mapper=_NULLABLE_PANDAS_TYPES.get)
    for name in wide:
        result[name] = pd.Series(frame[name].to_list(), index=result.index, dtype=object)
    return result[frame.columns]


def insert_rows(
    df: DataFrame,
    serialize: Optional[Callable[[Any], Any]] = None,
    strip_quotes: bool = False,
    stringify_timestamps: bool = False,
) -> List[tuple]:
    """
    Rows of df as tuples of Python values for a driver's executemany, with None for every
    missing value.

    serialize is applied to object columns, and strip_quotes removes surrounding double
    quotes from text values. iterrows built a Series per row, which turned every value of
    a numeric-only frame into float: 2**53 + 1 became 9007199254740992.0. replace with
    np.nan: None left NaN in float and str columns under pandas 3.
    """
    from pandas.api.types import is_object_dtype, is_string_dtype

    from mage_ai.shared.pandas_utils import missing_as_none

    frame = df.copy(deep=False)
    for column in frame.columns:
        series = frame[column]
        if serialize is not None and is_object_dtype(series.dtype):
            series = series.map(serialize)
        if strip_quotes and (is_object_dtype(series.dtype) or is_string_dtype(series.dtype)):
            series = series.map(lambda v: v.strip('"') if isinstance(v, str) and v else v)
        frame[column] = series
    rows = missing_as_none(frame).itertuples(index=False, name=None)
    if stringify_timestamps:
        from pandas import Timestamp

        return [tuple(str(v) if isinstance(v, Timestamp) else v for v in row) for row in rows]
    return list(rows)
