import base64
import decimal
import importlib
import inspect
import json
import os
import traceback
import uuid
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from warnings import warn

import numpy as np
import pandas as pd
import polars as pl
import yaml
from pandas import DataFrame
from sklearn.utils import estimator_html_repr

from mage_ai.data_cleaner.shared.utils import is_geo_dataframe, is_spark_dataframe
from mage_ai.data_preparation.models.project import Project
from mage_ai.data_preparation.models.project.constants import FeatureUUID
from mage_ai.data_preparation.models.variables.constants import VariableType
from mage_ai.settings.platform.constants import user_project_platform_activated
from mage_ai.shared.complex import is_model_sklearn, is_model_xgboost
from mage_ai.shared.hash import unflatten_dict
from mage_ai.shared.outputs import load_custom_object, save_custom_object
from mage_ai.shared.parsers import (
    convert_matrix_to_dataframe,
    encode_complex,
    object_to_dict,
    object_to_uuid,
)

MAX_PARTITION_BYTE_SIZE = 100 * 1024 * 1024
JSON_SERIALIZABLE_COLUMN_TYPES = {
    dict.__name__,
    list.__name__,
}
STRING_SERIALIZABLE_COLUMN_TYPES = {
    'ObjectId',
}
# Column types of object columns stored as text and read back as their type.
DECIMAL_COLUMN_TYPE = 'Decimal'
FLOAT_COLUMN_TYPE = 'float_with_nan'
TIMETZ_COLUMN_TYPE = 'timetz'
UUID_COLUMN_TYPE = 'UUID'
# Categorical columns whose categories are not strings. Parquet keeps the dictionary of a
# string column only, and cannot store interval categories, so the column is stored as its
# codes and the categories are kept in DATAFRAME_PANDAS_METADATA_FILE.
CATEGORY_CODES_COLUMN_TYPE = 'category_codes'
# Object columns whose values differ in type, or that Arrow cannot hold, such as integers
# beyond 64 bits. Each value is stored as tagged JSON and read back with its type.
OBJECT_JSON_COLUMN_TYPE = 'object_json'

AMBIGUOUS_COLUMN_TYPES = {
    'mixed-integer',
    'complex',
    'unknown-array',
}

CAST_TYPE_COLUMN_TYPES = {
    'Int64',
    'int64',
    'float64',
}

POLARS_CAST_TYPE_COLUMN_TYPES = {
    'Float64': pl.Float64,
    'Int64': pl.Int64,
}


# Values inside dict and list columns that JSON has no type for are written as
# {"__mage_type__": name, "value": text} and restored on read. Variables written before
# the tags existed read the same as before.
TYPE_TAG = '__mage_type__'


def _tag(name: str, value: Any) -> Dict:
    return {TYPE_TAG: name, 'value': value}


def _encode_tagged(value: Any) -> Any:
    if isinstance(value, decimal.Decimal):
        return _tag('decimal', str(value))
    if isinstance(value, datetime):
        return _tag('datetime', value.isoformat())
    if isinstance(value, date):
        return _tag('date', value.isoformat())
    if isinstance(value, time):
        return _tag('time', value.isoformat())
    if isinstance(value, timedelta):
        return _tag('timedelta', [value.days, value.seconds, value.microseconds])
    if isinstance(value, (bytes, bytearray, memoryview)):
        return _tag('bytes', base64.b64encode(bytes(value)).decode('ascii'))
    if isinstance(value, uuid.UUID):
        return _tag('uuid', str(value))
    if isinstance(value, complex):
        return _tag('complex', [repr(value.real), repr(value.imag)])
    if isinstance(value, pd.Interval):
        return _tag('interval', [value.left, value.right, value.closed])
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    return encode_complex(value)


DECODERS = {
    'decimal': decimal.Decimal,
    'datetime': datetime.fromisoformat,
    'date': date.fromisoformat,
    'time': time.fromisoformat,
    'timedelta': lambda parts: timedelta(days=parts[0], seconds=parts[1], microseconds=parts[2]),
    'bytes': base64.b64decode,
    'uuid': uuid.UUID,
    'complex': lambda parts: complex(float(parts[0]), float(parts[1])),
    'interval': lambda parts: pd.Interval(parts[0], parts[1], closed=parts[2]),
}


def _decode_tagged(obj: Dict) -> Any:
    if len(obj) == 2 and obj.get(TYPE_TAG) in DECODERS and 'value' in obj:
        return DECODERS[obj[TYPE_TAG]](obj['value'])
    return obj


# json.dumps and json.loads build a new encoder or decoder on every call when given
# options, which costs more than encoding a small value. These are built once.
_TAGGED_ENCODER = json.JSONEncoder(default=_encode_tagged, allow_nan=True, ensure_ascii=False)
_TAGGED_DECODER = json.JSONDecoder(object_hook=_decode_tagged)


def serialize_json_value(value: Any) -> Any:
    # A NaN in place of the whole value is pandas' missing marker.
    if _is_missing(value):
        return None
    # The json module hands bytes and Decimal to the default function; simplejson decodes
    # bytes as UTF-8 first. NaN and infinity are written as tokens loads accepts.
    return _TAGGED_ENCODER.encode(value)


# Types each value of an OBJECT_JSON_COLUMN_TYPE column may have. Tuples and sets are left
# out because JSON would return them as lists.
OBJECT_JSON_VALUE_TYPES = (
    bool, int, float, complex, str, bytes, dict, list,
    decimal.Decimal, date, time, timedelta, uuid.UUID, np.generic, np.ndarray,
)
INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1


def needs_object_json(values: List[Any]) -> bool:
    """
    Whether an object column must be stored as tagged JSON to keep its values: values of
    more than one type, complex numbers, or integers beyond 64 bits. Columns holding
    other objects keep the default handling.
    """
    kinds = {type(v) for v in values}
    if not kinds or not all(issubclass(k, OBJECT_JSON_VALUE_TYPES) for k in kinds):
        return False
    if len(kinds) > 1:
        return True
    kind = kinds.pop()
    if issubclass(kind, complex):
        return True
    if kind is int:
        return any(v < INT64_MIN or v > INT64_MAX for v in values)
    return False


# Dtypes that Parquet cannot store or stores at another resolution. The column is written
# as values Parquet holds and cast back to the recorded dtype on read.
def restorable_dtype(dtype: Any) -> bool:
    if isinstance(dtype, pd.SparseDtype):
        return True
    return isinstance(dtype, np.dtype) and (
        dtype.kind == 'c' or dtype in (np.dtype('datetime64[s]'), np.dtype('timedelta64[s]'))
    )


def restore_column_dtypes(df: pd.DataFrame, column_types: Dict) -> pd.DataFrame:
    for column, column_type in column_types.items():
        if column not in df.columns or not isinstance(column_type, str):
            continue
        if not (column_type.startswith(('Sparse[', 'complex')) or column_type in (
            'datetime64[s]', 'timedelta64[s]',
        )):
            continue
        if str(df[column].dtype) != column_type:
            df[column] = df[column].astype(column_type)
    return df


def stores_category_codes(dtype: Any) -> bool:
    return isinstance(dtype, pd.CategoricalDtype) and not (
        pd.api.types.is_string_dtype(dtype.categories.dtype)
        or pd.api.types.is_object_dtype(dtype.categories.dtype)
        and dtype.categories.inferred_type == 'string'
    )


def encode_categories(dtype: pd.CategoricalDtype) -> Dict:
    return dict(
        categories=_TAGGED_ENCODER.encode(dtype.categories.tolist()),
        categories_dtype=str(dtype.categories.dtype),
        ordered=bool(dtype.ordered),
    )


def decode_categories(encoded: Dict) -> pd.CategoricalDtype:
    categories = pd.Index(_TAGGED_DECODER.decode(encoded['categories']))
    try:
        categories = categories.astype(encoded['categories_dtype'])
    except (TypeError, ValueError):
        pass
    return pd.CategoricalDtype(categories, ordered=encoded['ordered'])


def restore_categories(df: pd.DataFrame, categories: Dict) -> pd.DataFrame:
    for column, encoded in (categories or {}).items():
        if column not in df.columns:
            continue
        codes = df[column].fillna(-1).astype('int64').to_numpy()
        df[column] = pd.Series(
            pd.Categorical.from_codes(codes, dtype=decode_categories(encoded)),
            index=df.index,
        )
    return df


def _dumps_tagged(value: Any) -> str:
    return _TAGGED_ENCODER.encode(value)


def _loads_tagged(text: str) -> Any:
    return _TAGGED_DECODER.decode(text)


def encode_column_labels(columns: pd.Index) -> Optional[Dict]:
    """
    Parquet needs string column names, so labels are written as str. Record the original
    labels when that changes them, so 0 and 1 from a NumPy array come back as 0 and 1.
    """
    multi = isinstance(columns, pd.MultiIndex)
    if not multi and columns.name is None and all(isinstance(c, str) for c in columns):
        return None
    labels = [list(c) for c in columns] if multi else columns.tolist()
    return dict(labels=_dumps_tagged(labels), names=list(columns.names), multi=multi)


def decode_column_labels(encoded: Dict, count: int) -> Optional[pd.Index]:
    labels = _loads_tagged(encoded['labels'])
    if len(labels) != count:
        # A sample file keeps only the first columns.
        labels = labels[:count]
        if len(labels) != count:
            return None
    if encoded.get('multi'):
        return pd.MultiIndex.from_tuples([tuple(label) for label in labels], names=encoded['names'])
    return pd.Index(labels, name=encoded['names'][0], tupleize_cols=False)


def pad_safe(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Make integer and boolean columns nullable before a shorter frame is padded with
    missing values, which would turn them into float or object columns. Integers above
    2**53 lose precision as float.
    """
    frame = frame.copy(deep=False)
    for column in frame.columns:
        dtype = frame[column].dtype
        if isinstance(dtype, np.dtype) and dtype.kind in 'iub':
            frame[column] = frame[column].astype(
                'boolean' if dtype.kind == 'b' else pd.api.types.pandas_dtype(
                    f"{'U' if dtype.kind == 'u' else ''}Int{dtype.itemsize * 8}",
                ),
            )
    return frame


def restore_padded_dtype(series: pd.Series, column_type: Optional[str]) -> pd.Series:
    """
    Cast a column cut back to its length to the NumPy integer or boolean dtype it had
    before pad_safe.
    """
    if not isinstance(column_type, str) or str(series.dtype) == column_type:
        return series
    try:
        dtype = np.dtype(column_type)
    except TypeError:
        return series
    if dtype.kind in 'iub' and not series.isna().any():
        return series.astype(dtype)
    return series


def serialize_object_json_value(value: Any) -> Any:
    # In a column of mixed values a float NaN is a value, so only None and the pandas
    # missing markers are NULL.
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    return _TAGGED_ENCODER.encode(value)


def serialize_string_value(value: Any) -> Any:
    return value if value is None else str(value)


def _is_missing(value: Any) -> bool:
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    return isinstance(value, float) and value != value


def _parser(cls: type, parse: Any) -> Any:
    """
    Parse stored text into cls. Values that already have the type pass through: outputs
    written before these columns were stored as text hold Decimal values and UUID bytes.
    """

    def parse_value(value: Any) -> Any:
        if isinstance(value, cls):
            return value
        if cls is uuid.UUID and isinstance(value, bytes):
            return uuid.UUID(bytes=value)
        return parse(value)

    return parse_value


# Object columns stored as text and parsed back, keyed by their recorded column type.
TEXT_COLUMN_TYPES = {
    DECIMAL_COLUMN_TYPE: (str, _parser(decimal.Decimal, decimal.Decimal)),
    # Floats with NaN: stored as text, so NaN and NULL stay apart.
    FLOAT_COLUMN_TYPE: (repr, _parser(float, float)),
    # Times with an offset: Parquet times have no time zone.
    TIMETZ_COLUMN_TYPE: (lambda value: value.isoformat(), _parser(time, time.fromisoformat)),
    # pyarrow stores UUID objects as 16 bytes and returns bytes.
    UUID_COLUMN_TYPE: (str, _parser(uuid.UUID, uuid.UUID)),
    # Arrow has no complex type.
    'complex64': (repr, _parser(complex, complex)),
    'complex128': (repr, _parser(complex, complex)),
}


def serialize_columns(df: pd.DataFrame, column_types: Dict) -> pd.DataFrame:
    """
    Replace dict and list columns with JSON strings, ObjectId columns with strings, and
    the columns in TEXT_COLUMN_TYPES with their text. Other columns are left untouched.
    Modifies and returns df.
    """
    for column, column_type in column_types.items():
        # Object columns keep None. Series.map would infer the str dtype, which stores NaN.
        if column_type in JSON_SERIALIZABLE_COLUMN_TYPES:
            df[column] = pd.Series(
                [serialize_json_value(v) for v in df[column].tolist()],
                index=df.index,
                dtype=object,
            )
        elif column_type == OBJECT_JSON_COLUMN_TYPE:
            df[column] = pd.Series(
                [serialize_object_json_value(v) for v in df[column].tolist()],
                index=df.index,
                dtype=object,
            )
        elif column_type in STRING_SERIALIZABLE_COLUMN_TYPES:
            df[column] = pd.Series(
                [serialize_string_value(v) for v in df[column].tolist()],
                index=df.index,
                dtype=object,
            )
        elif column_type in TEXT_COLUMN_TYPES:
            to_text = TEXT_COLUMN_TYPES[column_type][0]
            df[column] = pd.Series(
                [None if v is None else to_text(v) for v in df[column].tolist()],
                index=df.index,
                dtype=object,
            )
    return df


def cast_column_types(df: pd.DataFrame, column_types: Dict):
    for column, column_type in column_types.items():
        if column_type in CAST_TYPE_COLUMN_TYPES:
            try:
                if df is None or df.empty:
                    continue
                df[column] = df[column].astype(column_type)
            except Exception:
                traceback.print_exc()
    return df


def cast_column_types_polars(df: pl.DataFrame, column_types: Dict):
    for column, column_type in column_types.items():
        if column_type in POLARS_CAST_TYPE_COLUMN_TYPES:
            try:
                if df is None or df.is_empty():
                    continue
                df = df.cast({column: POLARS_CAST_TYPE_COLUMN_TYPES.get(column_type)})
            except Exception:
                traceback.print_exc()
    return df


def deserialize_json_value(value: Any) -> Any:
    if isinstance(value, str):
        return _TAGGED_DECODER.decode(value)
    return None if _is_missing(value) else value


def deserialize_list_value(value: Any) -> Any:
    if isinstance(value, str):
        return _TAGGED_DECODER.decode(value)
    if isinstance(value, np.ndarray):
        return list(value)
    return None if _is_missing(value) else value


def deserialize_columns(df: pd.DataFrame, column_types: Dict) -> pd.DataFrame:
    """
    Parse the JSON strings in dict and list columns, turn the numpy arrays pyarrow
    returns for list columns into lists, and parse the columns in TEXT_COLUMN_TYPES.
    Missing values come back as None. Columns missing from df, such as those cut from a
    sample file, are skipped. Modifies and returns df.
    """
    for column, column_type in column_types.items():
        if column not in df.columns:
            continue
        if column_type == dict.__name__:
            df[column] = pd.Series(
                [deserialize_json_value(v) for v in df[column].tolist()],
                index=df.index,
                dtype=object,
            )
        elif column_type == list.__name__:
            df[column] = pd.Series(
                [deserialize_list_value(v) for v in df[column].tolist()],
                index=df.index,
                dtype=object,
            )
        elif column_type == OBJECT_JSON_COLUMN_TYPE:
            df[column] = pd.Series(
                [deserialize_json_value(v) for v in df[column].tolist()],
                index=df.index,
                dtype=object,
            )
        elif column_type in TEXT_COLUMN_TYPES:
            parse = TEXT_COLUMN_TYPES[column_type][1]
            df[column] = pd.Series(
                [None if _is_missing(v) else parse(v) for v in df[column].tolist()],
                index=df.index,
                dtype=object,
            )
    return df


# def dask_from_pandas(df: pd.DataFrame) -> dd:
#     ddf = dd.from_pandas(df, npartitions=1)
#     npartitions = 1 + ddf.memory_usage(deep=True).sum().compute() // MAX_PARTITION_BYTE_SIZE
#     ddf = ddf.repartition(npartitions=npartitions)

#     return ddf


# def apply_transform(ddf: dd, apply_function) -> dd:
#     res = ddf.apply(apply_function, axis=1, meta=ddf)
#     return res.compute()


def should_serialize_pandas(column_types: Dict) -> bool:
    if not column_types:
        return False
    for _, column_type in column_types.items():
        if (
            column_type in JSON_SERIALIZABLE_COLUMN_TYPES
            or column_type in STRING_SERIALIZABLE_COLUMN_TYPES
            or column_type in TEXT_COLUMN_TYPES
            or column_type == OBJECT_JSON_COLUMN_TYPE
        ):
            return True
    return False


def should_deserialize_pandas(column_types: Dict) -> bool:
    if not column_types:
        return False
    for _, column_type in column_types.items():
        if (
            column_type in JSON_SERIALIZABLE_COLUMN_TYPES
            or column_type in TEXT_COLUMN_TYPES
            or column_type == OBJECT_JSON_COLUMN_TYPE
        ):
            return True
    return False


def is_yaml_serializable(key: str, value: Any) -> bool:
    try:
        s = yaml.dump({key: value})
        yaml.safe_load(s)
        return True
    except Exception:
        return False


def is_basic_iterable(data: Any) -> bool:
    return isinstance(data, (list, set, tuple))


def is_dataframe_or_series(data: Any) -> bool:
    return isinstance(data, (pd.DataFrame, pd.Series, pl.DataFrame, pl.Series))


def infer_variable_type(
    data: Any,
    repo_path: Optional[str] = None,
    variable_type: Optional[Any] = None,
) -> Tuple[Optional[Any], Optional[bool]]:
    from scipy.sparse import csr_matrix

    basic_iterable = is_basic_iterable(data)
    variable_type_use = variable_type

    if isinstance(data, (pl.DataFrame, pl.LazyFrame)) or (
        basic_iterable and len(data) >= 1 and all(isinstance(d, pl.DataFrame) for d in data)
    ):
        # Need to import here to mock in unit tests.
        from mage_ai.settings.server import MEMORY_MANAGER_POLARS_V2, MEMORY_MANAGER_V2

        if (MEMORY_MANAGER_V2 and MEMORY_MANAGER_POLARS_V2) or Project(
            repo_path=repo_path
        ).is_feature_enabled(FeatureUUID.POLARS):
            variable_type_use = VariableType.POLARS_DATAFRAME
        # If Polars is not enabled, we will fall back to the original logic in variable_manager
        # before this change:
        # if type(data) is pd.DataFrame:
        #     variable_type = VariableType.DATAFRAME
        # elif is_spark_dataframe(data):
        #     variable_type = VariableType.SPARK_DATAFRAME
        # elif is_geo_dataframe(data):
        #     variable_type = VariableType.GEO_DATAFRAME
        # variable = Variable(
        #     clean_name(variable_uuid),
        #     self.pipeline_path(pipeline_uuid),
        #     block_uuid,
        #     partition=partition,
        #     storage=self.storage,
        #     variable_type=variable_type,
        #     clean_block_uuid=clean_block_uuid,
        # )
    elif isinstance(data, pd.DataFrame):
        variable_type_use = VariableType.DATAFRAME
    elif is_spark_dataframe(data):
        variable_type_use = VariableType.SPARK_DATAFRAME
    elif is_geo_dataframe(data):
        variable_type_use = VariableType.GEO_DATAFRAME
    elif isinstance(data, csr_matrix) or (
        basic_iterable and len(data) >= 1 and all(isinstance(d, csr_matrix) for d in data)
    ):
        variable_type_use = VariableType.MATRIX_SPARSE
    elif isinstance(data, pd.Series) or (
        basic_iterable and len(data) >= 1 and all(isinstance(d, pd.Series) for d in data)
    ):
        variable_type_use = VariableType.SERIES_PANDAS
    elif isinstance(data, pl.Series) or (
        basic_iterable and len(data) >= 1 and all(isinstance(d, pl.Series) for d in data)
    ):
        variable_type_use = VariableType.SERIES_POLARS
    elif is_model_sklearn(data) or (
        basic_iterable and len(data) >= 1 and all(is_model_sklearn(d) for d in data)
    ):
        variable_type_use = VariableType.MODEL_SKLEARN
    elif is_model_xgboost(data) or (
        basic_iterable and len(data) >= 1 and all(is_model_xgboost(d) for d in data)
    ):
        variable_type_use = VariableType.MODEL_XGBOOST
    elif is_list_complex(data) or (
        basic_iterable
        and len(data) >= 1
        and len(data) <= 100  # If there are over 100 complex items in this list, we won’t handle.
        and all(is_list_complex(d) for d in data)
    ):
        variable_type_use = VariableType.LIST_COMPLEX
    elif is_dictionary_complex(data) or (
        basic_iterable and len(data) >= 1 and all(is_dictionary_complex(d) for d in data)
    ):
        variable_type_use = VariableType.DICTIONARY_COMPLEX
    elif is_custom_object(data) or (
        basic_iterable and len(data) >= 1 and all(is_custom_object(d) for d in data)
    ):
        variable_type_use = VariableType.CUSTOM_OBJECT
    elif basic_iterable:
        variable_type_use = VariableType.ITERABLE

    return variable_type_use, basic_iterable


def is_dictionary_complex(data: Any) -> bool:
    return isinstance(data, dict) and any(is_user_defined_complex(v) for v in data.values())


def is_list_complex(data: Any) -> bool:
    return isinstance(data, (list, set, tuple)) and any(is_user_defined_complex(v) for v in data)


def is_primitive(value: Any) -> bool:
    built_in_types = (int, float, str, bool)
    return isinstance(value, built_in_types)


def is_user_defined_complex(value: Any) -> bool:
    if is_primitive(value):
        return False

    # Consider only user-defined or less common complex types
    built_in_types = (list, dict, tuple, set, type(None))

    return not isinstance(value, built_in_types)


def serialize_complex(
    data: Any,
    column_types: Optional[Dict] = None,
    combine_values_and_column_types: Optional[bool] = False,
    path: Optional[List[str]] = None,
    save_path: Optional[str] = None,
) -> Tuple[Any, Dict]:
    if column_types is None:
        column_types = {}
    if path is None:
        path = []

    def update_column_types(
        value: Any,
        key_path: str,
        full_save_path: Optional[str] = None,
        serialized_value: Optional[Union[bool, int, float, str, List, Dict, DataFrame]] = None,
        variable_type: Optional[VariableType] = None,
        combine_values_and_column_types=combine_values_and_column_types,
    ):
        """
        Updates column_types dictionary based on the value's type and structure.
        Optionally includes the serialized value if combine_values_and_column_types is True.
        """
        type_info = None

        if variable_type in [
            VariableType.CUSTOM_OBJECT,
            VariableType.MODEL_SKLEARN,
            VariableType.MODEL_XGBOOST,
        ]:
            type_info = object_to_dict(value, variable_type=variable_type)
            if full_save_path:
                type_info['path'] = full_save_path
        else:
            type_info = object_to_dict(value, variable_type=variable_type)

        if combine_values_and_column_types:
            type_info['value'] = serialized_value

        column_types[key_path] = type_info

    def serialize(
        value: Any,
        current_path: Optional[List[str]] = None,
        combine_values_and_column_types=combine_values_and_column_types,
        save_path=save_path,
    ) -> Any:
        """Recursively serializes data while updating column types accordingly."""
        serialized_value = None
        current_path = current_path or []

        full_save_path = os.path.join(save_path, *current_path) if save_path else None
        key_path = '.'.join(current_path)
        variable_type, _ = infer_variable_type(value)

        if isinstance(value, dict):
            serialized_value = {k: serialize(v, current_path + [k]) for k, v in value.items()}
        elif is_basic_iterable(value):
            value_iter = list(value) if isinstance(value, set) else value
            serialized_value = [
                serialize(
                    v,
                    current_path + [str(i)],
                )
                for i, v in enumerate(value_iter)
            ]
        else:
            if variable_type in [
                VariableType.CUSTOM_OBJECT,
                VariableType.MODEL_SKLEARN,
                VariableType.MODEL_XGBOOST,
            ]:
                if full_save_path:
                    os.makedirs(os.path.dirname(full_save_path), exist_ok=True)
                    _, full_save_path = save_custom_object(value, full_save_path, variable_type)

            serialized_value, _ = prepare_data_for_output(value)

        if current_path:
            update_column_types(
                value,
                key_path,
                variable_type=variable_type,
                full_save_path=full_save_path,
                serialized_value=serialized_value,
            )

        return serialized_value

    # Start the serialization process from the root.
    serialized_data = serialize(data, path)

    return serialized_data, column_types


def deserialize_custom_complex_objects(
    value: Any,
    path: str,
    variable_type: VariableType,
):
    data = load_custom_object(os.path.dirname(path), variable_type)
    if data is not None:
        return data
    return value


def construct_value(type_info: Dict[str, Union[str, Optional[str]]], value: Any) -> Any:
    """
    Constructs a Python object from value based on type information.
    """
    type_name = type_info['name']

    if 'path' in type_info and 'variable_type' in type_info:
        return deserialize_custom_complex_objects(
            value,
            str(type_info['path']),
            VariableType(type_info['variable_type']),
        )

    if isinstance(value, pd.DataFrame):
        return pd.DataFrame(value)
    elif isinstance(value, pd.Series):
        return pd.Series(value)
    elif isinstance(value, pl.DataFrame):
        return pl.DataFrame(value)
    elif isinstance(value, pl.Series):
        return pl.Series(value)
    elif 'Timestamp' == type_name:
        return pd.Timestamp(value)
    elif 'datetime' == type_name:
        return datetime.fromisoformat(value)
    elif 'ndarray' == type_name:
        return np.array(value)
    elif 'DataFrame' == type_name and not isinstance(value, str):
        return pd.DataFrame(value)
    elif 'Series' == type_name and not isinstance(value, str):
        return pd.Series(value)
    elif 'tuple' == type_name:
        return tuple(value)
    elif 'set' == type_name:
        return set(value)
    elif 'int' == type_name:
        return int(value)
    elif 'float' == type_name:
        return float(value)
    elif 'str' == type_name:
        return str(value)
    elif 'bool' == type_name:
        return bool(value)
    elif not isinstance(value, str):
        # For simplicity, assuming direct values don't need complex deserialization
        module_name = type_info['module']
        class_name = type_info['name']
        module = importlib.import_module(str(module_name))
        class_ = getattr(module, str(class_name))
        return class_(value)

    return value


def deserialize_element(value: Any, path: str, column_types: Dict[str, Dict]):
    """
    Recursively deserializes a nested structure based on its column type definition.
    """
    if path in column_types:
        type_info = column_types[path]

        # Handle complex nested structures
        if type_info['name'] in ['list', 'tuple', 'set']:
            # Recursively construct elements of the list, tuple, or set
            constructed_elements = [
                deserialize_element(
                    v,
                    f'{path}.{i}',
                    column_types,
                )
                for i, v in enumerate(value)
            ]

            if type_info['name'] == 'tuple':
                return tuple(constructed_elements)
            elif type_info['name'] == 'set':
                return set(constructed_elements)

            # list
            return constructed_elements

        return construct_value(type_info, value)

    # Default case for values without specific type info (assumed to be simple types)
    return value


def unflatten_and_deserialize(flattened_data: Dict, column_types: Dict[str, Dict]) -> Dict:
    staging_data = {}

    for key, value in flattened_data.items():
        deserialized_value = deserialize_element(value, key, column_types)
        staging_data[key] = deserialized_value

    return unflatten_dict(staging_data)


def deserialize_complex(data: Any, column_types: Dict[str, Dict], unflatten: bool = False) -> Dict:
    """
    Deserialize serialized data (from JSON) back to its original structure and types.
    """
    if unflatten and isinstance(data, dict):
        return unflatten_and_deserialize(data, column_types)

    if isinstance(data, dict):
        return {key: deserialize_element(value, key, column_types) for key, value in data.items()}
    elif isinstance(data, list):
        # Assuming top-level list doesn't have a path, use an empty string as a placeholder path
        return [
            deserialize_element(
                item,
                str(index),
                column_types,
            )
            for index, item in enumerate(data)
        ]
    else:
        # Top-level simple types
        return deserialize_element(data, '', column_types)


def is_custom_object(obj: Any) -> bool:
    if not is_user_defined_complex(obj):
        return False

    for base_class in inspect.getmro(obj.__class__):
        if base_class.__module__ not in ('__builtin__', 'builtins'):
            return True
    return False


def prepare_data_for_output(
    data: Any,
    single_item_only: bool = False,
) -> Tuple[
    Union[DataFrame, Dict, str, List[Union[DataFrame, Dict, str]]],
    Optional[VariableType],
]:
    variable_type, basic_iterable = infer_variable_type(data)

    if single_item_only and basic_iterable and len(data) >= 1:
        data = data[0]

    if VariableType.SERIES_PANDAS == variable_type:
        if basic_iterable:
            data = DataFrame(data).T
        else:
            data = data.to_frame()
    elif VariableType.MATRIX_SPARSE == variable_type:
        if basic_iterable:
            data = convert_matrix_to_dataframe(data[0])
        else:
            data = convert_matrix_to_dataframe(data)
    elif VariableType.MODEL_SKLEARN == variable_type:
        if basic_iterable:
            data = [estimator_html_repr(d) for d in data]
        else:
            data = estimator_html_repr(data)
    elif VariableType.MODEL_XGBOOST == variable_type:
        if basic_iterable:
            data = [object_to_uuid(d) for d in data]
        else:
            data = object_to_uuid(data)
    elif VariableType.CUSTOM_OBJECT == variable_type:
        if basic_iterable:
            data = [object_to_uuid(d) for d in data]
        else:
            data = object_to_uuid(data)
    elif VariableType.DICTIONARY_COMPLEX == variable_type:
        if basic_iterable:
            data = [serialize_complex(d)[0] for d in data]
        else:
            data = serialize_complex(data)[0]
    elif VariableType.LIST_COMPLEX == variable_type:
        if basic_iterable:
            data = [serialize_complex(d)[0] for d in data]
        else:
            data = serialize_complex(data)[0]
    else:
        variable_type = None

    return data, variable_type


def warn_for_repo_path(repo_path: Optional[str]) -> None:
    """
    Warn if repo_path is not provided when using project platform and user
    authentication is enabled.
    """
    if repo_path is None and user_project_platform_activated():
        try:
            func_name = inspect.stack()[1][3]
            message = f'repo_path argument in {func_name} must be provided.'
        except Exception:
            message = 'repo_path argument must be provided.'
        warn(
            f'{message} Some functionalities may not work as expected',
            SyntaxWarning,
            stacklevel=2,
        )
