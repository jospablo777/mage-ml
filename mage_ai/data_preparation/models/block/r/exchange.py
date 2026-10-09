"""
The data R blocks receive and return.

Data frames cross as Arrow IPC files, which R reads and writes with the arrow package;
other values cross as JSON. A job directory holds:

- manifest.json: the block type and one entry per input, with its file and kind.
- input_<n>.arrow or input_<n>.json: the outputs of the upstream blocks.
- globals.json: the pipeline's variables.
- block.R: the block's code.
- output/: the block's return value, with its own manifest.json; error.txt and
  traceback.txt when the block fails.

R's arrow package reads most Arrow types exactly. Columns are cast before R reads them
where it would not, or where R has a better type:

- int64 becomes int32, R's integer, when every value fits, and bit64::integer64 in R
  otherwise. R's integers come back as int64.
- string_view and binary_view, which arrow cannot read, become large_string and
  large_binary.
- float16, which arrow reads as its raw bits, becomes float32.
- uint64 becomes int64 when every value fits, and text otherwise; arrow reads it as double.
- Columns holding structs or maps cross as JSON text, which R decodes into lists: arrow
  turns a NULL struct into a row of fields, with "" in text fields.
- Values pyarrow cannot type, such as a column of mixed types, cross as JSON text.
- UUIDs cross as text, and other extension types as the values they store.

In JSON, integers that a double cannot hold cross as {"$int64": "<digits>"}, which
mageml reads as bit64::integer64.

Decimals become doubles in R, with 15 to 17 significant digits, and -2**63, which bit64
uses as its NA, becomes NA. A warning names the columns where that happens.
"""
import json
import math
import os
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.feather as feather
import simplejson
import yaml

from mage_ai.shared.parsers import encode_complex

INT64_MIN = -(2**63)
# The largest integer a double holds exactly, and how larger ones cross as JSON.
DOUBLE_INTEGER_MAX = 2**53
INT64_MARKER = '$int64'
# R's integers are 32-bit, with -2**31 as NA.
R_INTEGER_NA = -(2**31)
R_INTEGER_MAX = 2**31 - 1
# Doubles hold 15 significant digits exactly.
DOUBLE_DIGITS = 15
# Marks a column of JSON text.
JSON_FIELD_METADATA = {b'mage.json': b'true'}
# Values R blocks cannot use.
SKIPPED_GLOBALS = frozenset(['logger'])

_PANDAS_TYPES = {
    pa.int8(): pd.Int8Dtype(),
    pa.int16(): pd.Int16Dtype(),
    pa.int32(): pd.Int32Dtype(),
    pa.int64(): pd.Int64Dtype(),
    pa.uint8(): pd.UInt8Dtype(),
    pa.uint16(): pd.UInt16Dtype(),
    pa.uint32(): pd.UInt32Dtype(),
    pa.uint64(): pd.UInt64Dtype(),
    pa.bool_(): pd.BooleanDtype(),
}


def _encode(value: Any) -> Any:
    encoded = encode_complex(value)
    # encode_complex returns what it cannot encode, such as a UUID, unchanged.
    return str(value) if encoded is value else encoded


def _mark_int64(value: Any) -> Any:
    """
    value with every integer that a double cannot hold as {"$int64": "<digits>"}, which
    mageml reads as bit64::integer64; jsonlite reads JSON numbers as doubles.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return {INT64_MARKER: str(value)} if abs(value) > DOUBLE_INTEGER_MAX else value
    if isinstance(value, dict):
        return {key: _mark_int64(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_mark_int64(item) for item in value]
    return value


def json_for_r(value: Any, default: Callable = None) -> str:
    """value as JSON text for R."""
    text = simplejson.dumps(value, default=default or _encode, ignore_nan=True)
    return simplejson.dumps(_mark_int64(json.loads(text)))


def _json_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json_for_r(value)


def _json_array(values: List[Any]) -> pa.Array:
    return pa.array([_json_text(v) for v in values], pa.large_string())


def _has_nested_records(data_type: pa.DataType) -> bool:
    if pa.types.is_struct(data_type) or pa.types.is_map(data_type):
        return True
    if pa.types.is_dictionary(data_type):
        return _has_nested_records(data_type.value_type)
    return any(
        _has_nested_records(data_type.field(i).type) for i in range(data_type.num_fields)
    )


def _r_type(data_type: pa.DataType) -> pa.DataType:
    """The type a column is cast to before R reads it."""
    if pa.types.is_string_view(data_type):
        return pa.large_string()
    if pa.types.is_binary_view(data_type):
        return pa.large_binary()
    if pa.types.is_float16(data_type):
        return pa.float32()
    if pa.types.is_list_view(data_type) or pa.types.is_large_list_view(data_type):
        return pa.large_list(_r_type(data_type.value_type))
    if pa.types.is_list(data_type):
        return pa.list_(_r_type(data_type.value_type))
    if pa.types.is_large_list(data_type):
        return pa.large_list(_r_type(data_type.value_type))
    if pa.types.is_fixed_size_list(data_type):
        return pa.list_(_r_type(data_type.value_type))
    return data_type


def _r_column(
    name: str, array: pa.ChunkedArray, warnings: List[str],
) -> Tuple[pa.ChunkedArray, bool]:
    """A column as R reads it exactly, and whether it is JSON text."""
    data_type = array.type
    if isinstance(data_type, pa.BaseExtensionType):
        if isinstance(data_type, pa.UuidType):
            values = [None if v is None else str(v) for v in array.to_pylist()]
            return pa.chunked_array([pa.array(values, pa.large_string())]), False
        # R reads the values an extension type stores.
        return _r_column(name, array.cast(data_type.storage_type), warnings)
    if _has_nested_records(data_type):
        return pa.chunked_array([_json_array(array.to_pylist())], pa.large_string()), True
    if pa.types.is_uint64(data_type):
        largest = pc.max(array).as_py()
        if largest is None or largest < 2**63:
            return _r_column(name, array.cast(pa.int64()), warnings)
        return array.cast(pa.large_string()), False
    if pa.types.is_decimal(data_type):
        if data_type.precision > DOUBLE_DIGITS:
            warnings.append(
                f'Column {name} is {data_type}, which R reads as double with 15 to 17 '
                'significant digits.',
            )
        return array, False
    if pa.types.is_int64(data_type) or pa.types.is_uint32(data_type):
        bounds = pc.min_max(array).as_py()
        smallest, largest = bounds['min'], bounds['max']
        if smallest is None or (smallest > R_INTEGER_NA and largest <= R_INTEGER_MAX):
            # R's integer; integer64 is for the values it cannot hold.
            return array.cast(pa.int32()), False
        if smallest == INT64_MIN:
            warnings.append(f'Column {name} holds -2**63, which R reads as NA.')
    r_type = _r_type(data_type)
    return (array.cast(r_type) if r_type != data_type else array), False


def _pandas_table(frame: pd.DataFrame) -> pa.Table:
    index = frame.index
    if any(name is not None for name in index.names):
        # A named index holds data, such as the keys of a group by.
        frame = frame.reset_index()
    fields = []
    arrays = []
    for position in range(frame.shape[1]):
        name = str(frame.columns[position])
        series = frame.iloc[:, position]
        try:
            array = pa.array(series, from_pandas=True)
            if not isinstance(array, pa.ChunkedArray):
                array = pa.chunked_array([array])
            arrays.append(array)
            fields.append(pa.field(name, array.type))
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
            values = series.astype(object).where(series.notna(), None).tolist()
            arrays.append(pa.chunked_array([_json_array(values)]))
            fields.append(pa.field(name, pa.large_string(), metadata=JSON_FIELD_METADATA))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def to_arrow_table(value: Any) -> Optional[pa.Table]:
    """value as an Arrow table, or None when it is not a data frame."""
    if isinstance(value, pl.LazyFrame):
        value = value.collect()
    if isinstance(value, pl.DataFrame):
        # The oldest level writes text as large_string; Polars 2 writes string_view.
        return value.to_arrow(compat_level=pl.CompatLevel.oldest())
    if isinstance(value, pl.Series):
        return to_arrow_table(value.to_frame())
    if isinstance(value, pd.Series):
        return _pandas_table(value.to_frame(name=value.name if value.name is not None else 'value'))
    if isinstance(value, pd.DataFrame):
        return _pandas_table(value)
    if isinstance(value, pa.Table):
        return value
    return None


def write_frame(table: pa.Table, path: str) -> Tuple[List[str], List[str]]:
    """
    Write table for R and return the columns that are JSON text and the warnings about
    values R reads approximately.
    """
    names = table.column_names
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError(
            f'R blocks need unique column names; {", ".join(duplicates)} appear more than once.',
        )
    json_columns = []
    warnings = []
    columns = []
    for field, column in zip(table.schema, table.columns):
        name = field.name
        if field.metadata == JSON_FIELD_METADATA:
            array, is_json = column, True
        else:
            array, is_json = _r_column(name, column, warnings)
        if is_json:
            json_columns.append(name)
        columns.append(array)
    result = pa.Table.from_arrays(columns, names=names)
    feather.write_feather(result, path, compression='uncompressed')
    return json_columns, warnings


def write_inputs(values: List[Any], job_dir: str) -> Tuple[List[Dict], List[str]]:
    """Write the upstream outputs into job_dir and return their manifest entries."""
    entries = []
    warnings = []
    for position, value in enumerate(values, start=1):
        table = to_arrow_table(value)
        if table is not None:
            name = f'input_{position}.arrow'
            json_columns, column_warnings = write_frame(table, os.path.join(job_dir, name))
            warnings.extend(f'Input {position}: {w}' for w in column_warnings)
            entries.append(dict(kind='frame', path=name, json_columns=json_columns))
        else:
            name = f'input_{position}.json'
            with open(os.path.join(job_dir, name), 'w', encoding='utf-8') as file:
                file.write(json_for_r(value))
            entries.append(dict(kind='json', path=name))
    return entries, warnings


def globals_for_r(global_vars: Optional[Dict]) -> Tuple[Dict, List[str]]:
    """The variables that JSON can hold, and the names of the others."""
    result = {}
    skipped = []
    for key, value in (global_vars or {}).items():
        if key in SKIPPED_GLOBALS:
            continue
        try:
            text = json_for_r(value, default=encode_complex)
        except (TypeError, ValueError, OverflowError):
            skipped.append(key)
            continue
        result[key] = json.loads(text)
    return result, skipped


def _decode_json_column(array: pa.ChunkedArray) -> pd.Series:
    return pd.Series(
        [None if v is None else json.loads(v) for v in array.to_pylist()],
        dtype=object,
    )


def _pandas_type(data_type: pa.DataType):
    if data_type in _PANDAS_TYPES:
        return _PANDAS_TYPES[data_type]
    if (
        pa.types.is_date32(data_type)
        or pa.types.is_nested(data_type)
        or pa.types.is_time(data_type)
    ):
        # to_pandas turned dates into datetime64 and lists into NumPy arrays.
        return pd.ArrowDtype(data_type)
    return None


def read_frame(path: str, json_columns: List[str]) -> pd.DataFrame:
    table = feather.read_table(path)
    # R's integers become Int64, as integers that came from pandas or Polars were.
    table = table.cast(pa.schema([
        field.with_type(pa.int64()) if pa.types.is_int32(field.type) else field
        for field in table.schema
    ], metadata=table.schema.metadata))
    plain = table.drop_columns(json_columns) if json_columns else table
    frame = plain.to_pandas(types_mapper=_pandas_type)
    for name in json_columns:
        frame[name] = _decode_json_column(table.column(name))
    return frame[table.column_names]


def read_output(job_dir: str) -> Tuple[bool, Any]:
    """Whether the block returned a value, and the value."""
    out = os.path.join(job_dir, 'output')
    manifest_path = os.path.join(out, 'manifest.json')
    if not os.path.exists(manifest_path):
        return False, None
    with open(manifest_path, encoding='utf-8') as file:
        manifest = json.load(file)
    kind = manifest.get('kind')
    if kind == 'frame':
        return True, read_frame(
            os.path.join(out, 'data.arrow'), manifest.get('json_columns') or [],
        )
    if kind == 'json':
        with open(os.path.join(out, 'data.json'), encoding='utf-8') as file:
            return True, json.load(file, parse_constant=lambda name: math.nan)
    return False, None


# The io_config.yaml settings that mageml's db_connect() uses.
DATABASE_CONFIG_PREFIXES = ('POSTGRES_', 'MYSQL_', 'DUCKDB_')
IO_CONFIG_FUNCTIONS = re.compile(r'\b(db_connect|io_config|read_sql|write_table)\s*\(')
STRING_LITERALS = re.compile(r'"((?:[^"\\]|\\.)*)"|\'((?:[^\'\\]|\\.)*)\'')


def database_settings(code: str, repo_path: Optional[str]) -> Dict[str, Dict]:
    """
    The database settings of the io_config.yaml profiles that code names, for
    mageml's db_connect(). The profiles are the string literals in code that name one, and
    'default'; other settings, such as cloud credentials, are left out.
    """
    if not repo_path or not IO_CONFIG_FUNCTIONS.search(code):
        return {}
    from jinja2 import Template

    from mage_ai.data_preparation.shared.utils import get_template_vars
    from mage_ai.io.config import ConfigFileLoader, ConfigKey

    path = next(
        (
            os.path.join(repo_path, name)
            for name in ('io_config.yaml', 'io_config.yml')
            if os.path.exists(os.path.join(repo_path, name))
        ),
        None,
    )
    if path is None:
        return {}
    with open(path, encoding='utf-8') as file:
        profiles = yaml.safe_load(Template(file.read()).render(**get_template_vars())) or {}
    literals = {a or b for a, b in STRING_LITERALS.findall(code)} | {'default'}
    keys = [key for key in ConfigKey if str(key).startswith(DATABASE_CONFIG_PREFIXES)]
    settings = {}
    for name in sorted(literals):
        profile = profiles.get(name)
        if name == 'version' or not isinstance(profile, dict):
            continue
        loader = ConfigFileLoader(config=profile)
        values = {str(key): loader.get(key) for key in keys if loader.contains(key)}
        settings[name] = {k: v for k, v in values.items() if v is not None}
    return settings
