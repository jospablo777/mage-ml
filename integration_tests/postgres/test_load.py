"""
Reading PostgreSQL types through Mage's Postgres client.

The reference values come from psycopg2 directly. exact_types and polars must return
every value unchanged; the default load is pinned to its documented conversions.
"""

import datetime
import decimal
import json
import math

import numpy as np
import pandas as pd
import polars as pl
import psycopg2
import pytest

from integration_tests.data import postgres_dataset

EXACT_PANDAS_DTYPES = {
    'id': 'Int64',
    'c_smallint': 'Int64',
    'c_integer': 'Int64',
    'c_bigint': 'Int64',
    'c_numeric': 'object',
    'c_numeric_free': 'object',
    # NaN and NULL both occur, and pandas merges them in nullable floats.
    'c_real': 'object',
    'c_double': 'Float64',
    'c_bool': 'boolean',
    'c_text': 'str',
    'c_varchar': 'str',
    'c_char': 'str',
    'c_bytea': 'object',
    'c_date': 'object',
    'c_time': 'object',
    'c_timetz': 'object',
    'c_timestamp': 'datetime64[us]',
    'c_timestamptz': 'datetime64[us, UTC]',
    'c_interval': 'timedelta64[us]',
    'c_uuid': 'str',
    'c_json': 'object',
    'c_jsonb': 'object',
    'c_int_array': 'object',
    'c_bigint_array': 'object',
    'c_float_array': 'object',
    'c_bool_array': 'object',
    'c_text_array': 'object',
    'c_numeric_array': 'object',
    'c_date_array': 'object',
    'c_uuid_array': 'object',
    'c_int_matrix': 'object',
    'c_enum': 'str',
}

POLARS_DTYPES = {
    'id': pl.Int32,
    'c_smallint': pl.Int16,
    'c_integer': pl.Int32,
    'c_bigint': pl.Int64,
    'c_numeric': pl.Decimal(38, 10),
    # Rows hold NaN and values wider than 38 digits, which Polars decimals cannot.
    'c_numeric_free': pl.String,
    'c_real': pl.Float32,
    'c_double': pl.Float64,
    'c_bool': pl.Boolean,
    'c_text': pl.String,
    'c_varchar': pl.String,
    'c_char': pl.String,
    'c_bytea': pl.Binary,
    'c_date': pl.Date,
    'c_time': pl.Time,
    'c_timetz': pl.String,
    'c_timestamp': pl.Datetime('us'),
    'c_timestamptz': pl.Datetime('us', 'UTC'),
    'c_interval': pl.Duration('us'),
    'c_uuid': pl.String,
    'c_json': pl.String,
    'c_jsonb': pl.String,
    'c_int_array': pl.List(pl.Int32),
    'c_bigint_array': pl.List(pl.Int64),
    'c_float_array': pl.List(pl.Float64),
    'c_bool_array': pl.List(pl.Boolean),
    'c_text_array': pl.List(pl.String),
    'c_numeric_array': pl.List(pl.String),
    'c_date_array': pl.List(pl.Date),
    'c_uuid_array': pl.List(pl.String),
    'c_int_matrix': pl.List(pl.List(pl.Int64)),
    'c_enum': pl.String,
}


def query(schema, table='src'):
    return f'SELECT * FROM {schema}.{table} ORDER BY id'


def same(expected, actual) -> bool:
    """
    Equality that keeps NULL, NaN and value types apart.
    """
    if expected is None:
        return actual is None or actual is pd.NA or actual is pd.NaT
    if actual is None or actual is pd.NA or actual is pd.NaT:
        return False
    if isinstance(expected, float) and math.isnan(expected):
        return isinstance(actual, (float, np.floating)) and math.isnan(actual)
    if isinstance(expected, list):
        if isinstance(actual, np.ndarray):
            actual = actual.tolist()
        return (
            isinstance(actual, list)
            and len(expected) == len(actual)
            and all(same(e, a) for e, a in zip(expected, actual))
        )
    if isinstance(expected, memoryview):
        expected = expected.tobytes()
    if isinstance(expected, decimal.Decimal):
        if not isinstance(actual, decimal.Decimal):
            return False
        if expected.is_nan():
            return actual.is_nan()
        return expected == actual
    if isinstance(expected, bool):
        return isinstance(actual, (bool, np.bool_)) and bool(actual) == expected
    if isinstance(expected, int):
        return (
            isinstance(actual, (int, np.integer))
            and not isinstance(actual, bool)
            and int(actual) == expected
        )
    if isinstance(expected, float):
        return isinstance(actual, (float, np.floating)) and float(actual) == expected
    if isinstance(expected, datetime.datetime):
        return (
            isinstance(actual, datetime.datetime)
            and pd.Timestamp(actual) == pd.Timestamp(expected)
            and ((actual.tzinfo is None) == (expected.tzinfo is None))
        )
    if isinstance(expected, datetime.timedelta):
        return (
            isinstance(actual, (datetime.timedelta, np.timedelta64))
            and pd.Timedelta(actual) == expected
        )
    return type(actual) is type(expected) and actual == expected


def as_float32(value):
    return None if value is None else float(np.float32(value))


def cell_differences(expected_rows, actual_rows, convert=None, nan_for_null=(), limit=10):
    """
    nan_for_null names columns where NaN is the frame's missing value, as in the pandas
    str dtype.
    """
    convert = dict(convert or {})
    # psycopg2 reads real as the shortest decimal text; compare the float32 values.
    convert.setdefault('c_real', as_float32)
    differences = []
    for expected, actual in zip(expected_rows, actual_rows):
        for column, value in expected.items():
            got = actual[column]
            value = convert[column](value) if column in convert else value
            if column == 'c_real' and isinstance(got, float):
                got = as_float32(got)
            if (
                value is None
                and column in nan_for_null
                and isinstance(got, float)
                and math.isnan(got)
            ):
                continue
            if not same(value, got):
                differences.append((column, expected['id'], value, got))
    return differences[:limit]


def test_default_load_converts_integers_with_nulls_and_numerics_to_float(
    mage_postgres,
    schema,
    source_table,
):
    """
    read_sql turns an integer column holding a NULL into float64, and numeric into float.
    bigint values above 2**53 lose precision. These are the documented costs of the
    default load; exact_types and polars avoid them.
    """
    frame = mage_postgres.load(query(schema), verbose=False)

    assert str(frame['c_smallint'].dtype) == 'float64'
    assert str(frame['c_bigint'].dtype) == 'float64'
    assert frame.loc[frame['id'] == 1, 'c_bigint'].item() == float(2**53)
    assert isinstance(frame.loc[frame['id'] == 1, 'c_numeric'].item(), float)
    assert str(frame['id'].dtype) == 'int64'


def test_exact_pandas_load_has_the_documented_dtypes(mage_postgres, schema, source_table):
    frame = mage_postgres.load(query(schema), verbose=False, exact_types=True)

    assert {column: str(dtype) for column, dtype in frame.dtypes.items()} == EXACT_PANDAS_DTYPES


def test_exact_pandas_load_returns_every_value(mage_postgres, pg, schema, source_table):
    expected = postgres_dataset.fetch_rows(pg, schema, source_table)
    frame = mage_postgres.load(query(schema), verbose=False, exact_types=True)

    string_columns = [c for c, t in EXACT_PANDAS_DTYPES.items() if t == 'str']
    assert len(frame) == len(expected)
    assert cell_differences(expected, frame.to_dict('records'), nan_for_null=string_columns) == []


def test_polars_load_has_the_documented_dtypes(mage_postgres, schema, source_table):
    frame = mage_postgres.load(query(schema), verbose=False, polars=True)

    assert isinstance(frame, pl.DataFrame)
    assert dict(frame.schema) == POLARS_DTYPES


def test_polars_load_returns_every_value(mage_postgres, pg, schema, source_table):
    expected = postgres_dataset.fetch_rows(pg, schema, source_table)
    frame = mage_postgres.load(query(schema), verbose=False, polars=True)

    convert = {
        # Polars carries JSON as text and timetz as ISO text; numeric beyond Polars
        # decimals as text.
        'c_timetz': lambda v: None if v is None else v.isoformat(),
        'c_numeric_free': lambda v: None if v is None else str(v),
        'c_numeric_array': lambda v: (
            None if v is None else [None if x is None else str(x) for x in v]
        ),
    }
    actual = frame.to_dicts()
    for row in actual:
        for column in postgres_dataset.JSON_COLUMNS:
            if row[column] is not None:
                row[column] = json.loads(row[column])
    assert len(actual) == len(expected)
    assert cell_differences(expected, actual, convert=convert) == []


@pytest.mark.parametrize('mode', ['default', 'exact_types', 'polars'])
def test_empty_result_keeps_the_columns(mage_postgres, schema, source_table, mode):
    options = {mode: True} if mode != 'default' else {}
    frame = mage_postgres.load(
        f'SELECT * FROM {schema}.src WHERE false',
        verbose=False,
        **options,
    )

    assert list(frame.columns) == postgres_dataset.COLUMN_NAMES
    assert len(frame) == 0
    if mode == 'polars':
        assert frame.schema['c_bigint'] == pl.Int64
    if mode == 'exact_types':
        assert str(frame['c_bigint'].dtype) == 'Int64'


@pytest.mark.parametrize('mode', ['exact_types', 'polars'])
def test_query_parameters_are_bound(mage_postgres, schema, source_table, mode):
    frame = mage_postgres.load(
        f'SELECT id FROM {schema}.src WHERE id = %(id)s',
        verbose=False,
        params={'id': 2},
        **{mode: True},
    )

    assert len(frame) == 1


@pytest.mark.parametrize('mode', ['default', 'exact_types', 'polars'])
def test_client_is_usable_after_a_failed_query(mage_postgres, schema, source_table, mode):
    options = {mode: True} if mode != 'default' else {}
    with pytest.raises(psycopg2.errors.UndefinedTable):
        mage_postgres.load('SELECT * FROM missing_table_for_this_test', verbose=False, **options)

    frame = mage_postgres.load(f'SELECT id FROM {schema}.src', verbose=False, **options)

    assert len(frame) > 0


def test_limit_is_applied(mage_postgres, schema, source_table):
    frame = mage_postgres.load(query(schema), verbose=False, exact_types=True, limit=7)

    assert len(frame) == 7
