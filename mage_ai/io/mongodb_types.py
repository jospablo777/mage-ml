"""
Conversions between data frames and BSON for Mage's MongoDB client.
"""
import datetime as dt
import decimal
import json
import uuid
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import pyarrow as pa
from bson import Binary, Decimal128, ObjectId
from bson.int64 import Int64
from bson.regex import Regex
from bson.timestamp import Timestamp as BsonTimestamp


def to_bson(value: Any) -> Any:
    """
    A value BSON can store, for export. pymongo raised InvalidDocument for Decimal, date,
    timedelta, NumPy arrays and sets, and ValueError for UUID.

    Decimal becomes Decimal128, which holds 34 digits exactly. A date becomes midnight
    UTC, since BSON has no date type, and a timedelta its total seconds. BSON dates hold
    milliseconds: microseconds are dropped by MongoDB, as by every BSON client.
    """
    # pd.NaT passes isinstance checks for datetime, so missing values go first.
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and value != value:
        return None
    if isinstance(value, dict):
        return {str(k): to_bson(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        values = sorted(value, key=str) if isinstance(value, (set, frozenset)) else value
        return [to_bson(v) for v in values]
    if isinstance(value, np.ndarray):
        return [to_bson(v) for v in value.tolist()]
    if isinstance(value, np.generic):
        return to_bson(value.item())
    if isinstance(value, decimal.Decimal):
        return Decimal128(value)
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime(warn=False)
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day, tzinfo=dt.timezone.utc)
    if isinstance(value, (pd.Timedelta, dt.timedelta)):
        return value.total_seconds()
    if isinstance(value, dt.time):
        return value.isoformat()
    return value


def records_for_export(frame: pd.DataFrame) -> List[Dict]:
    """Rows of a frame as BSON-ready documents; missing values are None."""
    from mage_ai.io.export_utils import insert_rows

    columns = [str(c) for c in frame.columns]
    rows = []
    for column_index, column in enumerate(frame.columns):
        series = frame[column]
        if isinstance(series.dtype, pd.ArrowDtype) and pa.types.is_nested(
            series.dtype.pyarrow_dtype,
        ):
            rows.append(pa.array(series.array).to_pylist())
        else:
            rows.append([row[0] for row in insert_rows(frame.iloc[:, [column_index]])])
    return [
        {name: to_bson(value) for name, value in zip(columns, row)}
        for row in zip(*rows)
    ]


def from_bson(value: Any) -> Any:
    """A Python value for a BSON one, for loads with exact types."""
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, Decimal128):
        return value.to_decimal()
    if isinstance(value, Int64):
        return int(value)
    if isinstance(value, dt.datetime):
        # pymongo returns BSON dates as naive datetimes in UTC.
        return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Binary):
        return bytes(value)
    if isinstance(value, (Regex, BsonTimestamp)):
        return str(value)
    if isinstance(value, dict):
        return {k: from_bson(v) for k, v in value.items()}
    if isinstance(value, list):
        return [from_bson(v) for v in value]
    return value


def _json_default(value: Any) -> Any:
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def arrow_table_from_documents(documents: List[Dict]) -> pa.Table:
    """
    An Arrow table of documents, one column per field. A field missing from a document is
    null. Each column takes the type of its values: integers with nulls stay integers,
    Decimal128 is decimal, dates are UTC timestamps, and embedded documents are structs.
    A field whose values have different types is stored as JSON text.
    """
    names = list(dict.fromkeys(name for document in documents for name in document))
    columns = {}
    for name in names:
        values = [from_bson(document.get(name)) for document in documents]
        try:
            columns[name] = pa.array(values)
        except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
            columns[name] = pa.array([
                None if value is None else json.dumps(value, default=_json_default)
                for value in values
            ], type=pa.string())
    return pa.table(columns) if columns else pa.table({})
