"""
A push route that keeps feature values exact.

Feast's /push and /write-to-online-store build a pandas frame from the JSON body and
convert it without the feature view's schema. Measured with Feast 0.66 and the
PostgreSQL online store, they:

- round Int64 values above 2**53 when the same column holds a NULL in the request
  (2**60 + 1 is stored as 2**60); Feast's conversion does this even for nullable dtypes;
- truncate floats sent for Int64 features (1.5 becomes 1) and store numbers sent for
  String features;
- read integer timestamps as nanoseconds since the epoch;
- keep the last row of an entity within one request, not its latest event, and replace
  a newer stored value with an older event pushed later;
- write the online store before failing on an offline store that cannot take writes.

This route checks every value against the schema and answers 422 with each error. It
sorts rows by event time, can skip rows older than the stored ones, refuses offline
writes the offline store does not support before writing anything, and pushes rows in
groups where each Int64 column is complete or entirely NULL, the case Feast converts
exactly.
"""
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from fastapi import HTTPException
from feast.data_source import PushMode, PushSource
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.types import Bool, Float32, Float64, Int32, Int64, String
from pydantic import BaseModel

MODES = {
    'online': PushMode.ONLINE,
    'offline': PushMode.OFFLINE,
    'online_and_offline': PushMode.ONLINE_AND_OFFLINE,
}
INTEGERS = {Int32: (-(2**31), 2**31 - 1), Int64: (-(2**63), 2**63 - 1)}


class TypedPushRequest(BaseModel):
    push_source_name: str
    df: Dict[str, List[Any]]
    to: str = 'online'
    only_newer: bool = False
    assume_utc: bool = False


def _check(value: Any, dtype, assume_utc: bool) -> Tuple[Any, Optional[str]]:
    """The value for the dtype, or an error message."""
    if value is None:
        return None, None
    if dtype in INTEGERS:
        low, high = INTEGERS[dtype]
        if isinstance(value, bool) or not isinstance(value, int):
            return None, f'{value!r} is not an integer'
        if not low <= value <= high:
            return None, f'{value} is out of range for {dtype}'
        return value, None
    if dtype in (Float32, Float64):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None, f'{value!r} is not a number'
        return float(value), None
    if dtype == String:
        if not isinstance(value, str):
            return None, f'{value!r} is not a string'
        return value, None
    if dtype == Bool:
        if not isinstance(value, bool):
            return None, f'{value!r} is not a boolean'
        return value, None
    if dtype == 'timestamp':
        if not isinstance(value, str):
            return None, (
                f'{value!r} is not an ISO 8601 string; integers would be read as nanoseconds'
            )
        try:
            parsed = dt.datetime.fromisoformat(value)
        except ValueError:
            return None, f'{value!r} is not an ISO 8601 timestamp'
        if parsed.tzinfo is None:
            if not assume_utc:
                return None, f'{value!r} has no time zone; send an offset or set assume_utc'
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc), None
    return value, None


def _column(values: List[Any], dtype) -> pd.Series:
    if all(v is None for v in values):
        return pd.Series(values, dtype=object)
    if dtype in INTEGERS:
        return pd.Series(values, dtype='int64' if dtype == Int64 else 'int32')
    if dtype == Float32:
        return pd.Series([np.nan if v is None else v for v in values], dtype='float32')
    if dtype == Float64:
        return pd.Series([np.nan if v is None else v for v in values], dtype='float64')
    if dtype == 'timestamp':
        return pd.Series(pd.to_datetime(values, utc=True))
    return pd.Series(values, dtype=object)


def push_typed(store, request: TypedPushRequest) -> Dict[str, int]:
    if request.to not in MODES:
        raise HTTPException(422, f"to must be one of {sorted(MODES)}")
    views = [
        fv for fv in store.list_feature_views()
        if isinstance(fv.stream_source, PushSource)
        and fv.stream_source.name == request.push_source_name
    ]
    if not views:
        raise HTTPException(404, f'No feature view reads push source {request.push_source_name!r}')

    if request.to != 'online':
        offline_store = type(store._get_provider().offline_store)
        if offline_store.offline_write_batch is OfflineStore.offline_write_batch:
            raise HTTPException(
                501,
                f'{offline_store.__name__} does not support writes; nothing was written',
            )

    source = views[0].stream_source.batch_source
    schema: Dict[str, Any] = {source.timestamp_field: 'timestamp'}
    if source.created_timestamp_column:
        schema[source.created_timestamp_column] = 'timestamp'
    for view in views:
        for column in view.entity_columns:
            schema[column.name] = column.dtype
        for feature in view.features:
            schema[feature.name] = feature.dtype

    errors = []
    missing = [c for c in schema if c not in request.df]
    extra = [c for c in request.df if c not in schema]
    errors += [dict(row=None, column=c, message='missing') for c in missing]
    errors += [dict(row=None, column=c, message='not in the feature view') for c in extra]
    lengths = {len(v) for v in request.df.values()}
    if len(lengths) > 1:
        errors.append(dict(row=None, column=None, message='columns have different lengths'))
    if errors:
        raise HTTPException(422, dict(message='The rows do not match the schema', errors=errors))

    rows = []
    for index in range(lengths.pop() if lengths else 0):
        row = {}
        for column, dtype in schema.items():
            value, error = _check(request.df[column][index], dtype, request.assume_utc)
            if error:
                errors.append(dict(row=index, column=column, message=error))
            row[column] = value
        rows.append(row)
    if errors:
        raise HTTPException(
            422, dict(message=f'{len(errors)} values do not match the schema', errors=errors[:100]),
        )
    if not rows:
        return dict(rows=0, pushed=0, skipped_older=0, batches=0)

    timestamp = source.timestamp_field
    created = source.created_timestamp_column
    join_keys = [c.name for v in views for c in v.entity_columns]
    # The online store keeps the last write: order rows so an entity's latest event is
    # written last.
    rows.sort(key=lambda r: (r[timestamp], r[created] if created else r[timestamp]))

    skipped = 0
    if request.only_newer:
        stored = store.get_online_features(
            features=[f'{views[0].name}:{views[0].features[0].name}'],
            entity_rows=[{k: r[k] for k in join_keys} for r in rows],
        ).to_dict(include_event_timestamps=True)
        stamps = stored.get(f'{views[0].features[0].name}__ts') or []
        kept = []
        for row, stamp in zip(rows, stamps):
            if stamp is not None and _as_utc(stamp) > row[timestamp]:
                skipped += 1
            else:
                kept.append(row)
        rows = kept

    integer_columns = [c for c, t in schema.items() if t in INTEGERS]
    groups: Dict[Tuple[bool, ...], List[Dict]] = {}
    for row in rows:
        groups.setdefault(tuple(row[c] is None for c in integer_columns), []).append(row)
    for group in groups.values():
        frame = pd.DataFrame({
            column: _column([r[column] for r in group], dtype) for column, dtype in schema.items()
        })
        store.push(request.push_source_name, frame, to=MODES[request.to])
    return dict(rows=len(rows) + skipped, pushed=len(rows), skipped_older=skipped,
                batches=len(groups))


def _as_utc(value) -> dt.datetime:
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value, dt.timezone.utc)
    if getattr(value, 'tzinfo', None) is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)
