"""
A client for Feast's feature server REST API, for blocks that read and write features.

Feast cannot run in MageML's environment (it requires pandas below 3), so blocks talk to a
Feast feature server over HTTP. The client sends what stock Feast stores exactly and reads
what it returns without losing values; docs/development/feast-integration.md has the
behaviors behind each rule:

- Large integers: Feast builds a pandas frame from a push, and an Int64 column with a NULL
  becomes float64, rounding values above 2**53. Pushes with such values go in requests
  where each of those columns is either complete or all NULL.
- Event order: the online store keeps the last write, not the latest event. Rows go in
  event-time order, in order of requests, so the latest event is written last.
- Timestamps: Feast reads integers as nanoseconds and naive strings as UTC. Datetimes go
  as ISO 8601 with their offset; integer and naive timestamps are refused unless
  assume_utc is set for naive ones.
- Errors: Feast answers most invalid requests with 500 and a plain string. Only
  connection errors, timeouts, 429, 502, 503 and 504 are retried; FeastError carries the
  route, status and body.
- Reads: values are mapped by the response's feature names, and integers stay exact.

    from mage_ai.feature_stores.feast import FeastClient

    feast = FeastClient()  # FEAST_URL, FEAST_TOKEN
    features = feast.get_online_features(
        entities={'driver_id': [1001, 1002]},
        features=['driver_hourly_stats:conv_rate', 'driver_hourly_stats:city'],
    )
    feast.push('driver_stats_push', frame)
"""
import datetime as dt
import math
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# Integers above this lose precision as float64.
EXACT_FLOAT_INTEGER = 2**53
RETRIED_STATUSES = frozenset((429, 502, 503, 504))
OUTPUTS = ('pandas', 'polars', 'arrow', 'dict')
PUSH_TARGETS = ('online', 'offline', 'online_and_offline')


class FeastError(Exception):
    """A request to the feature server failed, or a frame cannot be sent exactly."""

    def __init__(self, message: str, route: Optional[str] = None,
                 status: Optional[int] = None, body: Optional[str] = None):
        super().__init__(message)
        self.route = route
        self.status = status
        self.body = body


class FeastClient:
    def __init__(
        self,
        url: Optional[str] = None,
        token: Optional[str] = None,
        timeout: float = 30.0,
        retries: int = 3,
        backoff: float = 0.5,
        headers: Optional[Dict[str, str]] = None,
        verify: Any = True,
    ):
        """
        url: the feature server, such as http://feast:6566; FEAST_URL by default.
        token: sent as a bearer token; FEAST_TOKEN by default.
        retries: attempts after the first for connection errors, timeouts, 429, 502, 503
        and 504, waiting backoff * 2**attempt seconds or the server's Retry-After.
        verify: TLS verification, as in requests (a CA bundle path, or False).
        """
        import requests
        from requests.adapters import HTTPAdapter

        self.url = (url or os.getenv('FEAST_URL') or '').rstrip('/')
        if not self.url:
            raise FeastError('No feature server URL: pass url= or set FEAST_URL.')
        self.timeout = timeout
        self.retries = max(0, int(retries))
        self.backoff = backoff
        self.session = requests.Session()
        self.session.mount('http://', HTTPAdapter(pool_maxsize=16))
        self.session.mount('https://', HTTPAdapter(pool_maxsize=16))
        self.session.verify = verify
        token = token if token is not None else os.getenv('FEAST_TOKEN')
        if token:
            self.session.headers['Authorization'] = f'Bearer {token}'
        self.session.headers.update(headers or {})

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> 'FeastClient':
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # Requests

    def _request(self, method: str, route: str, body: Optional[Dict] = None,
                 timeout: Optional[float] = None):
        import requests

        url = f'{self.url}{route}'
        for attempt in range(self.retries + 1):
            last = attempt == self.retries
            try:
                response = self.session.request(
                    method, url, json=body, timeout=timeout or self.timeout,
                )
            except (requests.ConnectionError, requests.Timeout) as error:
                if last:
                    raise FeastError(
                        f'{route}: the feature server at {self.url} did not answer after '
                        f'{attempt + 1} attempts: {error}', route=route,
                    ) from error
                time.sleep(self.backoff * 2**attempt)
                continue
            if response.status_code in RETRIED_STATUSES and not last:
                time.sleep(_retry_after(response) or self.backoff * 2**attempt)
                continue
            if response.status_code >= 400:
                body_text = response.text[:4000]
                raise FeastError(
                    f'{route} failed with {response.status_code}: {body_text or "(empty body)"}',
                    route=route, status=response.status_code, body=body_text,
                )
            return response
        raise AssertionError('unreachable')

    def health(self) -> bool:
        """Whether the feature server answers /health."""
        try:
            self._request('GET', '/health')
            return True
        except FeastError:
            return False

    # Reading

    def get_online_features(
        self,
        entities: Any,
        features: Optional[Sequence[str]] = None,
        feature_service: Optional[str] = None,
        full_feature_names: bool = False,
        output: str = 'pandas',
        with_metadata: bool = False,
    ):
        """
        The online features of the entities, one row per entity row, in request order.

        entities: {'driver_id': [1001, 1002]}, or a frame whose columns are the entity keys.
        features: 'view:feature' references; or feature_service, a feature service's name.
        output: 'pandas' (Arrow-backed columns, integers exact with NULLs), 'polars',
        'arrow' or 'dict' ({name: [values]}).
        with_metadata: adds <name>__status (PRESENT, NULL_VALUE, NOT_FOUND, OUTSIDE_MAX_AGE)
        and <name>__event_timestamp for each feature. A feature that is NOT_FOUND is NULL;
        Feast also reports an entity whose features are all NULL as NOT_FOUND.
        """
        if output not in OUTPUTS:
            raise FeastError(f'output must be one of {OUTPUTS}, not {output!r}.')
        if (features is None) == (feature_service is None):
            raise FeastError('Pass either features or feature_service.')
        columns = _columns(entities)
        if not columns:
            raise FeastError('entities has no keys.')
        body: Dict[str, Any] = dict(
            entities={name: [_json_value(v, name) for v in values]
                      for name, values in columns.items()},
            full_feature_names=full_feature_names,
        )
        if features is not None:
            body['features'] = list(features)
        else:
            body['feature_service'] = feature_service
        payload = self._request('POST', '/get-online-features', body).json()
        names = payload.get('metadata', {}).get('feature_names') or []
        results = payload.get('results') or []
        if len(names) != len(results):
            raise FeastError(
                f'/get-online-features returned {len(results)} results for {len(names)} names.',
                route='/get-online-features',
            )
        data: Dict[str, List] = {}
        # Results follow metadata.feature_names, not the order of the request.
        for name, result in zip(names, results):
            statuses = result.get('statuses') or []
            values = result.get('values') or []
            data[name] = [
                None if index < len(statuses) and statuses[index] == 'NOT_FOUND' else value
                for index, value in enumerate(values)
            ]
            if with_metadata and name not in columns:
                data[f'{name}__status'] = statuses
                data[f'{name}__event_timestamp'] = [
                    _parse_timestamp(value) for value in result.get('event_timestamps') or []
                ]
        return _frame(data, output)

    # Writing

    def push(
        self,
        push_source_name: str,
        frame: Any,
        to: str = 'online',
        timestamp_column: str = 'event_timestamp',
        assume_utc: bool = False,
        batch_size: int = 1000,
        allow_registry_cache: bool = True,
    ) -> int:
        """
        Pushes the rows of a frame (pandas, Polars or Arrow) to a push source; returns the
        number of requests. Rows go in event-time order, so the online store ends with each
        entity's latest event, and large integers next to NULLs stay exact.

        to: 'online', 'offline' or 'online_and_offline'. The PostgreSQL offline store does
        not take pushes, and with 'online_and_offline' Feast writes the online store before
        failing; write offline rows to the offline store's table and call materialize.
        """
        if to not in PUSH_TARGETS:
            raise FeastError(f'to must be one of {PUSH_TARGETS}, not {to!r}.')
        requests_sent = 0
        for rows in _requests_for(frame, timestamp_column, assume_utc, batch_size):
            self._request('POST', '/push', dict(
                push_source_name=push_source_name,
                df=rows,
                to=to,
                allow_registry_cache=allow_registry_cache,
            ))
            requests_sent += 1
        return requests_sent

    def write_to_online_store(
        self,
        feature_view_name: str,
        frame: Any,
        timestamp_column: str = 'event_timestamp',
        assume_utc: bool = False,
        batch_size: int = 1000,
        allow_registry_cache: bool = True,
    ) -> int:
        """Writes the rows of a frame to a feature view's online store, as push does;
        returns the number of requests."""
        requests_sent = 0
        for rows in _requests_for(frame, timestamp_column, assume_utc, batch_size):
            self._request('POST', '/write-to-online-store', dict(
                feature_view_name=feature_view_name,
                df=rows,
                allow_registry_cache=allow_registry_cache,
            ))
            requests_sent += 1
        return requests_sent

    def materialize(
        self,
        start: dt.datetime,
        end: dt.datetime,
        feature_views: Optional[Sequence[str]] = None,
        timeout: float = 600.0,
    ) -> None:
        """Loads the offline store's rows between start and end into the online store."""
        body: Dict[str, Any] = dict(
            start_ts=_iso(start, 'start', assume_utc=False),
            end_ts=_iso(end, 'end', assume_utc=False),
        )
        if feature_views:
            body['feature_views'] = list(feature_views)
        self._request('POST', '/materialize', body, timeout=timeout)

    def materialize_incremental(
        self,
        end: dt.datetime,
        feature_views: Optional[Sequence[str]] = None,
        timeout: float = 600.0,
    ) -> None:
        """Loads the offline store's rows since the last materialization up to end."""
        body: Dict[str, Any] = dict(end_ts=_iso(end, 'end', assume_utc=False))
        if feature_views:
            body['feature_views'] = list(feature_views)
        self._request('POST', '/materialize-incremental', body, timeout=timeout)


# Frames to JSON

def _retry_after(response) -> Optional[float]:
    value = response.headers.get('Retry-After')
    try:
        return min(float(value), 60.0) if value else None
    except ValueError:
        return None


def _arrow_table(frame: Any):
    import pyarrow as pa

    if isinstance(frame, pa.Table):
        return frame
    if isinstance(frame, dict):
        return pa.table(frame)
    module = type(frame).__module__.split('.')[0]
    if module == 'polars':
        return frame.to_arrow()
    if module == 'pandas':
        return pa.Table.from_pandas(frame, preserve_index=False)
    raise FeastError(f'Cannot send a {type(frame).__name__}: pass a pandas, Polars or Arrow '
                     'frame, or a dict of columns.')


def _columns(entities: Any) -> Dict[str, List]:
    if isinstance(entities, dict):
        return {name: list(values) for name, values in entities.items()}
    table = _arrow_table(entities)
    return {name: table.column(name).to_pylist() for name in table.column_names}


def _iso(value: Any, column: str, assume_utc: bool) -> str:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            if not assume_utc:
                raise FeastError(
                    f'{column} has a timestamp without a time zone ({value}). Feast reads it '
                    'as UTC; make the column time zone aware, or pass assume_utc=True.',
                )
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    raise FeastError(f'{column}: expected a datetime, got {type(value).__name__} ({value!r}).')


def _json_value(value: Any, column: str, assume_utc: bool = False) -> Any:
    import decimal

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if math.isinf(value):
            raise FeastError(f'{column} holds {value}, which JSON cannot carry.')
        return value
    if isinstance(value, (dt.datetime, dt.date)):
        return _iso(value, column, assume_utc)
    if isinstance(value, decimal.Decimal):
        raise FeastError(
            f'{column} holds decimals; Feast has no decimal type. Cast the column to float '
            'or string before pushing.',
        )
    if isinstance(value, bytes):
        raise FeastError(f'{column} holds bytes; Feast reads JSON pushes as text.')
    if isinstance(value, list):
        return [_json_value(v, column, assume_utc) for v in value]
    if isinstance(value, dt.timedelta):
        raise FeastError(f'{column} holds durations, which Feast has no type for.')
    return value


def _requests_for(
    frame: Any, timestamp_column: str, assume_utc: bool, batch_size: int,
) -> Iterable[Dict[str, List]]:
    """The request bodies' df for a push: rows in event-time order, split where a large
    integer column changes between holding NULLs and not, and at batch_size rows."""
    import pyarrow as pa
    import pyarrow.compute as pc

    table = _arrow_table(frame)
    if table.num_rows == 0:
        return
    if timestamp_column not in table.column_names:
        raise FeastError(
            f'The frame has no {timestamp_column!r} column; pass timestamp_column= with the '
            "column the push source's timestamp_field names.",
        )
    timestamps = table.column(timestamp_column)
    if pa.types.is_integer(timestamps.type):
        raise FeastError(
            f'{timestamp_column} holds integers; Feast reads them as nanoseconds, so epoch '
            'seconds land in 1970. Convert the column to datetimes with a time zone.',
        )
    if pa.types.is_timestamp(timestamps.type) and timestamps.type.tz is None and not assume_utc:
        raise FeastError(
            f'{timestamp_column} has no time zone. Feast reads it as UTC; make the column '
            'time zone aware, or pass assume_utc=True.',
        )
    if timestamps.null_count:
        raise FeastError(f'{timestamp_column} has {timestamps.null_count} empty values.')
    # Stable: rows with the same event time keep their order, so the last one wins.
    order = pc.sort_indices(table, sort_keys=[(timestamp_column, 'ascending')])
    table = table.take(order)

    risky = [
        name for name in table.column_names
        if pa.types.is_integer(table.column(name).type)
        and table.column(name).null_count
        and _max_abs(table.column(name)) > EXACT_FLOAT_INTEGER
    ]
    columns = {name: table.column(name).to_pylist() for name in table.column_names}
    names = list(columns)
    batch_size = max(1, int(batch_size))

    def pattern(index: int) -> Tuple[bool, ...]:
        return tuple(columns[name][index] is None for name in risky)

    start = 0
    while start < table.num_rows:
        end = start + 1
        current = pattern(start)
        # A run of rows with the same NULL pattern in the large integer columns: each of
        # them is complete or all NULL within the request, which Feast keeps exact.
        while end < table.num_rows and end - start < batch_size and pattern(end) == current:
            end += 1
        yield {
            name: [_json_value(columns[name][i], name, assume_utc) for i in range(start, end)]
            for name in names
        }
        start = end


def _max_abs(column) -> int:
    import pyarrow.compute as pc

    bounds = pc.min_max(column).as_py()
    values = [abs(v) for v in (bounds.get('min'), bounds.get('max')) if v is not None]
    return max(values) if values else 0


# JSON to frames

def _parse_timestamp(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return value


def _frame(data: Dict[str, List], output: str):
    if output == 'dict':
        return data
    import pyarrow as pa

    # Arrow keeps integer columns with NULLs as int64; pandas would make them float64.
    table = pa.table({name: _array(name, values) for name, values in data.items()})
    if output == 'arrow':
        return table
    if output == 'polars':
        import polars as pl

        return pl.from_arrow(table)
    import pandas as pd

    return table.to_pandas(types_mapper=pd.ArrowDtype)


def _array(name: str, values: List):
    import pyarrow as pa

    try:
        return pa.array(values)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        # Mixed types in one feature (rare, from a schema change): keep them as text.
        return pa.array([None if v is None else str(v) for v in values], pa.string())
