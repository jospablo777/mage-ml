"""
Amplitude for blocks and for the Amplitude source of data integration pipelines.

Events come from the Export API (https://amplitude.com/docs/apis/analytics/export): a ZIP
of gzipped JSON lines per range of hours, selected by server_upload_time, available about
two hours after Amplitude receives them. Metrics come from the Dashboard REST API, the
event and property definitions from the Taxonomy API, and cohorts from the Behavioral
Cohorts API. Every request uses the project's API key and secret key.

    from mage_ai.io.amplitude import Amplitude

    amplitude = Amplitude.with_config(ConfigFileLoader(config_path, 'default'))
    events = amplitude.load_events('2024-06-01', '2024-06-02')
    users = amplitude.active_users('2024-06-01', '2024-06-30')

What the old source did and this client does not: a 403, a 429 or a server error was
logged and the run ended with no rows; every event of the range was held in memory, the
files joined by string concatenation; dates mixed local time and UTC.
"""
import csv
import datetime as dt
import gzip
import io
import json
import os
import tempfile
import time
import zipfile
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import pandas as pd
import requests

from mage_ai.io.base import BaseIO
from mage_ai.io.config import BaseConfigLoader, ConfigKey

HOSTS = {
    'US': 'https://amplitude.com',
    'EU': 'https://analytics.eu.amplitude.com',
}
EXPORT_HOUR_FORMAT = '%Y%m%dT%H'
DAY_FORMAT = '%Y%m%d'
# Amplitude: data can be exported about two hours after the server received it.
AVAILABILITY_DELAY_HOURS = 2
# Hours per Export API request. A request over 4 GB fails with 400: the window is halved
# down to one hour.
WINDOW_HOURS = 24
BATCH_SIZE = 10_000
# 429, and server errors other than 504 (a timeout, which a smaller window avoids).
RETRIED_STATUSES = (429, 500, 502, 503)
MAX_RETRIES = 6
MAX_BACKOFF_SECONDS = 120
COHORT_POLL_SECONDS = 5
COHORT_TIMEOUT_SECONDS = 3600
# Event fields that hold times, as 'YYYY-MM-DD HH:MM:SS.ffffff' in UTC.
TIME_FIELDS = (
    'client_event_time', 'client_upload_time', 'event_time', 'processed_time',
    'server_received_time', 'server_upload_time', 'user_creation_time',
)


class AmplitudeError(Exception):
    """A request Amplitude refused, with what to do about it."""


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def to_utc(value: Union[str, dt.date, dt.datetime, None]) -> Optional[dt.datetime]:
    """A UTC datetime; naive values and dates are taken as UTC."""
    if value is None or value == '':
        return None
    if isinstance(value, str):
        import dateutil.parser

        value = dateutil.parser.parse(value)
    if not isinstance(value, dt.datetime):
        value = dt.datetime(value.year, value.month, value.day)
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def floor_hour(value: dt.datetime) -> dt.datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def last_complete_hour(
    now: dt.datetime = None, delay_hours: int = AVAILABILITY_DELAY_HOURS,
) -> dt.datetime:
    """The start of the newest hour whose events can all be exported."""
    now = to_utc(now) or utc_now()
    return floor_hour(now) - dt.timedelta(hours=1 + delay_hours)


def hour_windows(
    start: dt.datetime, end: dt.datetime, hours: int,
) -> List[Tuple[dt.datetime, dt.datetime]]:
    """Ranges of whole hours from start to end, both included, each at most `hours` long."""
    windows = []
    first = floor_hour(start)
    end = floor_hour(end)
    while first <= end:
        last = min(first + dt.timedelta(hours=hours - 1), end)
        windows.append((first, last))
        first = last + dt.timedelta(hours=1)
    return windows


class Amplitude(BaseIO):
    def __init__(
        self,
        api_key: str,
        secret_key: str,
        region: str = 'US',
        host: str = None,
        timeout: int = 600,
        max_retries: int = MAX_RETRIES,
        verbose: bool = True,
        session: requests.Session = None,
        sleep: Callable[[float], None] = time.sleep,
        log: Callable[[str], None] = None,
    ) -> None:
        """
        Args:
            api_key (str): The project's API key (Settings > Projects > General).
            secret_key (str): The project's secret key.
            region (str): 'US' (amplitude.com) or 'EU' (analytics.eu.amplitude.com), where
                the project's data is stored. A key of one region is refused by the other.
            host (str): Another base URL, such as a proxy; overrides region.
            log: Where progress messages go; print when verbose. The integration source
                passes its logger: its stdout carries the records.
        """
        super().__init__(verbose=verbose)
        if not api_key or not secret_key:
            raise AmplitudeError(
                'Amplitude needs the project API key and secret key: Settings > Projects > '
                'your project > General.'
            )
        region = (region or 'US').strip().upper()
        if not host and region not in HOSTS:
            raise AmplitudeError(f"region must be 'US' or 'EU', not {region!r}.")
        self.api_key = api_key
        self.secret_key = secret_key
        self.region = region
        self.host = (host or HOSTS[region]).rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.sleep = sleep
        self.log = log or (print if verbose else (lambda message: None))

    @classmethod
    def with_config(cls, config: BaseConfigLoader, **kwargs) -> 'Amplitude':
        """
        From an io_config profile: AMPLITUDE_API_KEY, AMPLITUDE_SECRET_KEY,
        AMPLITUDE_REGION (US or EU) and, for a proxy or `mage examples amplitude-server`,
        AMPLITUDE_HOST.
        """
        options = dict(
            api_key=config[ConfigKey.AMPLITUDE_API_KEY],
            secret_key=config[ConfigKey.AMPLITUDE_SECRET_KEY],
            region=config[ConfigKey.AMPLITUDE_REGION] or 'US',
            host=config[ConfigKey.AMPLITUDE_HOST] or None,
        )
        options.update(kwargs)
        return cls(**options)

    # ---- HTTP ---------------------------------------------------------------------------

    def _request(
        self, path: str, params: Dict = None, stream: bool = False, allow: Tuple = (),
    ) -> requests.Response:
        """
        A GET with the project's credentials. 429 and server errors are retried with
        exponential backoff, waiting Retry-After when Amplitude sends it; statuses in
        `allow` are returned to the caller; others raise AmplitudeError.
        """
        url = path if path.startswith('http') else f'{self.host}{path}'
        attempt = 0
        while True:
            try:
                response = self.session.get(
                    url,
                    params=params,
                    auth=(self.api_key, self.secret_key),
                    stream=stream,
                    timeout=self.timeout,
                )
            except (requests.ConnectionError, requests.Timeout) as error:
                if attempt >= self.max_retries:
                    raise AmplitudeError(f'Amplitude did not answer {path}: {error}') from error
                self._backoff(attempt, None, path, str(error))
                attempt += 1
                continue
            status = response.status_code
            if status < 300 or status in allow:
                return response
            if status in RETRIED_STATUSES and attempt < self.max_retries:
                self._backoff(attempt, response.headers.get('Retry-After'), path, status)
                attempt += 1
                response.close()
                continue
            raise AmplitudeError(self._explain(path, response))

    def _backoff(self, attempt: int, retry_after: Optional[str], path: str, reason) -> None:
        delay = min(2 ** attempt * 2, MAX_BACKOFF_SECONDS)
        if retry_after:
            try:
                delay = max(float(retry_after), 1.0)
            except ValueError:
                pass
        self.log(f'Amplitude {path}: {reason}; trying again in {delay:.0f} s.')
        self.sleep(delay)

    def _explain(self, path: str, response: requests.Response) -> str:
        status = response.status_code
        try:
            body = response.text[:500].strip()
        except Exception:
            body = ''
        if status in (401, 403):
            other = 'EU' if self.region == 'US' else 'US'
            return (
                f'Amplitude refused the API key and secret key ({status}) for {path}. '
                f'Check both keys, and the region: this project is read from {self.region} '
                f'({self.host}); a project stored in {other} needs region {other}. {body}'
            )
        if status == 429:
            return (
                f'Amplitude kept refusing {path} for its rate limits (429) after '
                f'{self.max_retries} tries. The Export API allows 360 requests an hour and '
                f'4 at a time per project. {body}'
            )
        return f'Amplitude answered {status} for {path}: {body}'

    def _json(self, path: str, params: Dict = None) -> Any:
        return self._request(path, params=params).json()

    def test_connection(self) -> None:
        """Checks the keys and region with the cheapest call (the event categories)."""
        self._request('/api/2/taxonomy/category')

    # ---- Events (Export API) ------------------------------------------------------------

    def _download(self, start: dt.datetime, end: dt.datetime) -> Optional[str]:
        """
        The export of the hours from start to end, both included, as a temporary file;
        None when they hold no events. Raises _TooLarge for a range Amplitude cannot send
        in one request.
        """
        params = dict(
            start=start.strftime(EXPORT_HOUR_FORMAT), end=end.strftime(EXPORT_HOUR_FORMAT),
        )
        response = self._request(
            '/api/2/export', params=params, stream=True, allow=(400, 404, 504),
        )
        status = response.status_code
        if status == 404:
            response.close()
            return None
        if status in (400, 504):
            text = response.text[:500]
            response.close()
            if status == 504 or 'size' in text.lower() or 'exceed' in text.lower():
                raise _TooLarge(text)
            raise AmplitudeError(f'Amplitude refused the export of {params}: {text}')
        expected = response.headers.get('Content-Length')
        handle, path = tempfile.mkstemp(prefix='amplitude_', suffix='.zip')
        written = 0
        try:
            with os.fdopen(handle, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    file.write(chunk)
                    written += len(chunk)
        except (requests.exceptions.ChunkedEncodingError, requests.ConnectionError) as error:
            os.unlink(path)
            raise _Truncated(f'{written} bytes, then {error}') from error
        except BaseException:
            os.unlink(path)
            raise
        finally:
            response.close()
        if expected and int(expected) != written:
            os.unlink(path)
            raise _Truncated(f'{written} of {expected} bytes')
        return path

    def _download_checked(self, start: dt.datetime, end: dt.datetime) -> Optional[str]:
        for attempt in range(self.max_retries + 1):
            try:
                return self._download(start, end)
            except _Truncated as error:
                if attempt >= self.max_retries:
                    raise AmplitudeError(
                        f'The Amplitude export of {start} to {end} kept ending early: {error}',
                    )
                self._backoff(attempt, None, '/api/2/export', f'download cut short ({error})')
        return None

    def export_windows(
        self,
        start: Union[str, dt.datetime],
        end: Union[str, dt.datetime] = None,
        window_hours: int = WINDOW_HOURS,
        batch_size: int = BATCH_SIZE,
        delay_hours: int = AVAILABILITY_DELAY_HOURS,
    ) -> Iterator[Tuple[dt.datetime, dt.datetime, Iterator[List[Dict]]]]:
        """
        For each window of hours from start to end (both included, by server_upload_time):
        (first hour, last hour, batches of its events). The end is at most the newest hour
        Amplitude has finished (`delay_hours` after it ends); a window that is too large
        for one request is split. A window is done when its batches are consumed.
        """
        start = floor_hour(to_utc(start))
        newest = last_complete_hour(delay_hours=delay_hours)
        end = min(floor_hour(to_utc(end)), newest) if end else newest
        pending = hour_windows(start, end, window_hours)
        while pending:
            first, last = pending.pop(0)
            try:
                path = self._download_checked(first, last)
            except _TooLarge as error:
                hours = int((last - first).total_seconds() // 3600) + 1
                if hours == 1:
                    raise AmplitudeError(
                        f'The events of the hour {first:%Y-%m-%d %H}:00 UTC are more than the '
                        'Export API sends in one request (4 GB). Export that project to Amazon '
                        f'S3 or a warehouse with an Amplitude destination instead. ({error})',
                    ) from error
                half = first + dt.timedelta(hours=hours // 2 - 1)
                self.log(f'Amplitude export of {first} to {last} is too large; splitting it.')
                pending[:0] = [(first, half), (half + dt.timedelta(hours=1), last)]
                continue
            yield first, last, self._batches(path, batch_size)

    def _batches(self, path: Optional[str], batch_size: int) -> Iterator[List[Dict]]:
        if path is None:
            return
        try:
            batch = []
            for event in read_export(path):
                batch.append(event)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch
        finally:
            os.unlink(path)

    def iter_events(
        self,
        start: Union[str, dt.datetime],
        end: Union[str, dt.datetime] = None,
        event_types: List[str] = None,
        **kwargs,
    ) -> Iterator[Dict]:
        """The events uploaded from start to end, one at a time, as Amplitude sends them."""
        wanted = set(event_types) if event_types else None
        for _, _, batches in self.export_windows(start, end, **kwargs):
            for batch in batches:
                for event in batch:
                    if wanted is None or event.get('event_type') in wanted:
                        yield event

    def load_events(
        self,
        start: Union[str, dt.datetime],
        end: Union[str, dt.datetime] = None,
        event_types: List[str] = None,
        expand_properties: bool = False,
        polars: bool = False,
        **kwargs,
    ):
        """
        The events uploaded to Amplitude from start to end (UTC, whole hours, both
        included; end defaults to the newest complete hour), as a frame. Times are UTC
        datetimes; event_properties, user_properties and the other objects stay dicts, or
        with expand_properties become columns such as `event_properties.plan`.
        """
        events = list(self.iter_events(start, end, event_types=event_types, **kwargs))
        return events_frame(events, expand_properties=expand_properties, polars=polars)

    def load(self, *args, **kwargs):
        """The events of a range; see load_events."""
        return self.load_events(*args, **kwargs)

    def export(self, *args, **kwargs) -> None:
        raise NotImplementedError(
            'Send events to Amplitude with its SDKs or HTTP API; this client reads.',
        )

    # ---- Metrics (Dashboard REST API) ---------------------------------------------------

    def _series(self, path: str, start, end, params: Dict = None) -> List[Dict]:
        params = dict(params or {})
        params.update(
            start=to_utc(start).strftime(DAY_FORMAT), end=to_utc(end).strftime(DAY_FORMAT),
        )
        return series_rows(self._json(path, params).get('data') or {})

    def active_users(self, start, end, interval: int = 1, new: bool = False, polars=False):
        """Active (or new) users per day, week (7) or month (30): date, segment, value."""
        metric = 'new' if new else 'active'
        rows = self._series('/api/2/users', start, end, dict(m=metric, i=interval))
        return _frame(rows, polars)

    def average_session_length(self, start, end, polars=False):
        """Average session length in seconds per day."""
        return _frame(self._series('/api/2/sessions/average', start, end), polars)

    def sessions_per_user(self, start, end, polars=False):
        """Average sessions per user per day."""
        return _frame(self._series('/api/2/sessions/peruser', start, end), polars)

    def event_totals(
        self, event_type: str, start, end, metric: str = 'totals', interval: int = 1,
        group_by: str = None, polars=False,
    ):
        """
        Event segmentation: one event's totals (or uniques, pct_dau, average) per day;
        group_by splits it by a user property (built-in like `country`, custom as
        `gp:plan`).
        """
        event = dict(event_type=event_type)
        if group_by:
            event['group_by'] = [dict(type='user', value=group_by)]
        rows = self._series(
            '/api/2/events/segmentation', start, end,
            dict(e=json.dumps(event), m=metric, i=interval),
        )
        return _frame(rows, polars)

    # ---- Definitions (Taxonomy API) and other lists -------------------------------------

    def _data(self, path: str, params: Dict = None) -> List[Dict]:
        data = self._json(path, params).get('data')
        return data if isinstance(data, list) else []

    def event_types(self, polars=False):
        """The project's event types with their category, description and flags."""
        return _frame(self._data('/api/2/taxonomy/event'), polars)

    def event_properties(self, event_type: str = None, polars=False):
        """Event properties and their types, for all events or one."""
        params = dict(event_type=event_type) if event_type else None
        return _frame(self._data('/api/2/taxonomy/event-property', params), polars)

    def user_properties(self, polars=False):
        """User properties and their types."""
        return _frame(self._data('/api/2/taxonomy/user-property'), polars)

    def annotations(self, polars=False):
        """Chart annotations: releases, campaigns and other marked dates."""
        return _frame(self._data('/api/3/annotations'), polars)

    def cohorts(self, polars=False):
        """The project's behavioral cohorts: id, name, size, owners, times."""
        return _frame(self._json('/api/3/cohorts').get('cohorts') or [], polars)

    def iter_cohort_members(
        self, cohort_id: str, properties: Union[bool, List[str]] = False,
    ) -> Iterator[Dict]:
        """
        The users of a cohort. Amplitude computes the file asynchronously: the request is
        made, its status polled, and the file read. Each call counts toward the 500 cohort
        downloads a month of the Behavioral Cohorts API.
        """
        params = {}
        if properties:
            params['props'] = 1
            if isinstance(properties, (list, tuple)):
                params['propKeys'] = list(properties)
        request = self._json(f'/api/5/cohorts/request/{cohort_id}', params)
        request_id = request.get('request_id')
        if not request_id:
            raise AmplitudeError(
                f'Amplitude did not start the download of cohort {cohort_id}: {request}',
            )
        waited = 0.0
        while True:
            status = self._json(f'/api/5/cohorts/request-status/{request_id}')
            state = str(status.get('async_status') or '').upper()
            if 'COMPLETED' in state:
                break
            if 'FAIL' in state or 'ERROR' in state:
                raise AmplitudeError(f'Amplitude could not compute cohort {cohort_id}: {status}')
            if waited >= COHORT_TIMEOUT_SECONDS:
                raise AmplitudeError(f'Cohort {cohort_id} was not ready after {waited:.0f} s.')
            self.sleep(COHORT_POLL_SECONDS)
            waited += COHORT_POLL_SECONDS
        # Large cohorts redirect to a short-lived Amazon S3 link, which requests follows
        # without the Amplitude credentials.
        response = self._request(f'/api/5/cohorts/request/{request_id}/file')
        yield from cohort_rows(response.content, cohort_id)

    def cohort_members(
        self, cohort_id: str, properties: Union[bool, List[str]] = False, polars=False,
    ):
        """The users of a cohort as a frame: cohort_id, amplitude_id, user_id, properties."""
        return _frame(list(self.iter_cohort_members(cohort_id, properties)), polars)


class _TooLarge(Exception):
    pass


class _Truncated(Exception):
    pass


def read_export(path: str) -> Iterator[Dict]:
    """
    The events of an Export API file: a ZIP of gzipped JSON lines, one file per hour
    (`<project>/<project>_<date>_<hour>#<n>.json.gz`), read in hour order without loading
    the whole export.
    """
    with zipfile.ZipFile(path) as archive:
        for name in sorted(archive.namelist(), key=_hour_order):
            if name.endswith('/'):
                continue
            with archive.open(name) as member:
                if name.endswith('.zip'):
                    # Older exports nested a ZIP per day.
                    nested = io.BytesIO(member.read())
                    with tempfile.NamedTemporaryFile(suffix='.zip') as inner:
                        inner.write(nested.getvalue())
                        inner.flush()
                        yield from read_export(inner.name)
                    continue
                stream = gzip.GzipFile(fileobj=member) if name.endswith('.gz') else member
                for line in io.TextIOWrapper(stream, encoding='utf-8'):
                    line = line.strip()
                    if line:
                        yield json.loads(line)


def _hour_order(name: str) -> Tuple:
    """
    Export files in hour order: `<project>_<date>_<hour>#<n>.json.gz` does not pad the
    hour, so sorted as text hour 10 came before hour 2.
    """
    import re

    match = re.search(r'_(\d{4}-\d{2}-\d{2})_(\d+)#(\d+)', name)
    if not match:
        return (name, 0, 0, name)
    date, hour, part = match.groups()
    return (date, int(hour), int(part), name)


def series_rows(data: Dict) -> List[Dict]:
    """
    Rows of a Dashboard REST response: one per date and segment, with its value. The
    response holds xValues (dates), series (one list of values per segment) and the
    segments' names in seriesLabels or seriesMeta.
    """
    dates = data.get('xValues') or []
    series = data.get('series') or []
    labels = data.get('seriesLabels') or data.get('seriesMeta') or []
    rows = []
    for index, values in enumerate(series):
        label = labels[index] if index < len(labels) else None
        if isinstance(label, dict):
            label = label.get('segmentIndex', index)
        if isinstance(label, list):
            label = ' / '.join(str(part) for part in label)
        segment = '' if label is None else str(label)
        for position, value in enumerate(values if isinstance(values, list) else []):
            if position >= len(dates):
                break
            if isinstance(value, dict):
                value = value.get('value')
            rows.append(dict(date=str(dates[position])[:10], segment=segment, value=value))
    return rows


def cohort_rows(content: bytes, cohort_id: str) -> Iterator[Dict]:
    """The users of a cohort file, which Amplitude sends as CSV or as JSON."""
    text = content.decode('utf-8-sig').strip()
    if not text:
        return
    if text[0] in '[{':
        data = json.loads(text)
        if isinstance(data, dict):
            data = data.get('users') or data.get('data') or []
        records = data
    else:
        records = list(csv.DictReader(io.StringIO(text)))
    for record in records:
        record = {str(k).strip(): v for k, v in dict(record).items()}
        amplitude_id = record.pop('amplitude_id', None) or record.pop('Amplitude ID', None)
        user_id = record.pop('user_id', None) or record.pop('User ID', None)
        try:
            amplitude_id = int(amplitude_id) if amplitude_id not in (None, '') else None
        except (TypeError, ValueError):
            pass
        yield dict(
            cohort_id=str(cohort_id),
            amplitude_id=amplitude_id,
            user_id=user_id or None,
            properties=record or None,
        )


def events_frame(events: List[Dict], expand_properties: bool = False, polars: bool = False):
    """A frame of exported events with UTC datetime columns."""
    if expand_properties:
        frame = pd.json_normalize(events, max_level=1) if events else pd.DataFrame()
    else:
        frame = pd.DataFrame(events)
    for column in TIME_FIELDS:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], utc=True, format='ISO8601')
    if polars:
        import polars as pl

        return pl.from_pandas(frame)
    return frame


def _frame(rows: List[Dict], polars: bool):
    if polars:
        import polars as pl

        return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()
    return pd.DataFrame(rows)
