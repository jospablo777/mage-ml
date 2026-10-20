"""
Amplitude for blocks and for the Amplitude source of data integration pipelines.

Events come from the Export API (https://amplitude.com/docs/apis/analytics/export): a ZIP
of gzipped JSON lines per range of hours, selected by server_upload_time, available about
two hours after Amplitude receives them. Metrics come from the Dashboard REST API, the
event and property definitions from the Taxonomy API, and cohorts from the Behavioral
Cohorts API. Every read uses the project's API key and secret key.

Rows are sent to Amplitude with the Batch Event Upload API
(https://amplitude.com/docs/apis/analytics/batch-event-upload), with the API key only: as
events, or as user properties ($identify events).

    from mage_ai.io.amplitude import Amplitude

    amplitude = Amplitude.with_config(ConfigFileLoader(config_path, 'default'))
    events = amplitude.load_events('2024-06-01', '2024-06-02')
    users = amplitude.active_users('2024-06-01', '2024-06-30')
    amplitude.set_user_properties(scores)    # user_id, churn_risk

What the old source did and this client does not: a 403, a 429 or a server error was
logged and the run ended with no rows; every event of the range was held in memory, the
files joined by string concatenation; dates mixed local time and UTC.
"""
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import math
import os
import tempfile
import time
import zipfile
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple, Union

import pandas as pd
import requests

from mage_ai.io.base import BaseIO
from mage_ai.io.config import BaseConfigLoader, ConfigKey

HOSTS = {
    'US': 'https://amplitude.com',
    'EU': 'https://analytics.eu.amplitude.com',
}
# The Batch Event Upload API, which takes events.
UPLOAD_HOSTS = {
    'US': 'https://api2.amplitude.com',
    'EU': 'https://api.eu.amplitude.com',
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
# A Batch Event Upload request takes up to 2,000 events and 20 MB.
UPLOAD_MAX_EVENTS = 2000
UPLOAD_MAX_BYTES = 10 * 1024 * 1024
# Amplitude asks to wait at least 15 s after a 429 of its upload APIs.
UPLOAD_THROTTLE_SECONDS = 15
# The fields of an event Amplitude takes; any other column is an event property.
EVENT_FIELDS = frozenset((
    'adid', 'android_app_set_id', 'android_id', 'app_version', 'carrier', 'city',
    'country', 'device_brand', 'device_id', 'device_manufacturer', 'device_model', 'dma',
    'event_id', 'event_type', 'group_properties', 'groups', 'idfa', 'idfv', 'insert_id',
    'ip', 'language', 'location_lat', 'location_lng', 'os_name', 'os_version', 'platform',
    'price', 'productId', 'quantity', 'region', 'revenue', 'revenueType',
    'session_id', 'time', 'user_id', 'user_properties', 'event_properties',
))
# The fields an $identify event keeps (the Identify API's); any other column, revenue and
# price among them, is a user property to set.
IDENTIFY_FIELDS = frozenset((
    'app_version', 'carrier', 'city', 'country', 'device_brand', 'device_id',
    'device_manufacturer', 'device_model', 'dma', 'groups', 'language', 'os_name',
    'os_version', 'platform', 'region', 'user_id', 'user_properties',
))
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
        secret_key: str = None,
        region: str = 'US',
        host: str = None,
        upload_host: str = None,
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
            secret_key (str): The project's secret key; reading needs it, sending does not.
            region (str): 'US' (amplitude.com) or 'EU' (analytics.eu.amplitude.com), where
                the project's data is stored. A key of one region is refused by the other.
            host (str): Another base URL, such as a proxy; overrides region. Events are
                sent there too, unless upload_host is given.
            upload_host (str): Another base URL for sending events (api2.amplitude.com,
                api.eu.amplitude.com in the EU).
            log: Where progress messages go; print when verbose. The integration source
                passes its logger: its stdout carries the records.
        """
        super().__init__(verbose=verbose)
        # env_var() of a variable that is not set renders as the text None.
        unset = [
            name for name, value in (('api_key', api_key), ('secret_key', secret_key))
            if value == 'None'
        ]
        if unset:
            raise AmplitudeError(
                f'Amplitude {" and ".join(unset)} rendered as None: the environment variable '
                'or secret it reads is not set where MageML runs.'
            )
        if not api_key:
            raise AmplitudeError(
                'Amplitude needs the project API key (and its secret key to read): '
                'Settings > Projects > your project > General.'
            )
        region = (region or 'US').strip().upper()
        if not host and region not in HOSTS:
            raise AmplitudeError(f"region must be 'US' or 'EU', not {region!r}.")
        self.api_key = api_key
        self.secret_key = secret_key
        self.region = region
        self.host = (host or HOSTS[region]).rstrip('/')
        self.upload_host = (upload_host or host or UPLOAD_HOSTS[region]).rstrip('/')
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
        if not self.secret_key:
            raise AmplitudeError(
                'Reading from Amplitude needs the secret key as well as the API key: '
                'Settings > Projects > your project > General.'
            )
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

    def _backoff(
        self, attempt: int, retry_after: Optional[str], path: str, reason, minimum: float = 0,
    ) -> None:
        delay = max(min(2 ** attempt * 2, MAX_BACKOFF_SECONDS), minimum)
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

    def export(self, rows, *args, user_properties: bool = False, **kwargs) -> int:
        """
        Sends rows to Amplitude: as events (send_events), or with user_properties=True as
        user properties (set_user_properties).
        """
        if user_properties:
            return self.set_user_properties(rows, *args, **kwargs)
        return self.send_events(rows, *args, **kwargs)

    # ---- Sending (Batch Event Upload API) ----------------------------------------------

    def send_events(
        self,
        rows,
        event_type: str = None,
        key_columns: List[str] = None,
        columns: Dict[str, str] = None,
        min_id_length: int = None,
    ) -> int:
        """
        Sends rows as events: a pandas or Polars frame, or dicts. Each row needs user_id or
        device_id, and event_type (a column, or the argument for every row). time is a
        datetime, an ISO 8601 string or epoch seconds or milliseconds; Amplitude's other
        event fields (country, platform, revenue, session_id, ...) are used as they are,
        and every other column becomes an event property. columns names the frame's
        columns for those fields: {'user_id': 'customer_id', 'time': 'created_at'}.

        Each event gets an insert_id: the row's insert_id, else a hash of key_columns,
        else a hash of the event. Amplitude drops an insert_id it saw in the last 7 days,
        so sending the same rows again, or a retry, adds nothing. Returns the events sent.
        """
        events = (
            amplitude_event(
                row, event_type=event_type, key_columns=key_columns, columns=columns,
            )
            for row in _rows(rows)
        )
        return self.upload(events, min_id_length=min_id_length)

    def set_user_properties(
        self, rows, columns: Dict[str, str] = None, min_id_length: int = None,
    ) -> int:
        """
        Sets user properties: each row has user_id or device_id, and every other column is
        a property to set (columns: {'user_id': 'customer_id'}). A user_properties column
        of operations ({'$add': {...}}, {'$setOnce': {...}}) is sent as it is. Sent as
        $identify events, 2,000 a request. Returns the rows sent.
        """
        events = (
            amplitude_event(row, identify=True, columns=columns) for row in _rows(rows)
        )
        return self.upload(events, min_id_length=min_id_length)

    def upload(self, events: Iterable[Dict], min_id_length: int = None) -> int:
        """Sends events already in Amplitude's format, in requests of up to 2,000."""
        sent = 0
        for chunk in upload_chunks(item for item in events if item):
            sent += self._upload(chunk, min_id_length)
        return sent

    def _upload(self, events: List[Dict], min_id_length: int = None) -> int:
        """
        One request of the Batch Event Upload API. 429 waits at least 15 s, as Amplitude
        asks, and server errors are retried; every event has an insert_id, so a request
        sent again adds nothing twice. 413 (over 20 MB) splits the request.
        """
        body = dict(api_key=self.api_key, events=events)
        if min_id_length:
            body['options'] = dict(min_id_length=int(min_id_length))
        path = '/batch'
        attempt = 0
        while True:
            try:
                response = self.session.post(
                    f'{self.upload_host}{path}', json=body, timeout=self.timeout,
                )
            except (requests.ConnectionError, requests.Timeout) as error:
                if attempt >= self.max_retries:
                    raise AmplitudeError(f'Amplitude did not answer {path}: {error}') from error
                self._backoff(attempt, None, path, str(error))
                attempt += 1
                continue
            status = response.status_code
            if status < 300:
                return len(events)
            answer = _json_or_empty(response)
            if status == 413 and len(events) > 1:
                half = len(events) // 2
                return (
                    self._upload(events[:half], min_id_length)
                    + self._upload(events[half:], min_id_length)
                )
            if status == 429:
                over = (
                    answer.get('exceeded_daily_quota_users')
                    or answer.get('exceeded_daily_quota_devices')
                )
                if over:
                    raise AmplitudeError(
                        f'Amplitude refused events of {len(over)} users or devices that sent '
                        'over 500,000 events in the last 24 hours. Leave them out, or '
                        f'ask Amplitude to raise the limit. {sorted(over)[:5]}'
                    )
            if (status == 429 or status >= 500) and attempt < self.max_retries:
                self._backoff(
                    attempt, None, path, status,
                    minimum=UPLOAD_THROTTLE_SECONDS if status == 429 else 0,
                )
                attempt += 1
                continue
            raise AmplitudeError(self._explain_upload(response, answer, events))

    def _explain_upload(self, response: requests.Response, answer: Dict, events: List) -> str:
        status = response.status_code
        error = str(answer.get('error') or response.text[:300]).strip()
        if 'api key' in error.lower() or 'api_key' in error.lower():
            other = 'EU' if self.region == 'US' else 'US'
            return (
                f'Amplitude refused the API key ({error}). Check it, and the region: events '
                f'are sent to {self.region} ({self.upload_host}); a project stored in '
                f'{other} needs region {other}.'
            )
        if status == 400:
            details = []
            if answer.get('missing_field'):
                details.append(f"missing field {answer['missing_field']}")
            for key, kind in (
                ('events_with_invalid_fields', 'invalid'),
                ('events_with_missing_fields', 'missing'),
            ):
                for field, indexes in (answer.get(key) or {}).items():
                    first = events[indexes[0]] if indexes and indexes[0] < len(events) else {}
                    who = first.get('user_id') or first.get('device_id')
                    details.append(
                        f'{field} {kind} in {len(indexes)} of {len(events)} events '
                        f'(the first: {first.get("event_type")} of {who})'
                    )
            if 'user_id' in str(details) or 'device_id' in str(details):
                details.append(
                    'Amplitude needs ids of at least 5 characters; min_id_length lowers it',
                )
            return f'Amplitude refused the events (400): {error}. ' + '; '.join(details)
        if status == 429:
            return (
                f'Amplitude kept throttling the events (429) after {self.max_retries} tries: '
                'a user or device sent over 1,000 events a second. Send fewer at a time.'
            )
        return f'Amplitude answered {status} for the events: {error}'

    def check_api_key(self) -> None:
        """
        Checks the API key and region for sending without sending an event: an empty
        request, which Amplitude refuses for the missing events once the key is valid.
        """
        try:
            response = self.session.post(
                f'{self.upload_host}/batch',
                json=dict(api_key=self.api_key, events=[]),
                timeout=min(self.timeout, 30),
            )
        except (requests.ConnectionError, requests.Timeout) as error:
            raise AmplitudeError(f'Amplitude did not answer {self.upload_host}: {error}') from error
        if response.status_code < 300:
            return
        answer = _json_or_empty(response)
        error = str(answer.get('error') or '').lower()
        if response.status_code == 400 and 'key' not in error:
            return
        raise AmplitudeError(self._explain_upload(response, answer, []))

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


def _rows(rows) -> Iterator[Dict]:
    """The rows of a pandas or Polars frame, or of an iterable of dicts."""
    if isinstance(rows, pd.DataFrame):
        yield from rows.to_dict('records')
    elif hasattr(rows, 'iter_rows'):
        yield from rows.iter_rows(named=True)
    elif isinstance(rows, dict):
        yield rows
    else:
        yield from rows


def _plain(value):
    """A JSON value: NaN and NaT are None, numpy scalars Python ones, times ISO 8601."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (dt.datetime, dt.date)):
        if value is pd.NaT:
            return None
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    if hasattr(value, 'tolist') and not isinstance(value, (str, bytes)):
        return _plain(value.tolist())
    if value is pd.NaT:
        return None
    return value


def event_time_ms(value) -> Optional[int]:
    """Milliseconds since the epoch, from a datetime, an ISO 8601 string or epoch s or ms."""
    if value is None or value == '':
        return None
    if isinstance(value, bool):
        raise AmplitudeError(f'time must be a date and time, not {value!r}.')
    if isinstance(value, (int, float)):
        # Seconds until the year 5138; milliseconds after.
        return int(value * 1000) if abs(value) < 1e11 else int(value)
    if isinstance(value, str) and value.strip().lstrip('-').isdigit():
        return event_time_ms(int(value))
    moment = to_utc(value)
    return int(moment.timestamp() * 1000)


def _insert_id(parts) -> str:
    text = json.dumps(parts, sort_keys=True, default=str, separators=(',', ':'))
    return hashlib.sha256(text.encode()).hexdigest()


def amplitude_event(
    row: Dict,
    event_type: str = None,
    identify: bool = False,
    key_columns: List[str] = None,
    columns: Dict[str, str] = None,
) -> Optional[Dict]:
    """
    A row as an event of the Batch Event Upload API. Amplitude's event fields are used as
    they are (time in epoch milliseconds); other columns become event properties, or with
    identify, user properties to $set on a $identify event, which keeps only the fields of
    the Identify API (ids, device and location). Columns starting with _mage_
    are left out. The insert_id is the row's ($insert_id of an exported event), else a
    hash of key_columns, else a hash of the event; an identify event without one has
    none, since setting a property twice changes nothing. A row of user properties that
    are all empty gives None: there is nothing to send. columns names the row's columns
    for Amplitude's fields, such as {'user_id': 'customer_id', 'time': 'created_at'}.
    """
    if columns:
        row = dict(row)
        renamed = {}
        for field, column in columns.items():
            if column and column != field and column in row:
                row[field] = row.pop(column)
                renamed[column] = field
        if key_columns:
            key_columns = [renamed.get(key, key) for key in key_columns]
    row = {
        key: _plain(value) for key, value in row.items()
        if not str(key).startswith('_mage_')
    }
    row = {key: value for key, value in row.items() if value is not None}
    insert_id = row.pop('insert_id', None) or row.pop('$insert_id', None)
    moment = row.pop('time', None)
    if moment is None and 'event_time' in row:
        moment = row.pop('event_time')
    fields = IDENTIFY_FIELDS if identify else EVENT_FIELDS
    event = {key: row.pop(key) for key in list(row) if key in fields}
    for key in ('user_id', 'device_id'):
        if key in event:
            event[key] = str(event[key])
    if not event.get('user_id') and not event.get('device_id'):
        raise AmplitudeError(
            f'An Amplitude event needs user_id or device_id; this row has neither: '
            f'{sorted(event) + sorted(row)}.'
        )
    if moment is not None:
        event['time'] = event_time_ms(moment)

    if identify:
        operations = event.pop('user_properties', None) or {}
        if not isinstance(operations, dict):
            raise AmplitudeError('user_properties must be an object of properties.')
        properties = {k: v for k, v in operations.items() if not k.startswith('$')}
        operations = {k: v for k, v in operations.items() if k.startswith('$')}
        properties.update(row)
        if properties:
            operations['$set'] = {**operations.get('$set', {}), **properties}
        if not operations:
            # Every property is empty: nothing to send.
            return None
        event['event_type'] = '$identify'
        event['user_properties'] = operations
        if insert_id:
            event['insert_id'] = str(insert_id)
        return event

    event['event_type'] = str(event.get('event_type') or event_type or '')
    if not event['event_type']:
        raise AmplitudeError(
            'An Amplitude event needs an event_type: a column, or one name for every row.',
        )
    if row:
        event['event_properties'] = {**(event.get('event_properties') or {}), **row}
    if not insert_id:
        if key_columns and all(key in row or key in event for key in key_columns):
            values = {**event, **row}
            insert_id = _insert_id([event['event_type']] + [values[k] for k in key_columns])
        else:
            insert_id = _insert_id(event)
    event['insert_id'] = str(insert_id)
    return event


def upload_chunks(events: Iterable[Dict]) -> Iterator[List[Dict]]:
    """Events in lists of up to 2,000 and 10 MB, the half of a request's 20 MB."""
    chunk, size = [], 0
    for item in events:
        length = len(json.dumps(item, separators=(',', ':'))) + 1
        if chunk and (len(chunk) >= UPLOAD_MAX_EVENTS or size + length > UPLOAD_MAX_BYTES):
            yield chunk
            chunk, size = [], 0
        chunk.append(item)
        size += length
    if chunk:
        yield chunk


def _json_or_empty(response: requests.Response) -> Dict:
    try:
        answer = response.json()
    except ValueError:
        return {}
    return answer if isinstance(answer, dict) else {}


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
