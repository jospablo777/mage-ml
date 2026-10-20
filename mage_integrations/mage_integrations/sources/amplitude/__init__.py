"""
The Amplitude source of data integration pipelines.

Streams:
- events: every event, from the Export API, by the hour Amplitude received it
  (server_upload_time). Each sync continues from the last hour it exported up to the
  newest hour Amplitude has finished (about two hours ago), and saves its position after
  each window, so a failed sync goes on from there. Events are keyed by uuid, which
  destinations use to drop the repeats Amplitude can send.
- active_users, new_users, average_session_length, sessions_per_user: daily metrics of
  the Dashboard REST API; the last days are read again, as late events change them.
- event_types, event_properties, user_properties: the tracking plan (Taxonomy API).
- annotations, cohorts: chart annotations and the behavioral cohorts.
- cohort_members: the users of the cohorts listed in cohort_ids.

The window of a scheduled run (_start_date and _end_date) is not used: the last hours of
a day were exported before Amplitude had them, and never read again. start_date and
end_date in the source's query read a range once (a backfill) without moving the saved
position back.
"""
import datetime as dt
from typing import Dict, Generator, List

import singer

from mage_ai.io.amplitude import (
    AVAILABILITY_DELAY_HOURS,
    WINDOW_HOURS,
)
from mage_ai.io.amplitude import Amplitude as AmplitudeClient
from mage_ai.io.amplitude import (
    AmplitudeError,
    floor_hour,
    last_complete_hour,
    to_utc,
    utc_now,
)
from mage_integrations.sources.amplitude.constants import (
    DEFAULT_METRICS_LOOKBACK_DAYS,
    DEFAULT_START_DAYS,
    METRIC_STREAMS,
    REPLICATION_KEYS,
    REPLICATION_METHODS,
    SAMPLE_ROWS,
    STREAM_ACTIVE_USERS,
    STREAM_ANNOTATIONS,
    STREAM_AVERAGE_SESSION_LENGTH,
    STREAM_COHORT_MEMBERS,
    STREAM_COHORTS,
    STREAM_EVENT_PROPERTIES,
    STREAM_EVENT_TYPES,
    STREAM_EVENTS,
    STREAM_NEW_USERS,
    STREAM_SESSIONS_PER_USER,
    STREAM_USER_PROPERTIES,
    TABLE_KEY_PROPERTIES,
)
from mage_integrations.sources.base import Source, main
from mage_integrations.sources.messages import write_state

LOGGER = singer.get_logger()
# Ranges the editor's sample reads, newest first, until one has events.
SAMPLE_LOOKBACK_HOURS = (1, 6, 24, 168)
EU_HOSTS = ('analytics.eu.amplitude.com',)
STANDARD_HOSTS = ('https://amplitude.com', 'https://analytics.eu.amplitude.com')
TIME_FIELDS = (
    'client_event_time', 'client_upload_time', 'event_time', 'processed_time',
    'server_received_time', 'server_upload_time', 'user_creation_time',
)


def iso_time(value):
    """Amplitude's 'YYYY-MM-DD HH:MM:SS.ffffff' (UTC) as ISO 8601 with its offset."""
    if not isinstance(value, str) or not value:
        return value
    try:
        return to_utc(value).isoformat()
    except (ValueError, OverflowError):
        return value


def event_record(event: Dict) -> Dict:
    record = dict(event)
    for field in TIME_FIELDS:
        if field in record:
            record[field] = iso_time(record[field])
    return record


def next_hour(bookmark: str) -> dt.datetime:
    """
    The first hour to export after a saved position. A window's position is its last
    microsecond, so the next hour follows; a position inside an hour (a sync stopped in
    the middle of it) reads that hour again.
    """
    return floor_hour(to_utc(bookmark) + dt.timedelta(microseconds=1))


def window_bookmark(last_hour: dt.datetime) -> str:
    return (last_hour + dt.timedelta(hours=1) - dt.timedelta(microseconds=1)).isoformat()


class Amplitude(Source):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._client = None

    @property
    def client(self) -> AmplitudeClient:
        if self._client is None:
            config = self.config or {}
            host = (config.get('host') or '').strip() or None
            region = (config.get('region') or '').strip().upper() or None
            if region is None:
                region = 'EU' if host and any(h in host for h in EU_HOSTS) else 'US'
            if host and host.rstrip('/') in STANDARD_HOSTS:
                host = None
            self._client = AmplitudeClient(
                api_key=config.get('api_key'),
                secret_key=config.get('secret_key'),
                region=region,
                host=host,
                verbose=False,
                log=self.logger.info,
            )
        return self._client

    def setting(self, key: str, default=None):
        value = (self.config or {}).get(key)
        return default if value in (None, '') else value

    # ---- Catalog ------------------------------------------------------------------------

    def get_forced_replication_method(self, stream_id: str) -> str:
        return REPLICATION_METHODS[stream_id]

    def get_table_key_properties(self, stream_id: str) -> List[str]:
        return TABLE_KEY_PROPERTIES[stream_id]

    def get_valid_replication_keys(self, stream_id: str) -> List[str]:
        key = REPLICATION_KEYS.get(stream_id)
        return [key] if key else []

    def build_catalog_entry(self, stream_id: str, schema, **kwargs):
        """
        Streams come with their keys and replication key, and destinations upsert by the
        keys: Amplitude can send an event again, and metrics of recent days are read
        again, so rows are replaced instead of repeated.
        """
        keys = TABLE_KEY_PROPERTIES[stream_id]
        replication_key = REPLICATION_KEYS.get(stream_id)
        kwargs.setdefault('key_properties', keys)
        kwargs.setdefault('unique_constraints', keys)
        kwargs.setdefault('unique_conflict_method', 'UPDATE')
        if replication_key:
            kwargs.setdefault('replication_key', replication_key)
            kwargs.setdefault('bookmark_properties', [replication_key])
        return super().build_catalog_entry(stream_id, schema, **kwargs)

    def test_connection(self) -> None:
        # The editor waits 10 seconds for the answer: one quick try, one retry.
        client = self.client
        client.timeout, client.max_retries = 4, 1
        client.test_connection()

    # ---- Sync ---------------------------------------------------------------------------

    def start_date(self) -> dt.datetime:
        configured = to_utc(self.setting('start_date'))
        return configured or utc_now() - dt.timedelta(days=DEFAULT_START_DAYS)

    def query_range(self, query: Dict):
        query = query or {}
        return to_utc(query.get('start_date') or None), to_utc(query.get('end_date') or None)

    def sync_stream(self, stream, properties: Dict = None) -> int:
        if stream.tap_stream_id != STREAM_EVENTS:
            return super().sync_stream(stream, properties)
        return self.sync_events(stream, properties)

    def sync_events(self, stream, properties: Dict = None) -> int:
        """Events window by window, saving the position after each one."""
        bookmark = ((self.state or {}).get('bookmarks') or {}).get(STREAM_EVENTS) or {}
        saved = bookmark.get(REPLICATION_KEYS[STREAM_EVENTS])
        start, end = self.query_range(self.query)
        backfill = start is not None
        if not backfill:
            start = next_hour(saved) if saved else self.start_date()
        delay = int(self.setting('availability_delay_hours', AVAILABILITY_DELAY_HOURS))
        newest = last_complete_hour(delay_hours=delay)
        if start > newest:
            self.logger.info(
                f'Amplitude events: nothing new; the next hour, {start:%Y-%m-%d %H}:00 UTC, '
                f'can be exported after {start + dt.timedelta(hours=1 + delay):%H}:00 UTC.',
            )
            return 0
        event_types = self.setting('event_types') or None
        if isinstance(event_types, str):
            event_types = [t.strip() for t in event_types.split(',') if t.strip()]
        wanted = set(event_types) if event_types else None

        sorted_before = self.is_sorted
        # The position is saved per window, not per record: a STATE message for each of
        # millions of events cost more than the events.
        self.is_sorted = False
        count = 0
        try:
            for first, last, batches in self.client.export_windows(
                start,
                end,
                window_hours=int(self.setting('window_hours', WINDOW_HOURS)),
                delay_hours=delay,
            ):
                window_count = 0
                for batch in batches:
                    rows = [
                        event_record(event) for event in batch
                        if wanted is None or event.get('event_type') in wanted
                    ]
                    if rows:
                        self.write_records(stream, rows, properties)
                    window_count += len(rows)
                count += window_count
                self.logger.info(
                    f'Amplitude events of {first:%Y-%m-%d %H}:00 to {last:%Y-%m-%d %H}:59 UTC: '
                    f'{window_count}.',
                )
                position = window_bookmark(last)
                if not saved or to_utc(position) > to_utc(saved):
                    state = {}
                    singer.write_bookmark(
                        state, STREAM_EVENTS, REPLICATION_KEYS[STREAM_EVENTS], position,
                    )
                    write_state(state)
                    saved = position
                if self.load_sample_data and count >= SAMPLE_ROWS:
                    break
        finally:
            self.is_sorted = sorted_before
        return count

    def load_data(
        self,
        stream=None,
        bookmarks: Dict = None,
        query: Dict = None,
        sample_data: bool = False,
        **kwargs,
    ) -> Generator[List[Dict], None, None]:
        stream_id = stream.tap_stream_id if stream is not None else STREAM_EVENTS
        client = self.client

        if stream_id == STREAM_EVENTS:
            yield self.sample_events()
            return

        if stream_id in METRIC_STREAMS:
            yield self.metric_rows(stream_id, bookmarks, query, sample_data)
            return

        if stream_id == STREAM_EVENT_TYPES:
            yield client._data('/api/2/taxonomy/event')
        elif stream_id == STREAM_EVENT_PROPERTIES:
            yield client._data('/api/2/taxonomy/event-property')
        elif stream_id == STREAM_USER_PROPERTIES:
            yield client._data('/api/2/taxonomy/user-property')
        elif stream_id == STREAM_ANNOTATIONS:
            yield client._data('/api/3/annotations')
        elif stream_id == STREAM_COHORTS:
            yield client._json('/api/3/cohorts').get('cohorts') or []
        elif stream_id == STREAM_COHORT_MEMBERS:
            yield from self.cohort_member_batches()

    def sample_events(self) -> List[Dict]:
        """
        Sample data in the editor: the newest events, from the newest complete hour, or the
        last 6 hours, day or week when it has none (a quiet project), without downloading
        more than the first range that has events.
        """
        newest = last_complete_hour()
        for hours in SAMPLE_LOOKBACK_HOURS:
            events = []
            first = newest - dt.timedelta(hours=hours - 1)
            for event in self.client.iter_events(first, newest, window_hours=hours):
                events.append(event_record(event))
                if len(events) >= SAMPLE_ROWS:
                    return events
            if events:
                return events
        return []

    def metric_rows(
        self, stream_id: str, bookmarks: Dict, query: Dict, sample_data: bool = False,
    ) -> List[Dict]:
        start, end = self.query_range(query)
        if start is None:
            saved = (bookmarks or {}).get('date')
            lookback = int(self.setting('metrics_lookback_days', DEFAULT_METRICS_LOOKBACK_DAYS))
            start = (
                to_utc(saved) - dt.timedelta(days=lookback) if saved else self.start_date()
            )
        end = end or utc_now()
        if sample_data or self.load_sample_data:
            start = end - dt.timedelta(days=7)
        series = {
            STREAM_ACTIVE_USERS: ('/api/2/users', dict(m='active', i=1)),
            STREAM_NEW_USERS: ('/api/2/users', dict(m='new', i=1)),
            STREAM_AVERAGE_SESSION_LENGTH: ('/api/2/sessions/average', {}),
            STREAM_SESSIONS_PER_USER: ('/api/2/sessions/peruser', {}),
        }
        path, params = series[stream_id]
        # The Dashboard REST API reads at most a year a request.
        rows = []
        first = start
        while first <= end:
            last = min(first + dt.timedelta(days=364), end)
            rows += self.client._series(path, first, last, params)
            first = last + dt.timedelta(days=1)
        return sorted(rows, key=lambda row: (row['date'], row['segment']))

    def cohort_member_batches(self) -> Generator[List[Dict], None, None]:
        cohort_ids = self.setting('cohort_ids') or []
        if isinstance(cohort_ids, str):
            cohort_ids = [c.strip() for c in cohort_ids.split(',') if c.strip()]
        if not cohort_ids:
            self.logger.info(
                'Amplitude cohort_members: no cohort_ids in the source settings; nothing '
                'to read. Each cohort download counts toward 500 a month.',
            )
            return
        properties = self.setting('cohort_properties') or False
        for cohort_id in cohort_ids:
            batch = []
            for row in self.client.iter_cohort_members(str(cohort_id), properties):
                batch.append(row)
                if len(batch) >= 10_000:
                    yield batch
                    batch = []
            if batch:
                yield batch


__all__ = ['Amplitude', 'AmplitudeError']


if __name__ == '__main__':
    main(Amplitude)
