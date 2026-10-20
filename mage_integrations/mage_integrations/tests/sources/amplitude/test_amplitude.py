"""
The Amplitude source of data integration pipelines against a local server that answers as
Amplitude's APIs do (mage_ai.tests.io.fixtures_amplitude.py).
"""
import contextlib
import datetime as dt
import io
import json
import unittest
from unittest.mock import patch

from mage_ai.tests.io.fixtures_amplitude import (
    API_KEY,
    SECRET_KEY,
    FakeAmplitude,
    event,
)
from mage_integrations.sources.amplitude import Amplitude, next_hour, window_bookmark

DAY = dt.datetime(2024, 6, 1)
NOW = dt.datetime(2024, 6, 2, 12, 30, tzinfo=dt.timezone.utc)


def events_between(first_hour: int, hours: int, per_hour: int = 2, day=DAY):
    return [
        event(hour * 100 + n, day + dt.timedelta(hours=hour, minutes=n),
              event_type='Play Song' if n % 2 == 0 else 'Skip Song')
        for hour in range(first_hour, first_hour + hours) for n in range(per_hour)
    ]


def run(fake, streams, config=None, state=None, query=None, sample=False, now=NOW):
    """Runs a sync and returns its RECORD values per stream and its STATE messages."""
    source = Amplitude(
        config=dict(api_key=API_KEY, secret_key=SECRET_KEY, host=fake.url,
                    start_date='2024-06-01', **(config or {})),
        state=state, query=query or {}, log_to_stdout=False, verbose=False,
        selected_streams=streams, load_sample_data=sample,
    )
    source.client.sleep = lambda seconds: None
    catalog = source.discover(streams=streams)
    for stream in catalog.streams:
        # Every column, as when the streams are picked in the editor.
        for entry in stream.metadata:
            entry['metadata']['selected'] = True
    out = io.StringIO()
    with patch('mage_ai.io.amplitude.utc_now', return_value=now), \
            patch('mage_integrations.sources.amplitude.utc_now', return_value=now), \
            contextlib.redirect_stdout(out):
        if sample:
            source.catalog = catalog
            source.process()
        else:
            source.sync(catalog)
    records, states, samples = {}, [], []
    for line in out.getvalue().splitlines():
        if not line.startswith('{'):
            continue
        message = json.loads(line)
        if message.get('type') == 'RECORD':
            records.setdefault(message['stream'], []).append(message['record'])
        elif message.get('type') == 'STATE':
            states.append(message['value'])
        elif message.get('type') == 'SAMPLE_DATA':
            samples.append(message)
    return records, states, samples


class AmplitudeSourceTest(unittest.TestCase):
    def test_streams_are_discovered_with_keys_and_replication(self):
        source = Amplitude(config=dict(api_key='a', secret_key='b'))
        streams = {s.tap_stream_id: s for s in source.discover().streams}
        self.assertEqual(set(streams), {
            'events', 'active_users', 'new_users', 'average_session_length',
            'sessions_per_user', 'event_types', 'event_properties', 'user_properties',
            'annotations', 'cohorts', 'cohort_members',
        })
        events = streams['events'].to_dict()
        self.assertEqual(events['key_properties'], ['uuid'])
        self.assertEqual(events['replication_method'], 'INCREMENTAL')
        self.assertEqual(events['replication_key'], 'server_upload_time')
        self.assertEqual(events['unique_constraints'], ['uuid'])
        self.assertEqual(events['unique_conflict_method'], 'UPDATE')
        self.assertEqual(streams['active_users'].to_dict()['replication_key'], 'date')
        self.assertEqual(streams['cohorts'].to_dict()['replication_method'], 'FULL_TABLE')

    def test_events_sync_window_by_window_and_continue_from_the_saved_hour(self):
        # Events up to 2024-06-02 09:00, the newest hour complete at 12:30.
        with FakeAmplitude(events_between(0, 34)) as fake:
            records, states, _ = run(fake, ['events'], config=dict(window_hours=12))
            events = records['events']
            self.assertEqual(len(events), 34 * 2)
            # Times are ISO 8601 in UTC.
            self.assertEqual(events[0]['event_time'], '2024-06-01T00:00:00+00:00')
            self.assertEqual(events[0]['event_properties']['song'], 'song 0')
            # One position per window, not per record: 00-11, 12-23, 00-09.
            positions = [s['bookmarks']['events']['server_upload_time'] for s in states]
            self.assertEqual(positions, [
                '2024-06-01T11:59:59.999999+00:00',
                '2024-06-01T23:59:59.999999+00:00',
                '2024-06-02T09:59:59.999999+00:00',
            ])

            # Late events of 09:00 arrived after the sync: they are not read twice, the
            # next sync starts at 10:00 and reads what is new.
            fake.events += events_between(10, 1, day=DAY + dt.timedelta(days=1))
            state = dict(bookmarks=dict(events=dict(server_upload_time=positions[-1])))
            records, states, _ = run(
                fake, ['events'], state=state, now=NOW + dt.timedelta(hours=1),
            )
            self.assertEqual([e['uuid'][-4:] for e in records['events']], ['1000', '1001'])

    def test_a_sync_with_nothing_new_writes_no_state(self):
        with FakeAmplitude(events_between(0, 2)) as fake:
            state = dict(bookmarks=dict(events=dict(
                server_upload_time='2024-06-02T09:59:59.999999+00:00',
            )))
            records, states, _ = run(fake, ['events'], state=state)
        self.assertEqual((records, states), ({}, []))

    def test_a_backfill_reads_its_range_and_keeps_the_saved_position(self):
        with FakeAmplitude(events_between(0, 6)) as fake:
            saved = '2024-06-02T09:59:59.999999+00:00'
            records, states, _ = run(
                fake, ['events'],
                state=dict(bookmarks=dict(events=dict(server_upload_time=saved))),
                query=dict(start_date='2024-06-01T02:00', end_date='2024-06-01T03:00'),
            )
        self.assertEqual(len(records['events']), 4)
        self.assertEqual(states, [])

    def test_event_types_filter_the_events(self):
        with FakeAmplitude(events_between(0, 3)) as fake:
            records, _, _ = run(fake, ['events'], config=dict(event_types='Skip Song'))
        self.assertEqual({e['event_type'] for e in records['events']}, {'Skip Song'})
        self.assertEqual(len(records['events']), 3)

    def test_metrics_read_the_last_days_again(self):
        with FakeAmplitude() as fake:
            records, _, _ = run(
                fake, ['active_users'],
                state=dict(bookmarks=dict(active_users=dict(date='2024-06-02'))),
                config=dict(metrics_lookback_days=1),
            )
        rows = records['active_users']
        self.assertEqual([r['date'] for r in rows], ['2024-06-01', '2024-06-02'])
        self.assertEqual(rows[0], dict(date='2024-06-01', segment='0', value=101))

    def test_definitions_annotations_and_cohorts(self):
        with FakeAmplitude() as fake:
            records, _, _ = run(fake, [
                'event_types', 'event_properties', 'user_properties', 'annotations', 'cohorts',
                'cohort_members',
            ], config=dict(cohort_ids=['abc123']))
        self.assertEqual(records['event_types'][0]['event_type'], 'Play Song')
        self.assertEqual(records['event_properties'][0]['event_property'], 'song')
        self.assertEqual(records['user_properties'][0]['user_property'], 'plan')
        self.assertEqual(records['annotations'][0]['label'], 'Release 2.0')
        self.assertEqual(records['cohorts'][0]['name'], 'Power listeners')
        self.assertEqual(
            [(m['cohort_id'], m['amplitude_id']) for m in records['cohort_members']],
            [('abc123', 111), ('abc123', 222)],
        )

    def test_sample_data_reads_the_newest_hour(self):
        with FakeAmplitude(events_between(0, 34)) as fake:
            _, _, samples = run(fake, ['events'], sample=True)
        sample = json.loads(samples[0]['sample_data'])
        self.assertEqual(len(sample['uuid']), 2)

    def test_wrong_keys_stop_the_sync(self):
        with FakeAmplitude(events_between(0, 2)) as fake:
            source = Amplitude(config=dict(
                api_key=API_KEY, secret_key='wrong', host=fake.url, region='US',
            ), log_to_stdout=False)
            with self.assertRaisesRegex(Exception, 'region'):
                source.test_connection()

    def test_eu_hosts_of_old_settings_read_as_the_eu_region(self):
        source = Amplitude(config=dict(
            api_key='a', secret_key='b', host='https://analytics.eu.amplitude.com',
        ))
        self.assertEqual(source.client.host, 'https://analytics.eu.amplitude.com')
        self.assertEqual(source.client.region, 'EU')

    def test_positions_mark_whole_hours(self):
        last = dt.datetime(2024, 6, 1, 5, tzinfo=dt.timezone.utc)
        self.assertEqual(next_hour(window_bookmark(last)), last + dt.timedelta(hours=1))
        # A position inside an hour reads that hour again.
        self.assertEqual(next_hour('2024-06-01T05:10:00+00:00'), last)

    def test_listed_streams_say_what_they_hold(self):
        streams = {
            s['tap_stream_id']: s
            for s in Amplitude(config=dict(api_key='a', secret_key='b')).discover_streams()
        }
        self.assertIn('Every event', streams['events']['schema']['description'])
        self.assertIn('500 downloads', streams['cohort_members']['schema']['description'])

    def test_a_quiet_project_samples_its_last_events(self):
        # The newest complete hour (09:00 on day 2) has no events; the day before has.
        with FakeAmplitude(events_between(0, 20)) as fake:
            _, _, samples = run(fake, ['events'], sample=True)
            exports = [r for r in fake.requests if r.startswith('/api/2/export')]
        sample = json.loads(samples[0]['sample_data'])
        # Hours 10 to 19 of day 1, two events each.
        self.assertEqual(len(sample['uuid']), 20)
        # The last hour, then 6 hours, then a day: three requests, not a week of data.
        self.assertEqual(len(exports), 3)

