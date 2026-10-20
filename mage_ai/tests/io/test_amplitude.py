"""
The Amplitude client (mage_ai/io/amplitude.py) against a local server that answers as
Amplitude's APIs do (fixtures_amplitude.py).
"""
import datetime as dt
import gzip
import io
import json
import tempfile
import zipfile

from mage_ai.io.amplitude import (
    Amplitude,
    AmplitudeError,
    hour_windows,
    last_complete_hour,
    read_export,
    series_rows,
)
from mage_ai.tests.base_test import TestCase
from mage_ai.tests.io.fixtures_amplitude import (
    API_KEY,
    SECRET_KEY,
    FakeAmplitude,
    event,
)

DAY = dt.datetime(2024, 6, 1)


def day_of_events(count_per_hour: int = 3):
    return [
        event(hour * 100 + n, DAY + dt.timedelta(hours=hour, minutes=n))
        for hour in range(24) for n in range(count_per_hour)
    ]


class AmplitudeClientTest(TestCase):
    def client(self, fake, **kwargs):
        sleeps = []
        client = Amplitude(
            API_KEY, SECRET_KEY, host=fake.url, verbose=False, sleep=sleeps.append, **kwargs,
        )
        client.sleeps = sleeps
        return client

    def test_events_of_a_range_come_back_in_hour_order_with_utc_times(self):
        with FakeAmplitude(day_of_events()) as fake:
            frame = self.client(fake).load_events(DAY, DAY + dt.timedelta(hours=23))
        self.assertEqual(len(frame), 72)
        self.assertEqual(str(frame['event_time'].dtype), 'datetime64[us, UTC]')
        self.assertTrue(frame['server_upload_time'].is_monotonic_increasing)
        self.assertEqual(frame['event_properties'][0]['song'], 'song 0')
        self.assertEqual(frame['amplitude_id'][0], 900000000000)

    def test_properties_expand_into_columns(self):
        with FakeAmplitude(day_of_events(1)) as fake:
            frame = self.client(fake).load_events(
                DAY, DAY + dt.timedelta(hours=1), expand_properties=True, polars=True,
            )
        self.assertIn('event_properties.song', frame.columns)
        self.assertIn('user_properties.plan', frame.columns)
        self.assertEqual(frame.height, 2)

    def test_windows_split_when_amplitude_says_the_export_is_too_large(self):
        with FakeAmplitude(day_of_events(), max_hours_per_export=5) as fake:
            client = self.client(fake)
            events = list(client.iter_events(DAY, DAY + dt.timedelta(hours=23)))
            self.assertEqual(len(events), 72)
            self.assertEqual(len({e['uuid'] for e in events}), 72)
            exports = [r for r in fake.requests if r.startswith('/api/2/export')]
            # 24 hours: refused, then 12, 6 and 3-hour windows until each fits.
            self.assertGreater(len(exports), 5)

    def test_an_hour_past_the_limit_says_what_to_do(self):
        with FakeAmplitude(day_of_events(), max_hours_per_export=0.5) as fake:
            with self.assertRaisesRegex(AmplitudeError, 'Amazon S3 or a warehouse'):
                list(self.client(fake).iter_events(DAY, DAY))

    def test_rate_limits_are_waited_out_with_retry_after(self):
        with FakeAmplitude(day_of_events(1)) as fake:
            fake.rate_limited['/api/2/export'] = 2
            client = self.client(fake)
            events = list(client.iter_events(DAY, DAY + dt.timedelta(hours=2)))
        self.assertEqual(len(events), 3)
        self.assertEqual(len(client.sleeps), 2)

    def test_a_download_cut_short_is_fetched_again(self):
        with FakeAmplitude(day_of_events(1)) as fake:
            fake.truncate_next_export = True
            client = self.client(fake, timeout=5)
            events = list(client.iter_events(DAY, DAY + dt.timedelta(hours=2)))
        self.assertEqual(len(events), 3)

    def test_wrong_keys_fail_and_name_the_region(self):
        with FakeAmplitude() as fake:
            client = Amplitude(API_KEY, 'wrong', host=fake.url, verbose=False)
            with self.assertRaisesRegex(AmplitudeError, 'region'):
                client.test_connection()
            # A failed export used to end the run with no rows and no error.
            with self.assertRaises(AmplitudeError):
                list(client.iter_events(DAY, DAY))

    def test_hours_without_events_are_no_rows(self):
        with FakeAmplitude([]) as fake:
            self.assertEqual(len(self.client(fake).load_events(DAY, DAY)), 0)

    def test_the_end_is_the_newest_hour_amplitude_has_finished(self):
        now = dt.datetime(2024, 6, 1, 12, 30, tzinfo=dt.timezone.utc)
        self.assertEqual(
            last_complete_hour(now), dt.datetime(2024, 6, 1, 9, tzinfo=dt.timezone.utc),
        )
        windows = hour_windows(DAY, DAY + dt.timedelta(hours=49), 24)
        self.assertEqual([(a.hour, b.hour, (b - a).days) for a, b in windows],
                         [(0, 23, 0), (0, 23, 0), (0, 1, 0)])

    def test_metrics_definitions_and_cohorts(self):
        with FakeAmplitude() as fake:
            client = self.client(fake)
            users = client.active_users('2024-06-01', '2024-06-03')
            self.assertEqual(users['value'].tolist(), [101, 102, 103])
            self.assertEqual(users['date'].tolist(), ['2024-06-01', '2024-06-02', '2024-06-03'])
            new = client.active_users('2024-06-01', '2024-06-01', new=True)
            self.assertEqual(new['value'][0], 11)
            length = client.average_session_length('2024-06-01', '2024-06-01')
            self.assertEqual(length['value'][0], 61.5)
            self.assertEqual(client.event_types()['event_type'].tolist(), ['Play Song'])
            self.assertEqual(client.user_properties()['user_property'].tolist(), ['plan'])
            self.assertEqual(client.annotations()['label'].tolist(), ['Release 2.0'])
            self.assertEqual(client.cohorts()['name'].tolist(), ['Power listeners'])
            members = client.cohort_members('abc123', properties=True)
            self.assertEqual(members['amplitude_id'].tolist(), [111, 222])
            self.assertEqual(members['user_id'][0], 'user-1')
            self.assertTrue(members['user_id'].isna()[1])
            self.assertEqual(members['properties'][0], {'plan': 'pro'})
            self.assertEqual(len(client.sleeps), 1)

    def test_series_rows_name_segments_by_label(self):
        rows = series_rows({
            'series': [[1, 2], [3, 4]], 'seriesLabels': ['US', ['DE', 'web']],
            'xValues': ['2024-06-01', '2024-06-02'],
        })
        self.assertEqual(rows[2], dict(date='2024-06-01', segment='DE / web', value=3))

    def test_exports_with_a_zip_per_day_are_read(self):
        inner = io.BytesIO()
        with zipfile.ZipFile(inner, 'w') as archive:
            archive.writestr('1/1_2014-11-01_0#0.json.gz', gzip.compress(b'{"uuid": "a"}\n'))
        with tempfile.NamedTemporaryFile(suffix='.zip') as outer:
            with zipfile.ZipFile(outer.name, 'w') as archive:
                archive.writestr('1/2014-11-01.zip', inner.getvalue())
            self.assertEqual(list(read_export(outer.name)), [{'uuid': 'a'}])

    def test_keys_are_required_and_regions_checked(self):
        with self.assertRaisesRegex(AmplitudeError, 'API key'):
            Amplitude('', '')
        with self.assertRaisesRegex(AmplitudeError, "'US' or 'EU'"):
            Amplitude('a', 'b', region='APAC')
        self.assertEqual(Amplitude('a', 'b', region='eu').host, 'https://analytics.eu.amplitude.com')
        self.assertEqual(Amplitude('a', region='eu').upload_host, 'https://api.eu.amplitude.com')
        self.assertEqual(Amplitude('a').upload_host, 'https://api2.amplitude.com')
        self.assertEqual(json.loads(json.dumps(series_rows({}))), [])


class AmplitudeTemplateTest(TestCase):
    def test_the_data_loader_template_reads_yesterdays_events(self):
        import os
        from unittest.mock import patch

        from mage_ai.data_preparation.models.constants import BlockType
        from mage_ai.data_preparation.templates.template import fetch_template_source

        code = fetch_template_source(
            BlockType.DATA_LOADER, dict(template_path='data_loaders/amplitude.py'),
        )
        self.assertIn('load_data_from_amplitude', code)
        io_config = os.path.join(self.repo_path, 'io_config.yaml')
        with open(io_config, 'w') as file:
            file.write(
                f'version: 0.1.1\ndefault:\n  AMPLITUDE_API_KEY: {API_KEY}\n'
                f'  AMPLITUDE_SECRET_KEY: {SECRET_KEY}\n  AMPLITUDE_REGION: US\n',
            )
        namespace = dict(data_loader=lambda function: function)
        with FakeAmplitude(day_of_events(1)) as fake, \
                patch.dict('mage_ai.io.amplitude.HOSTS', US=fake.url), \
                patch('mage_ai.settings.repo.get_repo_path', return_value=self.repo_path):
            code = code.replace('from mage_ai.settings.repo import get_repo_path\n', '')
            namespace['get_repo_path'] = lambda: self.repo_path
            exec(compile(code, 'amplitude.py', 'exec'), namespace)
            frame = namespace['load_data_from_amplitude'](
                execution_date=DAY + dt.timedelta(days=1),
            )
        self.assertEqual(len(frame), 24)

    def test_the_data_exporter_template_sets_user_properties(self):
        import os
        from unittest.mock import patch

        import pandas as pd

        from mage_ai.data_preparation.models.constants import BlockType
        from mage_ai.data_preparation.templates.template import fetch_template_source

        code = fetch_template_source(
            BlockType.DATA_EXPORTER, dict(template_path='data_exporters/amplitude.py'),
        )
        io_config = os.path.join(self.repo_path, 'io_config.yaml')
        with open(io_config, 'w') as file:
            file.write(f'version: 0.1.1\ndefault:\n  AMPLITUDE_API_KEY: {API_KEY}\n')
        namespace = dict(data_exporter=lambda function: function)
        with FakeAmplitude() as fake, \
                patch.dict('mage_ai.io.amplitude.UPLOAD_HOSTS', US=fake.url):
            code = code.replace('from mage_ai.settings.repo import get_repo_path\n', '')
            namespace['get_repo_path'] = lambda: self.repo_path
            exec(compile(code, 'amplitude.py', 'exec'), namespace)
            namespace['export_data_to_amplitude'](
                pd.DataFrame({'user_id': ['user-1'], 'churn_risk': [0.7]}),
            )
        self.assertEqual(fake.users, {'user-1': {'churn_risk': 0.7}})



class AmplitudeSourceListingTest(TestCase):
    def test_the_editor_gets_the_sources_readme_and_settings(self):
        from mage_ai.server.api.integration_sources import build_integration_module_info

        info = build_integration_module_info('sources', dict(name='Amplitude'))
        self.assertIn('How events sync', info['docs'])
        self.assertEqual(
            set(info['templates']['config']),
            {'api_key', 'secret_key', 'region', 'start_date', 'cohort_ids'},
        )

    def test_the_editor_gets_the_destinations_readme_and_settings(self):
        from mage_ai.data_integrations.destinations.constants import DESTINATIONS
        from mage_ai.server.api.integration_sources import build_integration_module_info

        self.assertIn('Amplitude', [option['name'] for option in DESTINATIONS])
        info = build_integration_module_info('destinations', dict(name='Amplitude'))
        self.assertIn('Sent once', info['docs'])
        self.assertEqual(set(info['templates']['config']), {'api_key', 'region', 'streams'})

    def test_a_readme_of_another_spelling_keeps_the_templates(self):
        from mage_ai.server.api.integration_sources import build_integration_module_info

        # Airtable's README is Readme.md: on a case-sensitive file system the editor lost
        # its settings template along with its documentation.
        info = build_integration_module_info('destinations', dict(name='Airtable'))
        self.assertIn('Airtable', info['docs'])
        self.assertEqual(set(info['templates']['config']), {'token', 'base_id'})
