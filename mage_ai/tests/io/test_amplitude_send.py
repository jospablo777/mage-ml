"""
Sending rows to Amplitude (mage_ai/io/amplitude.py) against a local server that answers as
the Batch Event Upload API does (fixtures_amplitude.py).
"""
import datetime as dt

import pandas as pd
import polars as pl
import requests

from mage_ai.io.amplitude import (
    UPLOAD_THROTTLE_SECONDS,
    Amplitude,
    AmplitudeError,
    amplitude_event,
    event_time_ms,
    upload_chunks,
)
from mage_ai.tests.base_test import TestCase
from mage_ai.tests.io.fixtures_amplitude import API_KEY, FakeAmplitude

JUNE_FIRST_MS = 1717236000000  # 2024-06-01 10:00 UTC


class AmplitudeEventTest(TestCase):
    def test_fields_stay_and_other_columns_become_event_properties(self):
        item = amplitude_event({
            'user_id': 'user-1', 'event_type': 'Purchase', 'time': '2024-06-01T10:00:00Z',
            'price': 9.5, 'country': 'Spain', 'sku': 'merch-1', '_mage_created_at': 'x',
        })
        self.assertEqual(item['time'], JUNE_FIRST_MS)
        self.assertEqual(item['price'], 9.5)
        self.assertEqual(item['country'], 'Spain')
        self.assertEqual(item['event_properties'], {'sku': 'merch-1'})
        self.assertEqual(len(item['insert_id']), 64)

    def test_times_of_every_kind_are_epoch_milliseconds(self):
        moment = dt.datetime(2024, 6, 1, 10, tzinfo=dt.timezone.utc)
        for value in (
            moment, moment.replace(tzinfo=None), pd.Timestamp(moment), '2024-06-01 10:00:00',
            1717236000, 1717236000.0, JUNE_FIRST_MS, str(JUNE_FIRST_MS),
        ):
            self.assertEqual(event_time_ms(value), JUNE_FIRST_MS, value)
        with self.assertRaises(AmplitudeError):
            event_time_ms(True)

    def test_an_exported_event_keeps_its_insert_id_and_event_time(self):
        item = amplitude_event({
            '$insert_id': 'abc', 'user_id': 'user-1', 'event_type': 'Play Song',
            'event_time': '2024-06-01 10:00:00.000000',
        })
        self.assertEqual(item['insert_id'], 'abc')
        self.assertEqual(item['time'], JUNE_FIRST_MS)

    def test_insert_id_follows_the_key_columns(self):
        first = amplitude_event(
            {'user_id': 'user-1', 'order_id': 7, 'status': 'paid'},
            event_type='Order', key_columns=['order_id'],
        )
        changed = amplitude_event(
            {'user_id': 'user-1', 'order_id': 7, 'status': 'refunded'},
            event_type='Order', key_columns=['order_id'],
        )
        other = amplitude_event(
            {'user_id': 'user-1', 'order_id': 8, 'status': 'paid'},
            event_type='Order', key_columns=['order_id'],
        )
        self.assertEqual(first['insert_id'], changed['insert_id'])
        self.assertNotEqual(first['insert_id'], other['insert_id'])

    def test_columns_name_the_fields_and_keys_follow_them(self):
        row = {'customer_id': 'customer-1', 'created_at': '2024-06-01T10:00:00Z', 'amount': 5}
        columns = {'user_id': 'customer_id', 'time': 'created_at'}
        item = amplitude_event(
            row, event_type='Order', key_columns=['customer_id'], columns=columns,
        )
        self.assertEqual(item['user_id'], 'customer-1')
        self.assertEqual(item['time'], JUNE_FIRST_MS)
        self.assertEqual(item['event_properties'], {'amount': 5})
        changed = amplitude_event(
            dict(row, amount=6), event_type='Order', key_columns=['customer_id'],
            columns=columns,
        )
        self.assertEqual(item['insert_id'], changed['insert_id'])

    def test_without_keys_the_same_event_has_the_same_insert_id(self):
        row = {'user_id': 'user-1', 'event_type': 'Play', 'time': JUNE_FIRST_MS, 'song': 'a'}
        self.assertEqual(amplitude_event(row)['insert_id'], amplitude_event(dict(row))['insert_id'])
        self.assertNotEqual(
            amplitude_event(row)['insert_id'],
            amplitude_event(dict(row, song='b'))['insert_id'],
        )

    def test_ids_and_event_type_are_required(self):
        with self.assertRaisesRegex(AmplitudeError, 'user_id or device_id'):
            amplitude_event({'event_type': 'Play'})
        with self.assertRaisesRegex(AmplitudeError, 'event_type'):
            amplitude_event({'user_id': 'user-1'})
        item = amplitude_event({'user_id': 123456, 'event_type': 'x'})
        self.assertEqual(item['user_id'], '123456')

    def test_user_properties_are_set_on_an_identify_event(self):
        item = amplitude_event(
            {'user_id': 'user-1', 'plan': 'pro', 'churn_risk': 0.4, 'country': 'Spain',
             'user_properties': {'$add': {'purchases': 1}}},
            identify=True,
        )
        self.assertEqual(item['event_type'], '$identify')
        self.assertEqual(item['country'], 'Spain')
        self.assertEqual(item['user_properties'], {
            '$add': {'purchases': 1}, '$set': {'plan': 'pro', 'churn_risk': 0.4},
        })
        self.assertNotIn('insert_id', item)

    def test_revenue_and_other_event_fields_are_user_properties_of_a_user(self):
        item = amplitude_event(
            {'user_id': 'user-1', 'revenue': 19.5, 'price': 9.5, 'session_id': 3,
             'platform': 'Web', 'time': JUNE_FIRST_MS},
            identify=True,
        )
        self.assertEqual(item['user_properties'], {
            '$set': {'revenue': 19.5, 'price': 9.5, 'session_id': 3},
        })
        self.assertEqual(item['platform'], 'Web')
        self.assertEqual(item['time'], JUNE_FIRST_MS)

    def test_empty_values_are_left_out_and_a_row_of_them_sends_nothing(self):
        frame = pd.DataFrame({
            'user_id': ['user-1', 'user-2'],
            'score': [0.5, float('nan')],
            'seen': [pd.Timestamp('2024-06-01'), pd.NaT],
        })
        first, second = (amplitude_event(row, identify=True) for row in frame.to_dict('records'))
        self.assertEqual(first['user_properties']['$set'], {
            'score': 0.5, 'seen': '2024-06-01T00:00:00',
        })
        self.assertIsNone(second)

    def test_chunks_hold_at_most_2000_events(self):
        events = ({'user_id': 'user-1', 'event_type': 'x'} for _ in range(4500))
        self.assertEqual([len(chunk) for chunk in upload_chunks(events)], [2000, 2000, 500])


class AmplitudeSendTest(TestCase):
    def client(self, fake, **kwargs):
        sleeps = []
        client = Amplitude(API_KEY, host=fake.url, verbose=False, sleep=sleeps.append, **kwargs)
        client.sleeps = sleeps
        return client

    def test_frames_are_sent_once_however_often(self):
        frame = pd.DataFrame({
            'user_id': ['user-1', 'user-2', 'user-3'],
            'event_type': ['Purchase'] * 3,
            'time': pd.to_datetime(['2024-06-01 10:00', '2024-06-01 11:00', '2024-06-01 12:00']),
            'price': [9.5, 19.0, 35.0],
        })
        with FakeAmplitude() as fake:
            client = self.client(fake)
            self.assertEqual(client.send_events(frame), 3)
            self.assertEqual(client.send_events(pl.from_pandas(frame)), 3)
            self.assertEqual(len(fake.uploaded), 3)
            self.assertEqual(fake.uploaded[0]['time'], JUNE_FIRST_MS)

    def test_user_properties_reach_the_users(self):
        with FakeAmplitude() as fake:
            client = self.client(fake)
            sent = client.export(
                [{'user_id': 'user-1', 'churn_risk': 0.4}, {'user_id': 'user-2', 'plan': None}],
                user_properties=True,
            )
            self.assertEqual(sent, 1)
            self.assertEqual(fake.users, {'user-1': {'churn_risk': 0.4}})

    def test_sending_needs_no_secret_key_and_reading_does(self):
        with FakeAmplitude() as fake:
            client = self.client(fake)
            client.check_api_key()
            with self.assertRaisesRegex(AmplitudeError, 'secret key'):
                client.event_types()

    def test_a_key_from_an_unset_variable_is_named_as_such(self):
        # "{{ env_var('AMPLITUDE_API_KEY') }}" renders as None when the variable is unset.
        with self.assertRaisesRegex(AmplitudeError, 'api_key rendered as None.*not set'):
            Amplitude('None', verbose=False)
        with self.assertRaisesRegex(AmplitudeError, 'secret_key rendered as None'):
            Amplitude('key', 'None', verbose=False)

    def test_a_wrong_key_is_named_with_the_region(self):
        with FakeAmplitude() as fake:
            client = Amplitude('wrong-key', host=fake.url, verbose=False)
            with self.assertRaisesRegex(AmplitudeError, 'API key.*region EU'):
                client.check_api_key()
            with self.assertRaisesRegex(AmplitudeError, 'API key'):
                client.send_events([{'user_id': 'user-1', 'event_type': 'x'}])

    def test_throttling_waits_as_amplitude_asks(self):
        with FakeAmplitude() as fake:
            fake.rate_limited['/batch'] = 2
            client = self.client(fake)
            self.assertEqual(client.send_events([{'user_id': 'user-1', 'event_type': 'x'}]), 1)
            self.assertEqual(len(client.sleeps), 2)
            self.assertTrue(all(delay >= UPLOAD_THROTTLE_SECONDS for delay in client.sleeps))

    def test_users_over_the_daily_quota_stop_the_send(self):
        with FakeAmplitude() as fake:
            fake.over_quota_users = ['user-9']
            with self.assertRaisesRegex(AmplitudeError, '500,000.*user-9'):
                self.client(fake).send_events([{'user_id': 'user-9', 'event_type': 'x'}])
            self.assertEqual(fake.uploaded, [])

    def test_a_refused_event_is_explained(self):
        with FakeAmplitude() as fake:
            client = self.client(fake)
            with self.assertRaisesRegex(AmplitudeError, 'user_id invalid in 1 of 2.*min_id_length'):
                client.send_events([
                    {'user_id': 'user-1', 'event_type': 'x'}, {'user_id': 'u1', 'event_type': 'y'},
                ])
            self.assertEqual(client.send_events(
                [{'user_id': 'u1', 'event_type': 'y'}], min_id_length=2,
            ), 1)

    def test_a_request_too_large_is_split(self):
        with FakeAmplitude() as fake:
            client = self.client(fake)
            original = client.session.post
            sizes = []

            def post(url, json=None, **kwargs):
                sizes.append(len(json['events']))
                if len(json['events']) > 5:
                    response = requests.Response()
                    response.status_code = 413
                    response._content = b'{"code": 413, "error": "Payload too large"}'
                    return response
                return original(url, json=json, **kwargs)

            client.session.post = post
            events = [{'user_id': f'user-{n}', 'event_type': 'x'} for n in range(10)]
            self.assertEqual(client.send_events(events), 10)
            self.assertEqual(sizes, [10, 5, 5])
            self.assertEqual(len(fake.uploaded), 10)
