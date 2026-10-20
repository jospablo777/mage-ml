"""
The Amplitude destination of data integration pipelines against a local server that
answers as the Batch Event Upload API does (mage_ai.tests.io.fixtures_amplitude.py).
"""
import io
import json
import os
import tempfile
import unittest

from mage_ai.io.amplitude import AmplitudeError
from mage_ai.tests.io.fixtures_amplitude import API_KEY, FakeAmplitude
from mage_integrations.destinations.amplitude import Amplitude

ORDERS = dict(
    schema=dict(type='object', properties=dict(
        order_id=dict(type=['integer']),
        customer_id=dict(type=['string']),
        created_at=dict(type=['string'], format='date-time'),
        amount=dict(type=['null', 'number']),
        status=dict(type=['null', 'string']),
    )),
    key_properties=['order_id'],
)
STREAMS = {
    'orders': {'event_type': 'Order Completed', 'time': 'created_at', 'user_id': 'customer_id'},
}
SCORES = dict(
    schema=dict(type='object', properties=dict(
        user_id=dict(type=['string']),
        churn_risk=dict(type=['null', 'number']),
        segment=dict(type=['null', 'string']),
    )),
    key_properties=['user_id'],
)


def messages(stream: str, spec: dict, records: list, bookmark=None) -> str:
    lines = [dict(type='SCHEMA', stream=stream, **spec)]
    lines += [dict(type='RECORD', stream=stream, record=record) for record in records]
    if bookmark:
        lines.append(dict(type='STATE', value=dict(bookmarks={stream: bookmark})))
    return ''.join(json.dumps(line) + '\n' for line in lines)


def sync(fake, text: str, **config):
    """Runs the destination on Singer messages; returns the last state it saved."""
    with tempfile.TemporaryDirectory() as directory:
        state_path = os.path.join(directory, 'state.json')
        destination = Amplitude(
            config=dict(api_key=API_KEY, host=fake.url, **config),
            batch_processing=True,
            state_file_path=state_path,
        )
        destination.process(io.BytesIO(text.encode()))
        if not os.path.exists(state_path):
            return None
        with open(state_path) as f:
            return json.loads(f.read())


def order(order_id: int, status='paid'):
    return dict(
        order_id=order_id, customer_id=f'customer-{order_id}', created_at='2024-06-01T10:00:00+00:00',
        amount=19.5, status=status,
    )


class AmplitudeDestinationTest(unittest.TestCase):
    def test_rows_become_events_once_however_often_the_sync_runs(self):
        with FakeAmplitude() as fake:
            text = messages('orders', ORDERS, [order(n) for n in range(1, 4)], {'order_id': 3})
            state = sync(fake, text, streams=STREAMS)
            self.assertEqual(state, {'bookmarks': {'orders': {'order_id': 3}}})
            self.assertEqual(len(fake.uploaded), 3)
            first = fake.uploaded[0]
            self.assertEqual(first['event_type'], 'Order Completed')
            self.assertEqual(first['time'], 1717236000000)
            self.assertEqual(first['user_id'], 'customer-1')
            self.assertEqual(first['event_properties'], {
                'order_id': 1, 'amount': 19.5, 'status': 'paid',
            })

            # The same orders again, one changed: keyed by order_id, nothing is added.
            again = [order(1, status='refunded'), order(2), order(3)]
            sync(fake, messages('orders', ORDERS, again), streams=STREAMS)
            self.assertEqual(len(fake.uploaded), 3)

    def test_user_property_streams_set_properties(self):
        with FakeAmplitude() as fake:
            rows = [
                dict(user_id='user-1', churn_risk=0.8, segment='at risk'),
                dict(user_id='user-2', churn_risk=0.1, segment=None),
            ]
            sync(fake, messages('scores', SCORES, rows), streams={'scores': {'user_properties': True}})
            self.assertEqual(fake.users, {
                'user-1': {'churn_risk': 0.8, 'segment': 'at risk'},
                'user-2': {'churn_risk': 0.1},
            })

    def test_a_stream_without_event_type_says_what_to_set(self):
        with FakeAmplitude() as fake:
            with self.assertRaisesRegex(Exception, 'event_type: Order Completed.*user_properties: true'):
                sync(fake, messages('orders', ORDERS, [order(1)], {'order_id': 1}))
            self.assertEqual(fake.uploaded, [])

    def test_the_connection_test_checks_the_key(self):
        with FakeAmplitude() as fake:
            Amplitude(config=dict(api_key=API_KEY, host=fake.url)).test_connection()
            with self.assertRaisesRegex(AmplitudeError, 'API key'):
                Amplitude(config=dict(api_key='wrong-key', host=fake.url)).test_connection()


if __name__ == '__main__':
    unittest.main()
