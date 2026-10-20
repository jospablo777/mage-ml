"""
The Amplitude of the tutorials (mage_ai/examples/amplitude.py): the demo app's events
read through the client as from Amplitude, its daily metrics computed from them, and
webhook events sent as Amplitude's Webhook destination sends them.
"""
import datetime as dt
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from typer.testing import CliRunner

from mage_ai.examples import amplitude
from mage_ai.io.amplitude import Amplitude
from mage_ai.tests.base_test import TestCase

NOW = dt.datetime(2026, 10, 20, 12, 30)


class DemoAmplitudeTest(TestCase):
    def test_the_demo_apps_events_read_as_from_amplitude(self):
        events = amplitude.demo_events(days=2, now=NOW)
        with amplitude.FakeAmplitude(events) as fake:
            client = Amplitude(amplitude.API_KEY, amplitude.SECRET_KEY, host=fake.url,
                               verbose=False)
            frame = client.load_events(NOW - dt.timedelta(days=2), NOW - dt.timedelta(hours=3))
            users = client.active_users(NOW - dt.timedelta(days=1), NOW)
        self.assertGreater(len(frame), 500)
        self.assertIn('Purchase', set(frame['event_type']))
        purchase = frame[frame['event_type'] == 'Purchase'].iloc[0]
        self.assertIn('price', purchase['event_properties'])
        # Active users of a day are its distinct users.
        day = (NOW - dt.timedelta(days=1)).strftime('%Y-%m-%d')
        expected = len({e['user_id'] for e in events if e['event_time'][:10] == day})
        self.assertEqual(users['value'].tolist()[0], expected)

    def test_events_are_the_same_for_the_same_seed(self):
        first = amplitude.demo_events(days=1, now=NOW)
        self.assertEqual(first, amplitude.demo_events(days=1, now=NOW))


class WebhookSenderTest(TestCase):
    def test_events_go_one_per_request_and_some_twice(self):
        received = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                received.append((dict(self.headers), json.loads(body)))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"accepted": 1}')

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = f'http://127.0.0.1:{server.server_address[1]}/v1/streams/p/messages'

        sent = amplitude.send_webhook_events(url, 'ingest', count=40, rate=0,
                                             retry_share=0.25, seed=3)

        self.assertEqual(len(received), sent['events'] + sent['retries'])
        self.assertEqual(sent['events'], 40)
        self.assertGreater(sent['retries'], 0)
        headers, first = received[0]
        self.assertEqual(headers['Authorization'], 'Bearer ingest')
        self.assertEqual(headers['User-Agent'], 'Amplitude/Webhook/1.0')
        # One event per request, in Amplitude's format.
        self.assertIsInstance(first, dict)
        self.assertTrue({'uuid', 'event_type', 'event_time', 'user_id'} <= set(first))
        uuids = [body['uuid'] for _, body in received]
        self.assertEqual(len(set(uuids)), 40)

    def test_the_command_needs_a_stream_or_a_service(self):
        from mage_ai.cli.main import app

        result = CliRunner().invoke(app, ['examples', 'amplitude-webhook', '--count', '1'])
        self.assertEqual(result.exit_code, 1)
        self.assertIn('--pipeline', result.output)
