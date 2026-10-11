"""
The Feast REST client against a fake feature server that records each request and
answers as Feast does. integration_tests/feast runs the client against a real Feast.
"""
import datetime as dt
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pandas as pd
import polars as pl
import pyarrow as pa

from mage_ai.feature_stores.feast import EXACT_FLOAT_INTEGER, FeastClient, FeastError

UTC = dt.timezone.utc
BIG = EXACT_FLOAT_INTEGER + 1


class FakeFeast:
    """Records requests; `answers` holds (status, body) per request, then 200 {}."""

    def __init__(self):
        self.requests = []
        self.answers = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _answer(self):
                length = int(self.headers.get('Content-Length') or 0)
                body = json.loads(self.rfile.read(length)) if length else None
                owner.requests.append(dict(
                    method=self.command, path=self.path, body=body,
                    authorization=self.headers.get('Authorization'),
                ))
                status, payload = owner.answers.pop(0) if owner.answers else (200, {})
                data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
                self.send_response(status)
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = _answer
            do_POST = _answer

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.url = f'http://127.0.0.1:{self.server.server_address[1]}'
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class FeastClientTest(unittest.TestCase):
    def setUp(self):
        self.feast = FakeFeast()
        self.addCleanup(self.feast.close)
        self.client = FeastClient(self.feast.url, backoff=0)
        self.addCleanup(self.client.close)

    def pushed(self):
        return [r['body']['df'] for r in self.feast.requests if r['path'] == '/push']

    def test_rows_go_in_event_time_order_so_the_latest_event_is_written_last(self):
        frame = pd.DataFrame({
            'driver_id': [1, 1, 2],
            'conv_rate': [0.3, 0.1, 0.5],
            'event_timestamp': pd.to_datetime([
                '2025-01-03T00:00:00Z', '2025-01-01T00:00:00Z', '2025-01-02T00:00:00Z',
            ]),
        })
        self.assertEqual(self.client.push('driver_push', frame), 1)
        (rows,) = self.pushed()
        self.assertEqual(rows['conv_rate'], [0.1, 0.5, 0.3])
        self.assertEqual(rows['event_timestamp'][0], '2025-01-01T00:00:00+00:00')
        body = self.feast.requests[0]['body']
        self.assertEqual((body['push_source_name'], body['to']), ('driver_push', 'online'))

    def test_large_integers_next_to_nulls_go_in_requests_feast_keeps_exact(self):
        frame = pa.table({
            'driver_id': [1, 2, 3, 4],
            'trips': pa.array([BIG, None, BIG + 2, None], pa.int64()),
            'small': pa.array([1, None, 3, 4], pa.int64()),
            'event_timestamp': pa.array([
                dt.datetime(2025, 1, d, tzinfo=UTC) for d in (1, 2, 3, 4)
            ], pa.timestamp('us', tz='UTC')),
        })
        self.client.push('driver_push', frame)
        requests = self.pushed()
        # One request per run of rows where trips is complete or all NULL, in event order.
        self.assertEqual([r['trips'] for r in requests], [[BIG], [None], [BIG + 2], [None]])
        for rows in requests:
            self.assertTrue(all(v is None for v in rows['trips'])
                            or all(v is not None for v in rows['trips']))
        # Small integers with NULLs are exact as float64, so they do not split requests.
        self.assertEqual([v for r in requests for v in r['small']], [1, None, 3, 4])

    def test_rows_without_large_integers_go_in_batches(self):
        frame = pl.DataFrame({
            'driver_id': list(range(5)),
            'trips': [1, None, 3, None, 5],
            'event_timestamp': [dt.datetime(2025, 1, 1, tzinfo=UTC)] * 5,
        })
        self.assertEqual(self.client.push('driver_push', frame, batch_size=2), 3)
        self.assertEqual([r['driver_id'] for r in self.pushed()], [[0, 1], [2, 3], [4]])

    def test_timestamps_feast_would_misread_are_refused(self):
        naive = pd.DataFrame({'driver_id': [1], 'event_timestamp': [dt.datetime(2025, 1, 1)]})
        with self.assertRaisesRegex(FeastError, 'no time zone'):
            self.client.push('driver_push', naive)
        epoch = pd.DataFrame({'driver_id': [1], 'event_timestamp': [1735689600]})
        with self.assertRaisesRegex(FeastError, 'nanoseconds'):
            self.client.push('driver_push', epoch)
        with self.assertRaisesRegex(FeastError, "no 'ts' column"):
            self.client.push('driver_push', naive, timestamp_column='ts')
        self.assertEqual(self.feast.requests, [])

        self.client.push('driver_push', naive, assume_utc=True)
        self.assertEqual(self.pushed()[0]['event_timestamp'], ['2025-01-01T00:00:00+00:00'])

    def test_values_json_cannot_carry_exactly_are_refused(self):
        import decimal

        when = [dt.datetime(2025, 1, 1, tzinfo=UTC)]
        with self.assertRaisesRegex(FeastError, 'decimals'):
            self.client.push('p', pd.DataFrame({
                'price': [decimal.Decimal('1.10')], 'event_timestamp': when,
            }))
        with self.assertRaisesRegex(FeastError, 'inf'):
            self.client.push('p', pd.DataFrame({
                'rate': [float('inf')], 'event_timestamp': when,
            }))
        self.client.push('p', pd.DataFrame({'rate': [float('nan')], 'event_timestamp': when}))
        self.assertEqual(self.pushed()[0]['rate'], [None])

    def test_transient_failures_are_retried_and_feast_errors_are_not(self):
        when = [dt.datetime(2025, 1, 1, tzinfo=UTC)]
        frame = pd.DataFrame({'driver_id': [1], 'event_timestamp': when})
        self.feast.answers = [(503, {}), (502, {}), (200, {})]
        self.client.push('driver_push', frame)
        self.assertEqual(len(self.feast.requests), 3)

        self.feast.requests.clear()
        self.feast.answers = [(500, b'Feature view driver_stats does not exist')]
        with self.assertRaises(FeastError) as raised:
            self.client.push('driver_push', frame)
        self.assertEqual(len(self.feast.requests), 1)
        self.assertEqual((raised.exception.status, raised.exception.route), (500, '/push'))
        self.assertIn('driver_stats does not exist', str(raised.exception))

        self.feast.answers = [(503, {})] * 4
        with self.assertRaisesRegex(FeastError, '503'):
            self.client.push('driver_push', frame)

    def test_an_unreachable_server_fails_with_a_clear_error(self):
        client = FeastClient('http://127.0.0.1:9', retries=1, backoff=0, timeout=2)
        with self.assertRaisesRegex(FeastError, 'did not answer after 2 attempts'):
            client.get_online_features({'driver_id': [1]}, features=['v:f'])
        self.assertFalse(client.health())

    def test_online_features_map_by_name_and_keep_integers_exact(self):
        self.feast.answers = [(200, {
            'metadata': {'feature_names': ['driver_id', 'trips', 'city']},
            'results': [
                {'values': [1, 2, 3], 'statuses': ['PRESENT'] * 3,
                 'event_timestamps': ['1970-01-01T00:00:00Z'] * 3},
                {'values': [BIG, None, 5], 'statuses': ['PRESENT', 'NULL_VALUE', 'PRESENT'],
                 'event_timestamps': ['2025-01-01T00:00:00Z'] * 3},
                {'values': ['a', 'stale', 'c'], 'statuses': ['PRESENT', 'NOT_FOUND', 'PRESENT'],
                 'event_timestamps': ['2025-01-01T00:00:00Z'] * 3},
            ],
        })] * 3
        request = dict(entities={'driver_id': [1, 2, 3]},
                       features=['stats:city', 'stats:trips'])

        frame = self.client.get_online_features(**request)
        self.assertEqual(list(frame.columns), ['driver_id', 'trips', 'city'])
        self.assertEqual(frame['trips'].tolist()[0], BIG)
        self.assertTrue(pd.isna(frame['trips'].tolist()[1]))
        self.assertTrue(pd.isna(frame['city'].tolist()[1]), 'NOT_FOUND is NULL')

        polars = self.client.get_online_features(**request, output='polars', with_metadata=True)
        self.assertEqual(polars['trips'].to_list(), [BIG, None, 5])
        self.assertEqual(polars['city__status'].to_list(), ['PRESENT', 'NOT_FOUND', 'PRESENT'])
        self.assertEqual(polars['city__event_timestamp'][0], dt.datetime(2025, 1, 1, tzinfo=UTC))
        self.assertNotIn('driver_id__status', polars.columns)

        self.client.get_online_features(**request, output='dict')
        body = self.feast.requests[-1]['body']
        self.assertEqual(body['features'], ['stats:city', 'stats:trips'])
        self.assertEqual(body['entities'], {'driver_id': [1, 2, 3]})

    def test_online_features_take_entities_as_a_frame_and_a_feature_service(self):
        self.feast.answers = [(200, {'metadata': {'feature_names': []}, 'results': []})]
        self.client.get_online_features(pl.DataFrame({'driver_id': [7, 8]}),
                                        feature_service='driver_activity', output='dict')
        body = self.feast.requests[0]['body']
        self.assertEqual(body, dict(entities={'driver_id': [7, 8]}, full_feature_names=False,
                                    feature_service='driver_activity'))
        with self.assertRaisesRegex(FeastError, 'either features or feature_service'):
            self.client.get_online_features({'driver_id': [1]})

    def test_token_and_url_come_from_the_environment(self):
        from unittest.mock import patch

        with patch.dict('os.environ', {'FEAST_URL': self.feast.url, 'FEAST_TOKEN': 's3cret'}):
            client = FeastClient(backoff=0)
        self.assertTrue(client.health())
        self.assertEqual(self.feast.requests[-1]['authorization'], 'Bearer s3cret')
        with patch.dict('os.environ', {}, clear=True):
            with self.assertRaisesRegex(FeastError, 'FEAST_URL'):
                FeastClient()

    def test_materialize_sends_the_range_with_offsets(self):
        self.client.materialize(dt.datetime(2025, 1, 1, tzinfo=UTC),
                                dt.datetime(2025, 1, 2, tzinfo=UTC), ['driver_hourly_stats'])
        self.client.materialize_incremental(dt.datetime(2025, 1, 3, tzinfo=UTC))
        first, second = (r['body'] for r in self.feast.requests)
        self.assertEqual(first, dict(start_ts='2025-01-01T00:00:00+00:00',
                                     end_ts='2025-01-02T00:00:00+00:00',
                                     feature_views=['driver_hourly_stats']))
        self.assertEqual(second, dict(end_ts='2025-01-03T00:00:00+00:00'))
        self.assertEqual(self.feast.requests[1]['path'], '/materialize-incremental')
        with self.assertRaisesRegex(FeastError, 'without a time zone'):
            self.client.materialize(dt.datetime(2025, 1, 1), dt.datetime(2025, 1, 2))
