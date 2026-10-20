"""
A local server that answers as Amplitude's APIs do, from their documentation: the Export
API (a ZIP of gzipped JSON lines, one file per hour; 404 for no data; 400 past the size
limit), the Dashboard REST, Taxonomy, Annotations and Behavioral Cohorts APIs, Basic
authentication, and 429 with Retry-After. Tests use it through the client's `host`.
"""
import base64
import datetime as dt
import gzip
import io
import json
import threading
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List
from urllib.parse import parse_qs, urlparse

API_KEY = 'test-api-key'
SECRET_KEY = 'test-secret-key'  # pragma: allowlist secret
PROJECT = 123456


def event(index: int, uploaded: dt.datetime, event_type: str = 'Play Song') -> Dict:
    """An event as the Export API writes it."""
    at = uploaded.strftime('%Y-%m-%d %H:%M:%S.%f')
    return {
        '$insert_id': f'insert-{index}',
        'amplitude_id': 900000000000 + index,
        'app': PROJECT,
        'city': 'Zürich',
        'client_event_time': at,
        'client_upload_time': at,
        'country': 'Switzerland',
        'data': {'path': '/song'},
        'device_id': f'device-{index % 7}',
        'event_id': index,
        'event_properties': {
            'song': f'song {index}', 'seconds': index % 300, 'liked': index % 2 == 0,
        },
        'event_time': at,
        'event_type': event_type,
        'groups': {},
        'group_properties': {},
        'ip_address': '127.0.0.1',
        'paying': None,
        'platform': 'Web',
        'processed_time': at,
        'server_received_time': at,
        'server_upload_time': at,
        'session_id': 1717200000000 + index,
        'user_id': f'user-{index % 11}',
        'user_properties': {'plan': 'pro' if index % 3 else 'free'},
        'uuid': f'00000000-0000-0000-0000-{index:012d}',
    }


class FakeAmplitude:
    def __init__(self, events: List[Dict] = None, max_hours_per_export: int = None):
        self.events = list(events or [])
        # A request for more hours than this answers 400, as an export past 4 GB does.
        self.max_hours_per_export = max_hours_per_export
        # Requests to answer with 429 before serving, per path.
        self.rate_limited: Dict[str, int] = {}
        self.requests: List[str] = []
        self.cohort_polls = 0
        self.cohort_csv = 'amplitude_id,user_id,plan\n111,user-1,pro\n222,,free\n'
        self.truncate_next_export = False
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), self._handler())
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f'http://127.0.0.1:{self.server.server_address[1]}'

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.server.shutdown()
        self.server.server_close()

    def export_zip(self, start: dt.datetime, end: dt.datetime) -> bytes:
        hours: Dict[dt.datetime, List[Dict]] = {}
        for item in self.events:
            uploaded = dt.datetime.strptime(item['server_upload_time'], '%Y-%m-%d %H:%M:%S.%f')
            hour = uploaded.replace(minute=0, second=0, microsecond=0)
            if start <= hour <= end:
                hours.setdefault(hour, []).append(item)
        if not hours:
            return b''
        body = io.BytesIO()
        with zipfile.ZipFile(body, 'w') as archive:
            for hour, items in sorted(hours.items()):
                lines = ''.join(json.dumps(item) + '\n' for item in items).encode()
                name = f'{PROJECT}/{PROJECT}_{hour:%Y-%m-%d}_{hour.hour}#0.json.gz'
                archive.writestr(name, gzip.compress(lines))
        return body.getvalue()

    def _handler(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def send(self, status: int, body=b'', content_type='application/json', headers=None):
                if isinstance(body, (dict, list)):
                    body = json.dumps(body).encode()
                elif isinstance(body, str):
                    body = body.encode()
                self.send_response(status)
                self.send_header('Content-Type', content_type)
                length = len(body)
                if fake.truncate_next_export and content_type == 'application/zip':
                    fake.truncate_next_export = False
                    length += 100
                self.send_header('Content-Length', str(length))
                for key, value in (headers or {}).items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                parsed = urlparse(self.path)
                path, query = parsed.path, parse_qs(parsed.query)
                fake.requests.append(self.path)
                expected = base64.b64encode(f'{API_KEY}:{SECRET_KEY}'.encode()).decode()
                if self.headers.get('Authorization') != f'Basic {expected}':
                    return self.send(403, 'Invalid API key or secret key', 'text/plain')
                if fake.rate_limited.get(path):
                    fake.rate_limited[path] -= 1
                    return self.send(429, 'Too many requests', 'text/plain', {'Retry-After': '0'})
                if path == '/api/2/export':
                    try:
                        start = dt.datetime.strptime(query['start'][0], '%Y%m%dT%H')
                        end = dt.datetime.strptime(query['end'][0], '%Y%m%dT%H')
                    except (KeyError, ValueError):
                        return self.send(400, 'Invalid start or end', 'text/plain')
                    hours = int((end - start).total_seconds() // 3600) + 1
                    if fake.max_hours_per_export and hours > fake.max_hours_per_export:
                        return self.send(
                            400, 'Raw data files were too large: export size exceeded',
                            'text/plain',
                        )
                    body = fake.export_zip(start, end)
                    if not body:
                        return self.send(404, 'Raw data files were not found.', 'text/plain')
                    return self.send(200, body, 'application/zip')
                if path == '/api/2/taxonomy/category':
                    return self.send(200, {'success': True, 'data': [{'id': 1, 'name': 'Music'}]})
                if path == '/api/2/taxonomy/event':
                    return self.send(200, {'success': True, 'data': [{
                        'event_type': 'Play Song', 'category': {'name': 'Music'},
                        'description': 'A song started', 'display_name': None, 'is_active': True,
                        'is_hidden_from_dropdowns': False, 'is_hidden_from_persona_results': False,
                        'is_hidden_from_pathfinder': False, 'is_hidden_from_timeline': False,
                        'tags': [], 'owner': None,
                    }]})
                if path == '/api/2/taxonomy/event-property':
                    return self.send(200, {'success': True, 'data': [{
                        'event_property': 'song', 'event_type': 'Play Song', 'description': None,
                        'type': 'string', 'regex': None, 'enum_values': None,
                        'is_array_type': False, 'is_required': True, 'is_hidden': False,
                        'classifications': [],
                    }]})
                if path == '/api/2/taxonomy/user-property':
                    return self.send(200, {'success': True, 'data': [{
                        'user_property': 'plan', 'description': None, 'type': 'enum',
                        'regex': None, 'enum_values': 'free,pro', 'is_array_type': False,
                        'is_hidden': False, 'classifications': [], 'deleted': False,
                    }]})
                if path == '/api/3/annotations':
                    return self.send(200, {'data': [{
                        'id': 7, 'start': '2024-06-01T07:00:00+00:00', 'end': None,
                        'label': 'Release 2.0', 'details': 'New player',
                        'category': {'id': 1, 'name': 'Release'},
                        'chart_id': None,
                    }]})
                if path == '/api/3/cohorts':
                    return self.send(200, {'cohorts': [{
                        'id': 'abc123', 'appId': PROJECT, 'name': 'Power listeners', 'size': 2,
                        'archived': False, 'owners': ['me@example.com'], 'lastComputed': 1717200000,
                    }]})
                if path.startswith('/api/5/cohorts/request-status/'):
                    fake.cohort_polls += 1
                    if fake.cohort_polls < 2:
                        return self.send(
                            202, {'request_id': 'r1', 'async_status': 'JOB INPROGRESS'},
                        )
                    return self.send(200, {'request_id': 'r1', 'async_status': 'JOB COMPLETED'})
                if path.endswith('/file') and path.startswith('/api/5/cohorts/request/'):
                    return self.send(200, fake.cohort_csv, 'text/csv')
                if path.startswith('/api/5/cohorts/request/'):
                    return self.send(202, {'request_id': 'r1', 'cohort_id': path.rsplit('/', 1)[1]})
                if path in ('/api/2/users', '/api/2/sessions/average', '/api/2/sessions/peruser'):
                    start = dt.datetime.strptime(query['start'][0], '%Y%m%d')
                    end = dt.datetime.strptime(query['end'][0], '%Y%m%d')
                    days = [start + dt.timedelta(days=d) for d in range((end - start).days + 1)]
                    base = {'/api/2/users': 100, '/api/2/sessions/average': 60.5,
                            '/api/2/sessions/peruser': 2.5}[path]
                    if (query.get('m') or ['active'])[0] == 'new':
                        base = 10
                    return self.send(200, {'data': {
                        'series': [[base + d.day for d in days]],
                        'seriesMeta': [{'segmentIndex': 0}],
                        'xValues': [f'{d:%Y-%m-%d}' for d in days],
                    }})
                return self.send(404, 'Not found', 'text/plain')

        return Handler


def serve(port: int) -> None:
    """Serves events of the last two days on a fixed port, for the editor's browser tests."""
    now = dt.datetime.now(dt.timezone.utc).replace(
        minute=0, second=0, microsecond=0, tzinfo=None,
    )
    start = now - dt.timedelta(hours=48)
    events = [
        event(h * 100 + n, start + dt.timedelta(hours=h, minutes=n),
              event_type=['Sign Up', 'Add To Cart', 'Purchase'][n % 3])
        for h in range(44) for n in range(5)
    ]
    fake = FakeAmplitude(events)
    fake.server.server_close()
    fake.server = ThreadingHTTPServer(('127.0.0.1', port), fake._handler())
    print(f'Amplitude on {port} with {len(events)} events', flush=True)
    fake.server.serve_forever()


if __name__ == '__main__':
    import sys

    serve(int(sys.argv[1]))
