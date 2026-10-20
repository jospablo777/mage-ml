"""
Amplitude without an Amplitude account, for the tutorials and the tests.

`mage examples amplitude-server` runs a local server that answers as Amplitude's APIs do,
from their documentation: the Export API (a ZIP of gzipped JSON lines, one file per hour;
404 for no data; 400 past the size limit), the Dashboard REST, Taxonomy, Annotations and
Behavioral Cohorts APIs, Basic authentication, and 429 with Retry-After. It holds a music
app's events of the last days: sign-ups, plays, carts, purchases and subscriptions. It takes
events and user properties on /batch as the Batch Event Upload API does: the API key in the
body, 400 with the fields at fault, insert_id deduplication, and 429 with throttled users;
events it takes are exported like the others.

`mage examples amplitude-webhook` posts events to a stream as Amplitude's Webhook
destination does: one event per request, in Amplitude's event format, some sent again as
Amplitude retries them.

`mage examples amplitude-kafka` writes events to a Kafka topic as Amplitude's Confluent
Kafka REST destination does: each event as JSON, keyed by user_id (device_id without one).
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

# The local server's keys; any other is refused, as Amplitude refuses wrong keys.
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
        # What /batch took: the events, the insert_ids seen, and each user's properties.
        self.uploaded: List[Dict] = []
        self.insert_ids = set()
        self.users: Dict[str, Dict] = {}
        # Users whose events /batch refuses as over the daily quota.
        self.over_quota_users: List[str] = []
        # Called with a line about each request /batch takes; serve() prints it.
        self.on_batch = None
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

    def daily(self, path: str, metric: str, day: dt.datetime):
        """A Dashboard REST metric of a day, from the events."""
        key = day.strftime('%Y-%m-%d')
        today = [e for e in self.events if e['event_time'][:10] == key]
        users = {e['user_id'] for e in today}
        sessions = {(e['user_id'], e['session_id']) for e in today}
        if path == '/api/2/users':
            if metric != 'new':
                return len(users)
            return len({e['user_id'] for e in today if e['event_type'] == 'Sign Up'})
        if path == '/api/2/sessions/peruser':
            return round(len(sessions) / len(users), 3) if users else 0
        lengths = {}
        for e in today:
            moment = dt.datetime.strptime(e['event_time'], '%Y-%m-%d %H:%M:%S.%f')
            first, last = lengths.get((e['user_id'], e['session_id']), (moment, moment))
            lengths[(e['user_id'], e['session_id'])] = (min(first, moment), max(last, moment))
        seconds = [(b - a).total_seconds() for a, b in lengths.values()]
        return round(sum(seconds) / len(seconds), 2) if seconds else 0

    def take_batch(self, body: Dict):
        """The status and answer of the Batch Event Upload API for a request body."""
        if body.get('api_key') != API_KEY:
            return 400, {'code': 400, 'error': 'Invalid API key: ' + str(body.get('api_key'))}
        events = body.get('events')
        if not events:
            return 400, {
                'code': 400, 'error': 'Request missing required field',
                'missing_field': 'events',
            }
        if len(events) > 2000:
            return 400, {'code': 400, 'error': 'Too many events in the request'}
        shortest = int((body.get('options') or {}).get('min_id_length') or 5)
        invalid: Dict[str, List[int]] = {}
        missing: Dict[str, List[int]] = {}
        for index, item in enumerate(events):
            ids = [item.get('user_id'), item.get('device_id')]
            if not any(ids):
                missing.setdefault('user_id', []).append(index)
            for key in ('user_id', 'device_id'):
                if item.get(key) is not None and len(str(item[key])) < shortest:
                    invalid.setdefault(key, []).append(index)
            if not item.get('event_type'):
                missing.setdefault('event_type', []).append(index)
            if 'time' in item and not isinstance(item['time'], int):
                invalid.setdefault('time', []).append(index)
        if invalid or missing:
            return 400, {
                'code': 400, 'error': 'Request missing required field',
                'events_with_invalid_fields': invalid,
                'events_with_missing_fields': missing,
            }
        over = [e.get('user_id') for e in events if e.get('user_id') in self.over_quota_users]
        if over:
            return 429, {
                'code': 429, 'error': 'Too many requests for some devices and users',
                'exceeded_daily_quota_users': {user: 500001 for user in over},
                'exceeded_daily_quota_devices': {},
                'throttled_events': list(range(len(events))),
            }
        now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        repeated = 0
        kinds: Dict[str, int] = {}
        for item in events:
            insert_id = item.get('insert_id')
            if insert_id and insert_id in self.insert_ids:
                repeated += 1
                continue
            kinds[item['event_type']] = kinds.get(item['event_type'], 0) + 1
            if insert_id:
                self.insert_ids.add(insert_id)
            self.uploaded.append(item)
            who = item.get('user_id') or item.get('device_id')
            operations = item.get('user_properties') or {}
            properties = self.users.setdefault(who, {})
            if item['event_type'] == '$identify':
                properties.update(operations.get('$set') or {})
                for key, value in (operations.get('$setOnce') or {}).items():
                    properties.setdefault(key, value)
                for key, value in (operations.get('$add') or {}).items():
                    properties[key] = properties.get(key, 0) + value
                for key in operations.get('$unset') or {}:
                    properties.pop(key, None)
                continue
            properties.update({k: v for k, v in operations.items() if not k.startswith('$')})
            moment = now
            if item.get('time') is not None:
                moment = dt.datetime.fromtimestamp(item['time'] / 1000, dt.timezone.utc)
                moment = moment.replace(tzinfo=None)
            exported = event(900000 + len(self.uploaded), now, item['event_type'])
            exported.update(
                user_id=item.get('user_id'),
                device_id=item.get('device_id'),
                event_properties=item.get('event_properties') or {},
                user_properties=dict(properties),
                event_time=moment.strftime('%Y-%m-%d %H:%M:%S.%f'),
                client_event_time=moment.strftime('%Y-%m-%d %H:%M:%S.%f'),
                country=item.get('country'),
                platform=item.get('platform'),
                session_id=item.get('session_id', -1),
            )
            exported['$insert_id'] = insert_id
            self.events.append(exported)
        if self.on_batch:
            taken = ', '.join(
                f'{count} user property update{"s" * (count != 1)}' if name == '$identify'
                else f'{count} {name}'
                for name, count in sorted(kinds.items())
            ) or 'nothing new'
            dropped = f'; {repeated} already taken (same insert_id)' if repeated else ''
            self.on_batch(f'/batch took {taken}{dropped}.')
        return 200, {
            'code': 200, 'events_ingested': len(events),
            'payload_size_bytes': len(json.dumps(body)),
            'server_upload_time': int(now.timestamp() * 1000),
        }

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

            def do_POST(self):
                path = urlparse(self.path).path
                fake.requests.append(self.path)
                length = int(self.headers.get('Content-Length') or 0)
                if length > 20 * 1024 * 1024:
                    return self.send(413, {'code': 413, 'error': 'Payload too large'})
                if path != '/batch':
                    return self.send(404, 'Not found', 'text/plain')
                try:
                    body = json.loads(self.rfile.read(length) or b'{}')
                except ValueError:
                    return self.send(400, {'code': 400, 'error': 'Invalid JSON request body'})
                if fake.rate_limited.get(path):
                    fake.rate_limited[path] -= 1
                    return self.send(429, {
                        'code': 429, 'error': 'Too many requests for some devices and users',
                        'eps_threshold': 1000, 'throttled_devices': {},
                        'throttled_users': {'user-1': 1500}, 'throttled_events': [0],
                    })
                status, answer = fake.take_batch(body)
                return self.send(status, answer)

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
                if path == '/api/2/taxonomy/event' and fake.events:
                    names = sorted({e['event_type'] for e in fake.events})
                    return self.send(200, {'success': True, 'data': [dict(
                        event_type=name, category={'name': 'Demo'}, description=None,
                        display_name=None, is_active=True, tags=[], owner=None,
                    ) for name in names]})
                if path == '/api/2/taxonomy/event-property' and fake.events:
                    pairs = sorted({
                        (e['event_type'], key, type(value).__name__)
                        for e in fake.events for key, value in e['event_properties'].items()
                    })
                    kinds = dict(str='string', int='number', float='number', bool='boolean')
                    return self.send(200, {'success': True, 'data': [dict(
                        event_type=event_type, event_property=key, type=kinds.get(kind),
                        description=None, regex=None, enum_values=None, is_array_type=False,
                        is_required=False, is_hidden=False, classifications=[],
                    ) for event_type, key, kind in pairs]})
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
                    if fake.events:
                        metric = (query.get('m') or ['active'])[0]
                        return self.send(200, {'data': {
                            'series': [[fake.daily(path, metric, d) for d in days]],
                            'seriesMeta': [{'segmentIndex': 0}],
                            'xValues': [f'{d:%Y-%m-%d}' for d in days],
                        }})
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



EVENT_TYPES = [
    ('Play Song', 60), ('Skip Song', 15), ('Add To Cart', 8), ('Purchase', 4),
    ('Sign Up', 3), ('Subscribe', 2), ('Search', 8),
]
PLANS = ('free', 'free', 'free', 'pro')
COUNTRIES = ('Germany', 'United States', 'Brazil', 'Japan', 'Spain')


def demo_event(rng, index: int, uploaded: dt.datetime) -> Dict:
    """An event of the demo music app, uploaded at the given time (naive UTC)."""
    names = [name for name, _ in EVENT_TYPES]
    weights = [weight for _, weight in EVENT_TYPES]
    event_type = rng.choices(names, weights)[0]
    user = rng.randint(1, 400)
    record = event(index, uploaded, event_type=event_type)
    properties = {
        'Play Song': dict(song=f'song {rng.randint(1, 900)}', seconds=rng.randint(5, 360)),
        'Skip Song': dict(song=f'song {rng.randint(1, 900)}', seconds=rng.randint(1, 30)),
        'Add To Cart': dict(sku=f'merch-{rng.randint(1, 40)}', price=rng.choice([9.5, 19, 35])),
        'Purchase': dict(sku=f'merch-{rng.randint(1, 40)}', price=rng.choice([9.5, 19, 35]),
                         quantity=rng.randint(1, 3)),
        'Sign Up': dict(source=rng.choice(['ad', 'friend', 'search'])),
        'Subscribe': dict(plan='pro', months=rng.choice([1, 12])),
        'Search': dict(query=rng.choice(['jazz', 'lofi', 'rock', 'focus'])),
    }[event_type]
    record.update(
        user_id=f'user-{user}',
        device_id=f'device-{user}',
        amplitude_id=900000000000 + user,
        country=COUNTRIES[user % len(COUNTRIES)],
        city=None,
        event_properties=properties,
        user_properties=dict(plan=PLANS[user % len(PLANS)]),
        session_id=1_700_000_000_000 + user * 1000 + uploaded.hour,
    )
    return record


def demo_events(days: int = 7, now: dt.datetime = None, seed: int = 7) -> List[Dict]:
    """The demo app's events of the last days, busier in the evening (UTC)."""
    import random

    rng = random.Random(seed)
    now = now or dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    first = now.replace(minute=0, second=0, microsecond=0) - dt.timedelta(days=days)
    events, index = [], 0
    hour = first
    while hour < now:
        count = 10 + 30 * (hour.hour >= 17) + rng.randint(0, 10)
        for _ in range(count):
            uploaded = hour + dt.timedelta(seconds=rng.randint(0, 3599))
            if uploaded >= now:
                continue
            events.append(demo_event(rng, index, uploaded))
            index += 1
        hour += dt.timedelta(hours=1)
    return events


def serve(port: int = 18765, days: int = 7, host: str = '127.0.0.1') -> None:
    """Serves the demo app's events of the last days until stopped (Ctrl+C)."""
    events = demo_events(days)
    fake = FakeAmplitude(events)
    fake.on_batch = lambda line: print(line, flush=True)
    fake.server.server_close()
    fake.server = ThreadingHTTPServer((host, port), fake._handler())
    print(f'A local Amplitude on http://{host}:{port} with {len(events):,} events of the '
          f'last {days} days.', flush=True)
    try:
        fake.server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        fake.server.server_close()


def send_webhook_events(
    url: str,
    token: str,
    count: int = 100,
    rate: float = 10,
    retry_share: float = 0.05,
    seed: int = None,
    on_sent=None,
) -> Dict[str, int]:
    """
    Posts events to a stream's messages URL as Amplitude's Webhook destination does: one
    event per request, the configured Authorization header, Amplitude's User-Agent, and a
    share of the events sent again, as Amplitude retries one it got no answer for.
    Returns how many requests were sent and how many were retries.
    """
    import random
    import time
    import urllib.error
    import urllib.request

    rng = random.Random(seed)
    sent = retries = 0
    started = time.monotonic()

    def post(record: Dict) -> None:
        request = urllib.request.Request(url, data=json.dumps(record).encode(), headers={
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'User-Agent': 'Amplitude/Webhook/1.0',
        }, method='POST')
        for attempt in range(5):
            try:
                with urllib.request.urlopen(request, timeout=60):
                    return
            except urllib.error.HTTPError as error:
                if error.code < 500 and error.code != 429:
                    raise
            except OSError:
                pass
            time.sleep(1 + attempt)
        raise OSError(f'{url} did not take the event {record["uuid"]}.')

    for index in range(count):
        now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        record = demo_event(rng, index, now)
        record['uuid'] = f'{rng.getrandbits(128):032x}'
        post(record)
        sent += 1
        if rng.random() < retry_share:
            post(record)
            retries += 1
        if on_sent:
            on_sent(sent)
        if rate:
            ahead = sent / rate - (time.monotonic() - started)
            if ahead > 0:
                time.sleep(ahead)
    return dict(events=sent, retries=retries)


def send_kafka_events(
    bootstrap: str,
    topic: str,
    count: int = 100,
    rate: float = 10,
    compression: str = None,
    seed: int = None,
    on_sent=None,
) -> int:
    """
    Writes events to a topic as Amplitude's Confluent Kafka REST destination does: the
    event as JSON, keyed by user_id, or device_id for an event without one. compression
    (zstd, lz4, gzip) compresses the producer's batches. Returns the events written.
    """
    import random
    import time

    from kafka import KafkaProducer

    rng = random.Random(seed)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        compression_type=compression,
        key_serializer=lambda key: key.encode() if key else None,
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    started = time.monotonic()
    try:
        for index in range(count):
            now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
            record = demo_event(rng, index, now)
            record['uuid'] = f'{rng.getrandbits(128):032x}'
            if rng.random() < 0.1:
                # An anonymous visitor: no user_id, keyed by its device.
                record['user_id'] = None
            producer.send(topic, key=record['user_id'] or record['device_id'], value=record)
            if on_sent:
                on_sent(index + 1)
            if rate:
                ahead = (index + 1) / rate - (time.monotonic() - started)
                if ahead > 0:
                    time.sleep(ahead)
        producer.flush()
    finally:
        producer.close()
    return count
