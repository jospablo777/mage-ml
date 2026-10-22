"""
The Rust gateway in front of MageML's web server (MAGE_GATEWAY=on, ADR-0007), with
`mage start` as a user runs it: each GET of the recorded API without path parameters
answers the same through the gateway as from the server, the editor's pages and a
terminal WebSocket pass, and the gateway adds under 2 ms at p95.
"""
import json
import os
import re
import socket
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
import requests

SPEC = Path(__file__).resolve().parents[2] / 'contracts' / 'openapi.json'
COUNTS = ('equal', 'equal_after_reread', 'different', 'failed')
# Answers that hold the time or a counter, so two calls in a row differ without a gateway.
VOLATILE = re.compile(r'\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?|\b\d{10,}(\.\d+)?\b')


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


@pytest.fixture(scope='module')
def ports(tmp_path_factory):
    """The gateway's port and the server's, for a `mage start` with MAGE_GATEWAY=on."""
    from mage_ai.pipeline_services.engine import EngineNotFound, engine_path

    try:
        engine = engine_path()
    except EngineNotFound as error:
        pytest.skip(str(error))
    folder = tmp_path_factory.mktemp('gateway')
    project = folder / 'project'
    port = free_port()
    env = dict(
        os.environ, MAGE_GATEWAY='on', MAGE_ENGINE_PATH=str(engine),
        DISABLE_AUTO_BROWSER_OPEN='true', MAGE_DATA_DIR=str(folder / 'data'),
    )
    # From the checkout, MageML would use the development database in
    # mage_ai/orchestration/db and find the runs of earlier test sessions there.
    process = subprocess.Popen(
        [sys.executable, '-c', 'from mage_ai.cli.main import app; app()', 'start',
         str(project), '--port', str(port)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env, cwd=folder,
    )
    lines = []
    threading.Thread(target=lambda: lines.extend(process.stdout), daemon=True).start()
    pattern = re.compile(rf'serves port {port} for the server on (\d+)')
    deadline = time.monotonic() + 180
    upstream = None
    while upstream is None:
        assert process.poll() is None, ''.join(lines)
        assert time.monotonic() < deadline, ''.join(lines)
        found = [pattern.search(line) for line in list(lines)]
        upstream = next((int(m.group(1)) for m in found if m), None)
        time.sleep(0.2)
    yield port, upstream
    process.terminate()
    process.wait(timeout=30)


@pytest.fixture
def signed_in(ports):
    """A session signed in as the default owner, through the gateway."""
    from mage_ai.authentication.oauth2 import decode_token
    from mage_ai.settings.server import OAUTH2_APPLICATION_CLIENT_ID

    gateway, _ = ports
    session = requests.Session()
    session.headers['X-API-KEY'] = OAUTH2_APPLICATION_CLIENT_ID
    response = session.post(
        f'http://127.0.0.1:{gateway}/api/sessions',
        json=dict(session=dict(email='admin@admin.com', password='admin')), timeout=60,
    )
    response.raise_for_status()
    token = response.json()['session']['token']
    session.cookies.set('oauth_token', token)
    session.token = token
    # The editor's terminal sends the access token inside the session token.
    session.access_token = decode_token(token)['token']
    return session


def normalized(response):
    text = response.text
    try:
        text = json.dumps(json.loads(text), sort_keys=True)
    except ValueError:
        pass
    return response.status_code, VOLATILE.sub('<time>', text)


def test_every_recorded_get_answers_the_same_through_the_gateway(ports, signed_in):
    gateway, server = ports
    spec = json.loads(SPEC.read_text())
    paths = sorted(p for p, ops in spec['paths'].items() if 'get' in ops and '{' not in p)
    assert len(paths) > 50
    session = signed_in
    differences, statuses = [], []
    for path in paths + ['/', '/pipelines', '/api/status']:
        direct = normalized(session.get(f'http://127.0.0.1:{server}{path}', timeout=60))
        through = normalized(session.get(f'http://127.0.0.1:{gateway}{path}', timeout=60))
        statuses.append(direct[0])
        if direct != through:
            # A second read tells a gateway difference from an answer that changes by itself.
            again = normalized(session.get(f'http://127.0.0.1:{server}{path}', timeout=60))
            if again == direct:
                differences.append((path, direct[0], through[0], through[1][:300]))
    assert differences == []
    # Signed in, the answers hold data: not the same refusal twice.
    assert statuses.count(200) > len(statuses) * 0.7, statuses


def test_a_terminal_websocket_passes(ports, signed_in):
    """A shell answers a command through the gateway as it does from the server."""
    import asyncio

    from tornado.httpclient import HTTPRequest
    from tornado.websocket import websocket_connect

    from mage_ai.settings.server import OAUTH2_APPLICATION_CLIENT_ID

    async def conversation(port, name):
        connection = await websocket_connect(HTTPRequest(
            f'ws://127.0.0.1:{port}/websocket/terminal?term_name={name}',
            headers=dict(Cookie=f'oauth_token={signed_in.token}'),
        ))
        connection.write_message(json.dumps(dict(
            api_key=OAUTH2_APPLICATION_CLIENT_ID, token=signed_in.access_token,
            command=['stdin', 'echo gateway-$((40+2))\r'],
        )))
        text = ''
        # Until the shell has answered the command, or refused it.
        while not re.search(r'gateway-42|Unauthorized', text.replace('$((40+2))', '')):
            message = await asyncio.wait_for(connection.read_message(), 30)
            assert message is not None, text
            text += ''.join(part for part in json.loads(message)[1:] if isinstance(part, str))
        connection.close()
        return 'gateway-42' in text.replace('$((40+2))', '')

    gateway, server = ports
    assert asyncio.run(conversation(server, 'direct'))
    assert asyncio.run(conversation(gateway, 'through'))


def test_the_gateway_adds_under_two_milliseconds_at_p95(ports, signed_in):
    gateway, server = ports
    session = signed_in

    def timings(port):
        url = f'http://127.0.0.1:{port}/api/status'
        for _ in range(20):
            session.get(url, timeout=30)
        result = []
        for _ in range(300):
            start = time.perf_counter()
            session.get(url, timeout=30).raise_for_status()
            result.append((time.perf_counter() - start) * 1000)
        return result

    direct, through = timings(server), timings(gateway)
    p95 = statistics.quantiles(through, n=20)[-1] - statistics.quantiles(direct, n=20)[-1]
    median = statistics.median(through) - statistics.median(direct)
    print(f'gateway adds {median:.2f} ms at the median, {p95:.2f} ms at p95')
    assert p95 < 2.0, (median, p95)


LOAD = """
@data_loader
def load(*args, **kwargs):
    return {'n': 1}
"""


def test_block_runs_read_from_the_database_equal_the_servers(ports, signed_in):
    """The shadow read of GET /api/block_runs, for each filter, order and page."""
    gateway, _ = ports
    base = f'http://127.0.0.1:{gateway}/api'

    def call(method, path, **body):
        response = signed_in.request(method, f'{base}/{path}', json=body or None, timeout=60)
        response.raise_for_status()
        return response.json()

    pipelines = []
    for name in ('shadow_one', 'shadow_two'):
        made = call('POST', 'pipelines', pipeline=dict(name=name, type='python'))
        uuid = made['pipeline']['uuid']
        for block in ('first', 'second'):
            call('POST', f'pipelines/{uuid}/blocks', block=dict(
                name=f'{name}_{block}', type='data_loader', language='python', content=LOAD,
            ))
        schedule = call('POST', f'pipelines/{uuid}/pipeline_schedules', pipeline_schedule=dict(
            name=f'{name} trigger', schedule_type='api', status='active',
        ))['pipeline_schedule']
        for _ in range(2):
            call('POST', f"pipeline_schedules/{schedule['id']}/pipeline_runs",
                 pipeline_run=dict(variables={}))
        pipelines.append(uuid)
    run_id = call('GET', f'pipeline_runs?pipeline_uuid={pipelines[0]}')['pipeline_runs'][0]['id']

    queries = [
        '', '_limit=3', '_limit=3&_offset=3', '_limit=100',
        f'pipeline_uuid={pipelines[0]}', f'pipeline_run_id={run_id}',
        f'pipeline_uuid={pipelines[1]}&order_by=id%20asc', 'order_by=block_uuid+desc',
        'order_by=created_at', 'pipeline_uuid=nothing_here',
    ]
    def counts():
        report = requests.get(f'http://127.0.0.1:{gateway}/__gateway/shadow', timeout=10).json()
        found = report['routes'].get('block_runs', {})
        return {k: found.get(k, 0) for k in COUNTS}, report

    # The scheduler makes a run's block runs when it starts it.
    deadline = time.monotonic() + 120
    while any(call('GET', f'block_runs?pipeline_uuid={uuid}')['metadata']['count'] < 4
              for uuid in pipelines):
        assert time.monotonic() < deadline, 'the scheduler made no block runs'
        time.sleep(1)
    before, _ = counts()
    for query in queries:
        assert signed_in.get(f'{base}/block_runs?{query}', timeout=60).status_code == 200
    deadline = time.monotonic() + 30
    while True:
        after, report = counts()
        added = {k: after[k] - before[k] for k in after}
        if sum(added.values()) >= len(queries) or time.monotonic() > deadline:
            break
        time.sleep(0.2)
    # Equal, at once or once read again after a run moved on between the two reads.
    assert added['equal'] + added['equal_after_reread'] == len(queries), \
        json.dumps(report['recent_differences'], indent=1)
    assert added['different'] == added['failed'] == 0


def test_run_details_read_from_the_database_equal_the_servers(ports, signed_in):
    """The shadow read of GET /api/pipeline_runs/<id>, after the runs of the test above."""
    gateway, _ = ports
    base = f'http://127.0.0.1:{gateway}/api'
    runs = signed_in.get(f'{base}/pipeline_runs?_limit=100', timeout=60).json()['pipeline_runs']
    assert len(runs) >= 4

    def counts():
        report = requests.get(f'http://127.0.0.1:{gateway}/__gateway/shadow', timeout=10).json()
        found = report['routes'].get('pipeline_run', {})
        return {k: found.get(k, 0) for k in COUNTS}, report

    before, _ = counts()
    blocks = 0
    for run in runs:
        detail = signed_in.get(f"{base}/pipeline_runs/{run['id']}", timeout=60)
        assert detail.status_code == 200
        blocks += len(detail.json()['pipeline_run']['block_runs'])
    assert blocks >= 8
    deadline = time.monotonic() + 30
    while True:
        after, report = counts()
        added = {k: after[k] - before[k] for k in after}
        if sum(added.values()) >= len(runs) or time.monotonic() > deadline:
            break
        time.sleep(0.2)
    # Equal, at once or once read again after a run moved on between the two reads.
    assert added['equal'] + added['equal_after_reread'] == len(runs), \
        json.dumps(report['recent_differences'], indent=1)
    assert added['different'] == added['failed'] == 0
