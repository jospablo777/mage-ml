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
    project = tmp_path_factory.mktemp('gateway') / 'project'
    port = free_port()
    env = dict(
        os.environ, MAGE_GATEWAY='on', MAGE_ENGINE_PATH=str(engine),
        DISABLE_AUTO_BROWSER_OPEN='true',
    )
    process = subprocess.Popen(
        [sys.executable, '-c', 'from mage_ai.cli.main import app; app()', 'start',
         str(project), '--port', str(port)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env,
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
