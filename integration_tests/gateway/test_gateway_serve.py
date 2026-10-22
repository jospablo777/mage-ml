"""
MAGE_GATEWAY_SERVE: the gateway answers the block runs list, the run detail and the runs
list itself when
its own decision allows the read, with what the server answers, and leaves refusals to the
server, which answers them as before.
"""
import statistics
import time

import pytest
import requests

from integration_tests.gateway.test_gateway import LOAD, run_server

ROUTES = 'block_runs,pipeline_run,pipeline_runs'


@pytest.fixture(scope='module')
def served(tmp_path_factory):
    for port, upstream, _ in run_server(tmp_path_factory, MAGE_GATEWAY_SERVE=ROUTES):
        yield port, upstream


def sign_in(port):
    from mage_ai.settings.server import OAUTH2_APPLICATION_CLIENT_ID

    session = requests.Session()
    session.headers['X-API-KEY'] = OAUTH2_APPLICATION_CLIENT_ID
    response = session.post(f'http://127.0.0.1:{port}/api/sessions', json=dict(
        session=dict(email='admin@admin.com', password='admin'),
    ), timeout=60)
    session.cookies.set('oauth_token', response.json()['session']['token'])
    return session


def served_count(gateway):
    report = requests.get(f'http://127.0.0.1:{gateway}/__gateway/shadow', timeout=10).json()
    return sum(r.get('served', 0) for r in report['routes'].values())


def test_the_gateway_answers_as_the_server_and_leaves_refusals_to_it(served):
    from mage_ai.settings.server import OAUTH2_APPLICATION_CLIENT_ID

    gateway, server = served
    owner = sign_in(gateway)
    api = f'http://127.0.0.1:{gateway}/api'
    uuid = owner.post(f'{api}/pipelines', json=dict(pipeline=dict(
        name='served', type='python',
    )), timeout=60).json()['pipeline']['uuid']
    owner.post(f'{api}/pipelines/{uuid}/blocks', json=dict(block=dict(
        name='served_load', type='data_loader', language='python', content=LOAD,
    )), timeout=60)
    schedule = owner.post(f'{api}/pipelines/{uuid}/pipeline_schedules', json=dict(
        pipeline_schedule=dict(name='served trigger', schedule_type='api', status='active'),
    ), timeout=60).json()['pipeline_schedule']
    run = owner.post(f"{api}/pipeline_schedules/{schedule['id']}/pipeline_runs", json=dict(
        pipeline_run=dict(variables={}),
    ), timeout=60).json()['pipeline_run']
    deadline = time.monotonic() + 120
    while not owner.get(f'{api}/block_runs?pipeline_uuid={uuid}', timeout=60).json()['block_runs']:
        assert time.monotonic() < deadline, 'the scheduler made no block runs'
        time.sleep(1)

    before = served_count(gateway)
    for path in (f'block_runs?pipeline_uuid={uuid}', 'block_runs?_limit=5',
                 f"pipeline_runs/{run['id']}",
                 'pipeline_runs?disable_retries_grouping=true&include_pipeline_tags=true'
                 '&include_pipeline_uuids=true&_limit=30'):
        through = owner.get(f'{api}/{path}', timeout=60)
        direct = owner.get(f'http://127.0.0.1:{server}/api/{path}', timeout=60)
        assert through.status_code == direct.status_code == 200
        # The same answer, while the run may have moved on: read the server's again if not.
        if through.json() != direct.json():
            through = owner.get(f'{api}/{path}', timeout=60)
            direct = owner.get(f'http://127.0.0.1:{server}/api/{path}', timeout=60)
        assert through.json() == direct.json(), path
        assert through.headers['access-control-allow-origin'] == '*'
        assert through.headers['content-type'] == 'application/json'
        assert owner.get(f'{api}/{path}', timeout=60, headers={
            'If-None-Match': through.headers['etag'],
        }).status_code in (200, 304)
    assert served_count(gateway) > before

    # A caller without a session: the gateway leaves it to the server, which refuses.
    stranger = requests.Session()
    stranger.headers['X-API-KEY'] = OAUTH2_APPLICATION_CLIENT_ID
    refused = stranger.get(f'{api}/block_runs', timeout=60).json()
    assert refused['error']['code'] == 403, refused


def test_served_reads_are_quicker_than_the_servers(served):
    gateway, server = served
    owner = sign_in(gateway)

    def p95(port, path='block_runs?_limit=20'):
        url = f'http://127.0.0.1:{port}/api/{path}'
        for _ in range(10):
            owner.get(url, timeout=30)
        times = []
        for _ in range(100):
            start = time.perf_counter()
            owner.get(url, timeout=30).raise_for_status()
            times.append((time.perf_counter() - start) * 1000)
        return statistics.quantiles(times, n=20)[-1]

    direct, through = p95(server), p95(gateway)
    print(f'block runs list p95: server {direct:.1f} ms, gateway {through:.1f} ms')
    assert through < direct, (through, direct)
    page = 'pipeline_runs?disable_retries_grouping=true&include_pipeline_tags=true' \
        '&include_pipeline_uuids=true&_limit=30'
    direct, through = p95(server, page), p95(gateway, page)
    print(f'runs page list p95: server {direct:.1f} ms, gateway {through:.1f} ms')
    assert through < direct, (through, direct)
