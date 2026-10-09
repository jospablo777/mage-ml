"""
Success and failure responses a block that calls a REST API has to handle.

Errors come back as problem details (RFC 9457) with content type
application/problem+json. Rate limits and unavailable services send Retry-After.
"""
import io
import json
import uuid

import httpx
import polars as pl
import pytest
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from integration_tests.services.api.app import datasets, formats

PROBLEM = 'application/problem+json'


def retrying_session(total: int = 3) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=total,
        backoff_factor=0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=None,
        respect_retry_after_header=True,
    )
    session.mount('http://', HTTPAdapter(max_retries=retry))
    return session


def flaky_key() -> str:
    return f'k_{uuid.uuid4().hex[:12]}'


@pytest.mark.parametrize('code', [400, 401, 403, 404, 409, 410, 422, 429, 500, 502, 503])
def test_errors_are_problem_details(api_url, code):
    response = requests.get(f'{api_url}/status/{code}', timeout=10)

    assert response.status_code == code
    assert response.headers['content-type'] == PROBLEM
    body = response.json()
    assert body['status'] == code and body['title']
    with pytest.raises(requests.HTTPError):
        response.raise_for_status()


@pytest.mark.parametrize('code', [429, 503])
def test_rate_limits_send_retry_after(api_url, code):
    response = requests.get(f'{api_url}/status/{code}', params=dict(retry_after=7), timeout=10)

    assert response.headers['Retry-After'] == '7'


def test_no_content(api_url):
    response = requests.delete(f'{api_url}/status/204', timeout=10)

    assert response.status_code == 204
    assert response.content == b''


def test_retries_recover_from_transient_failures(api_url):
    key = flaky_key()

    response = retrying_session(total=3).get(
        f'{api_url}/flaky/{key}', params=dict(failures=2), timeout=10,
    )

    assert response.status_code == 200
    assert response.json()['calls'] == 3


def test_retries_give_up_and_report_the_last_status(api_url):
    key = flaky_key()

    with pytest.raises(requests.exceptions.RetryError, match='503'):
        retrying_session(total=2).get(
            f'{api_url}/flaky/{key}', params=dict(failures=5), timeout=10,
        )
    assert requests.get(f'{api_url}/flaky/{key}/calls', timeout=10).json()['calls'] == 3


def test_client_errors_are_not_retried(api_url):
    key = flaky_key()

    response = retrying_session(total=3).get(
        f'{api_url}/flaky/{key}', params=dict(failures=5, code=400), timeout=10,
    )

    assert response.status_code == 400
    assert requests.get(f'{api_url}/flaky/{key}/calls', timeout=10).json()['calls'] == 1


def test_post_retries_need_an_idempotency_key(api_url, collection):
    """A retried POST must not store the rows twice."""
    body, media = formats.encode(datasets.orders(10), 'parquet', datasets.FLAT_COLUMNS)
    headers = {'Content-Type': media, 'Idempotency-Key': uuid.uuid4().hex}
    url = f'{api_url}/collections/{collection}'

    first = requests.post(url, data=body, headers=headers, params=dict(mode='append'), timeout=10)
    second = requests.post(url, data=body, headers=headers, params=dict(mode='append'), timeout=10)

    assert first.status_code == 201 and second.status_code == 200
    assert second.headers['Idempotent-Replayed'] == 'true'
    assert second.json()['rows'] == 10


def test_timeouts(api_url):
    with pytest.raises(requests.exceptions.ReadTimeout):
        requests.get(f'{api_url}/slow', params=dict(seconds=3), timeout=0.5)
    with pytest.raises(httpx.ReadTimeout):
        httpx.get(f'{api_url}/slow', params=dict(seconds=3), timeout=0.5)


def test_connection_errors(api_url):
    with pytest.raises(requests.exceptions.ConnectionError):
        requests.get('http://127.0.0.1:9/health', timeout=2)


@pytest.mark.parametrize('kind', ['json', 'html'])
def test_malformed_json_bodies_raise(api_url, kind):
    """A 200 response can still carry a body that does not parse."""
    response = requests.get(f'{api_url}/malformed', params=dict(kind=kind), timeout=10)

    assert response.status_code == 200
    with pytest.raises(requests.exceptions.JSONDecodeError):
        response.json()


def test_malformed_csv_and_parquet_bodies_raise(api_url):
    csv = requests.get(f'{api_url}/malformed', params=dict(kind='csv'), timeout=10)
    parquet = requests.get(f'{api_url}/malformed', params=dict(kind='parquet'), timeout=10)

    with pytest.raises(pl.exceptions.ComputeError):
        pl.read_csv(io.BytesIO(csv.content))
    with pytest.raises(pl.exceptions.ComputeError):
        pl.read_parquet(io.BytesIO(parquet.content))


@pytest.mark.parametrize('code', [301, 302, 303, 307, 308])
def test_redirects_are_followed(api_url, code):
    response = requests.get(
        f'{api_url}/redirect', params=dict(to='/health', code=code), timeout=10,
    )

    assert response.status_code == 200
    assert response.history[0].status_code == code


def test_authentication(api_url):
    bearer = requests.get(f'{api_url}/secure/bearer', timeout=10)
    assert bearer.status_code == 401 and bearer.headers['WWW-Authenticate'] == 'Bearer'
    assert requests.get(
        f'{api_url}/secure/bearer', headers={'Authorization': 'Bearer test-token'}, timeout=10,
    ).json()['authenticated']

    assert requests.get(f'{api_url}/secure/api-key', timeout=10).status_code == 401
    assert requests.get(
        f'{api_url}/secure/api-key', headers={'X-API-Key': 'wrong'}, timeout=10,
    ).status_code == 403
    assert requests.get(
        f'{api_url}/secure/api-key', headers={'X-API-Key': 'test-api-key'}, timeout=10,
    ).status_code == 200

    assert requests.get(f'{api_url}/secure/basic', auth=('mage', 'wrong'), timeout=10) \
        .status_code == 401
    assert requests.get(f'{api_url}/secure/basic', auth=('mage', 'secret'), timeout=10) \
        .json()['scheme'] == 'basic'


def test_cursor_pagination_reads_every_row_once(api_url):
    rows, cursor = [], None
    while True:
        params = dict(rows=95, page_size=10)
        if cursor:
            params['cursor'] = cursor
        page = requests.get(f'{api_url}/datasets/orders/pages', params=params, timeout=10).json()
        rows.extend(page['data'])
        cursor = page['next_cursor']
        if not cursor:
            break

    assert [r['id'] for r in rows] == list(range(1, 96))


def test_link_header_pagination(api_url):
    url, ids = f'{api_url}/datasets/orders/offset?rows=25&limit=10', []
    while url:
        response = requests.get(url, timeout=10)
        ids.extend(r['id'] for r in response.json())
        url = response.links.get('next', {}).get('url')

    assert ids == list(range(1, 26))
    assert response.headers['X-Total-Count'] == '25'


def test_invalid_cursor(api_url):
    response = requests.get(
        f'{api_url}/datasets/orders/pages', params=dict(cursor='not base64!'), timeout=10,
    )

    assert response.status_code == 400


def test_streamed_ndjson(api_url):
    with requests.get(
        f'{api_url}/datasets/orders/stream', params=dict(rows=1000), stream=True, timeout=30,
    ) as response:
        ids = [json.loads(line)['id'] for line in response.iter_lines() if line]

    assert ids == list(range(1, 1001))


def test_schema_mismatch_lists_each_error(api_url, collection):
    rows = [dict(id='one', big_id=1, small=1, amount=1.0, price='x', flag='yes')]

    response = requests.post(
        f'{api_url}/collections/{collection}',
        json=rows,
        params=dict(schema='orders', columns='id,big_id,small,amount,price,flag'),
        timeout=10,
    )

    assert response.status_code == 422
    errors = {e['column'] for e in response.json()['errors']}
    assert errors == {'id', 'price', 'flag'}


def test_missing_and_extra_columns(api_url, collection):
    response = requests.post(
        f'{api_url}/collections/{collection}',
        json=[dict(id=1, unexpected=2)],
        params=dict(schema='orders', columns='id,name'),
        timeout=10,
    )

    messages = {(e['column'], e['message']) for e in response.json()['errors']}
    assert messages == {('name', 'missing'), ('unexpected', 'not in the schema')}


def test_unsupported_content_type(api_url, collection):
    response = requests.post(
        f'{api_url}/collections/{collection}', data=b'<xml/>',
        headers={'Content-Type': 'application/xml'}, timeout=10,
    )

    assert response.status_code == 415


def test_unknown_resources(api_url):
    assert requests.get(f'{api_url}/datasets/missing', timeout=10).status_code == 404
    assert requests.get(f'{api_url}/collections/missing', timeout=10).status_code == 404
    assert requests.get(
        f'{api_url}/datasets/orders', params=dict(format='xml'), timeout=10,
    ).status_code == 400


def test_appending_frames_with_different_columns_conflicts(api_url, collection):
    url = f'{api_url}/collections/{collection}'
    requests.post(url, json=[dict(id=1)], timeout=10).raise_for_status()

    response = requests.post(
        url, json=[dict(other='a')], params=dict(mode='append'), timeout=10,
    )

    assert response.status_code == 409


def test_prediction_endpoint(api_url):
    ok = requests.post(
        f'{api_url}/predict',
        json=dict(instances=[dict(x1=1, x2=2, segment='a'), dict(x1=0, x2=0, segment='b')]),
        timeout=10,
    )
    invalid = requests.post(
        f'{api_url}/predict', json=dict(instances=[dict(x1='high', segment='')]), timeout=10,
    )
    empty = requests.post(f'{api_url}/predict', json=dict(instances=[]), timeout=10)

    assert ok.json()['predictions'] == [9.0, 11.0]
    assert invalid.status_code == 422
    locations = {tuple(e['location'][-1:]) for e in invalid.json()['errors']}
    assert locations == {('x1',), ('x2',), ('segment',)}
    assert empty.status_code == 422


def test_echo_shows_what_the_client_sent(api_url):
    body = b'payload'

    response = requests.put(
        f'{api_url}/echo', data=body, params=dict(a='1'),
        headers={'X-Request-Id': 'abc'}, timeout=10,
    ).json()

    assert response['method'] == 'PUT'
    assert response['query'] == dict(a='1')
    assert response['headers']['x-request-id'] == 'abc'
    assert response['body_size'] == len(body)
