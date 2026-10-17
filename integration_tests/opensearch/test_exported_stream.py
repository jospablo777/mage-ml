"""
An exported stream from a webhook to OpenSearch, run by mage-service: each message is a
document with its values exact, `_id` makes a replayed batch replace its documents, a
replayed `create` counts as written, and a document the index refuses stops the stream
with the reason instead of dropping it.
"""
import json
import textwrap
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import pytest
import requests

from integration_tests.exported_services import (
    TOKEN,
    RunningService,
    export_service,
    stream_service_binary,
)


@pytest.fixture
def index(opensearch_url):
    name = f'it_{uuid.uuid4().hex[:10]}'
    yield name
    requests.delete(f'{opensearch_url}/{name}', timeout=30)


def make_pipeline(project, url, index, op_type='index'):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline

    name = f'search_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, pipeline_type=PipelineType.STREAMING, repo_path=project)

    def add(suffix, kind, code, language, upstream=()):
        block = Block.create(f'{name}_{suffix}', kind, project, language=language,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('source', 'data_loader', 'connector_type: webhook\n', 'yaml')
    step = add('step', 'transformer', '''
        @transformer
        def keep(messages, *args, **kwargs):
            return messages
    ''', 'python', [source])
    add('sink', 'data_exporter', f'''
        connector_type: opensearch
        host: {url}
        index_name: {index}
        http_auth: "{{{{ env_var('OPENSEARCH_AUTH') }}}}"
        _id: id
        _op_type: {op_type}
    ''', 'yaml', [step])
    return pipeline


def run(project, pipeline, tmp_path, *batches):
    """Post each batch to the exported service; the statuses and the stream's state."""
    try:
        directory = export_service(project, pipeline.uuid, tmp_path)
        service = RunningService(stream_service_binary(), directory, tmp_path / 'data',
                                 OPENSEARCH_AUTH='admin:admin')
        service.start()
        try:
            service.wait_running()
            statuses = [post(service, pipeline.uuid, batch) for batch in batches]
            return statuses, service.streams()[0]
        finally:
            service.stop()
    finally:
        pipeline.delete()


def post(service, pipeline, messages):
    request = urllib.request.Request(
        f'http://127.0.0.1:{service.port}/v1/streams/{pipeline}/messages',
        data=json.dumps(messages).encode(), method='POST',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def documents(url, index):
    requests.post(f'{url}/{index}/_refresh', timeout=30)
    answer = requests.get(f'{url}/{index}/_search', params=dict(size=100), timeout=30).json()
    return {hit['_id']: hit['_source'] for hit in answer['hits']['hits']}


@pytest.mark.parametrize('op_type', ['index', 'create'])
def test_messages_become_documents_and_replays_write_nothing_twice(
    mage_project, opensearch_url, index, op_type, tmp_path,
):
    pipeline = make_pipeline(mage_project, opensearch_url, index, op_type)
    first = [dict(id=i, big=2**63 - 1 - i, city='Zürich 🚀') for i in range(3)]
    # The same messages again, as after a crash, and one more.
    statuses, stream = run(mage_project, pipeline, tmp_path,
                           first, first + [dict(id=9, big=1, city='x')])

    assert statuses == [200, 200]
    assert stream['last_error'] is None, stream
    docs = documents(opensearch_url, index)
    assert sorted(docs) == ['0', '1', '2', '9']
    assert docs['1'] == dict(id=1, big=2**63 - 2, city='Zürich 🚀')


def test_a_refused_document_stops_the_stream_with_the_reason(
    mage_project, opensearch_url, index, tmp_path,
):
    pipeline = make_pipeline(mage_project, opensearch_url, index)
    # The first document maps `big` as a long, which 2**70 does not fit.
    statuses, stream = run(mage_project, pipeline, tmp_path,
                           [dict(id=1, big=1)], [dict(id=2, big=2**70)])

    assert statuses[0] == 200
    status, body = statuses[1]
    assert status == 503
    assert 'failed to parse field [big]' in body
    assert 'failed to parse field [big]' in stream['last_error']
    assert sorted(documents(opensearch_url, index)) == ['1']
