"""A MageML service on the engine for the operator to call, as its own e2e tests run it."""
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest
import requests

REPO = Path(__file__).resolve().parents[3]
TOKEN = 'airflow-test-token-0123456789'
BLOCKS = {
    'data_loaders/load.py': (
        "@data_loader\ndef load(*args, **kwargs):\n"
        "    print('loading', kwargs.get('n', 3))\n"
        "    return {'values': list(range(int(kwargs.get('n', 3))))}\n"
    ),
    'transformers/total.py': (
        "@transformer\ndef total(data, *args, **kwargs):\n"
        "    return {'total': sum(data['values'])}\n"
    ),
    'transformers/broken.py': (
        "@transformer\ndef broken(data, *args, **kwargs):\n"
        "    raise RuntimeError('this block always fails')\n"
    ),
    'transformers/slow.py': (
        "import time\n\n@transformer\ndef slow(data, *args, **kwargs):\n"
        "    time.sleep(60)\n    return data\n"
    ),
}


def _block(uuid, kind, file, upstream):
    return dict(uuid=uuid, type=kind, language='python', file=file, sha256='0',
                upstream=upstream)


MANIFEST = {
    'schema_version': 1,
    'service': {'name': 'airflow-test', 'project': 'proj', 'exported_at': '2026-10-21T00:00:00Z',
                'mage_version': 'test', 'source_sha256': 'abc'},
    'pipelines': [
        {'uuid': 'sum', 'name': 'sum', 'max_concurrent_runs': 4, 'variables': {'n': 4},
         'blocks': [_block('load', 'data_loader', 'data_loaders/load.py', []),
                    _block('total', 'transformer', 'transformers/total.py', ['load'])],
         'triggers': []},
        {'uuid': 'fails', 'name': 'fails', 'max_concurrent_runs': 1,
         'blocks': [_block('load', 'data_loader', 'data_loaders/load.py', []),
                    _block('broken', 'transformer', 'transformers/broken.py', ['load'])],
         'triggers': []},
        {'uuid': 'slow', 'name': 'slow', 'max_concurrent_runs': 1,
         'blocks': [_block('load', 'data_loader', 'data_loaders/load.py', []),
                    _block('slow', 'transformer', 'transformers/slow.py', ['load'])],
         'triggers': []},
    ],
}


def _engine() -> str:
    candidates = [
        os.getenv('MAGE_ENGINE_PATH'),
        REPO / 'mage_ai/pipeline_services/service/target/debug/mage-service',
        REPO / 'mage_ai/pipeline_services/service/target/release/mage-service',
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return str(candidate)
    pytest.skip('No mage-service build: cargo build -p mage-service, or set MAGE_ENGINE_PATH')


@pytest.fixture(scope='session')
def service():
    directory = Path(tempfile.mkdtemp(prefix='mageml-airflow-'))
    for file, code in BLOCKS.items():
        path = directory / 'proj' / file
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(code)
    (directory / 'service.json').write_text(json.dumps(MANIFEST))
    log_path = directory / 'service.log'
    log = open(log_path, 'w')
    process = subprocess.Popen(
        [_engine(), 'serve'],
        env=dict(
            os.environ,
            MAGE_SERVICE_DIR=str(directory),
            MAGE_SERVICE_WORKER=str(REPO / 'mage_ai/pipeline_services/runtime/worker.py'),
            MAGE_SERVICE_PYTHON=os.getenv('MAGE_SERVICE_TEST_PYTHON', sys.executable),
            MAGE_SERVICE_PORT='0',
            MAGE_SERVICE_TOKEN=TOKEN,
            MAGE_SERVICE_SHUTDOWN_SECONDS='2',
        ),
        stdout=log, stderr=subprocess.STDOUT,
    )
    url = None
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and url is None:
        match = re.search(r' serves .* on (http://\S+)', log_path.read_text())
        if match:
            url = match.group(1)
        elif process.poll() is not None:
            break
        else:
            time.sleep(0.05)
    if url is None:
        process.kill()
        raise RuntimeError(f'the service did not start:\n{log_path.read_text()[-3000:]}')
    os.environ['AIRFLOW_CONN_MAGEML_DEFAULT'] = json.dumps(
        dict(conn_type='mageml_service', host=url, password=TOKEN),
    )
    yield dict(url=url, token=TOKEN, directory=directory)
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
    log.close()


@pytest.fixture
def api(service):
    def call(method, path, **kwargs):
        response = requests.request(
            method, service['url'] + path,
            headers={'Authorization': f"Bearer {service['token']}"}, timeout=30, **kwargs,
        )
        return response.json()

    return call
