"""
Champion and challenger in an exported service (item 79), end to end: two versions of a
model registered in MLflow (the test server), a service whose definition names the
second as a challenger of the first, requests over HTTP that get the champion's scores
only, and the comparison at /v1/models from the challenger's shadow records.
"""
import json
import time
import urllib.request

import pytest

from integration_tests.exported_services import TOKEN, RunningService, service_binary
from integration_tests.postgres.test_sql_block_matrix import block_uuid, build

SCORE = '''
import pandas as pd

from mage_ai.pipeline_services.models import predict


@data_loader
def score(*args, **kwargs):
    ids = [int(i) for i in str(kwargs['ids']).split(',')]
    frame = pd.DataFrame({'id': ids, 'x': [float(i) for i in ids]})
    scores = predict('churn', frame[['x']].assign(id=frame['id']), uri='models:/churn/1',
                     key='id', flavor='native', features=['x'])
    return pd.DataFrame({'id': ids, 'score': scores})
'''


def register(mlflow_url, monkeypatch) -> str:
    """Two versions of a new registered model: 2x and 3x; returns its name."""
    mlflow = pytest.importorskip('mlflow')
    linear = pytest.importorskip('sklearn.linear_model')
    import uuid

    import numpy as np

    name = f'churn_{uuid.uuid4().hex[:8]}'
    monkeypatch.setenv('MLFLOW_TRACKING_URI', mlflow_url)
    mlflow.set_tracking_uri(mlflow_url)
    x = np.arange(10, dtype=float).reshape(-1, 1)
    for slope in (2.0, 3.0):
        model = linear.LinearRegression().fit(x, x[:, 0] * slope)
        with mlflow.start_run():
            mlflow.sklearn.log_model(model, name='model', registered_model_name=name)
    return name


def request(port, pipeline, block, ids):
    body = json.dumps(dict(variables=dict(ids=','.join(map(str, ids))))).encode()
    sent = urllib.request.Request(
        f'http://127.0.0.1:{port}/v1/pipelines/{pipeline}/runs?wait=60&output={block}',
        data=body, method='POST',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
    )
    with urllib.request.urlopen(sent, timeout=90) as response:
        return json.loads(response.read())


@pytest.mark.timeout(1800)
def test_a_challenger_scores_in_the_shadow_and_is_compared(mage_project, mlflow_url, tmp_path,
                                                            monkeypatch):
    from mage_ai.pipeline_services import export

    registered = register(mlflow_url, monkeypatch)
    pipeline = build(mage_project, [dict(uuid='score', type='data_loader', code=SCORE)])
    captured = export.capture(
        mage_project, [pipeline], name='scoring', models=[f'churn=models:/{registered}/1'],
        challengers=dict(churn=dict(sample=1.0, challengers=[
            dict(name='v2', uri=f'models:/{registered}/2'),
        ])),
    )
    directory = export.write(captured, str(tmp_path / 'service'), force=True)
    manifest = json.loads((directory / 'service.json').read_text())
    [model] = manifest['models']
    assert [c['name'] for c in model['challengers']] == ['v2']
    assert model['challengers'][0]['version'] == '2'

    service = RunningService(service_binary(), directory, tmp_path / 'data')
    service.start()
    try:
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                service.get('/healthz')
                break
            except OSError:
                time.sleep(0.25)
        block = block_uuid(pipeline, 'score')
        for ids in ([1, 2, 3], [4, 5]):
            run = request(service.port, pipeline, block, ids)
            assert run['status'] == 'completed', run
            # The champion's scores only: 2x.
            assert [r['score'] for r in run['output']] == pytest.approx([2.0 * i for i in ids])
        comparison = None
        for _ in range(100):
            [described] = service.get('/v1/models')['models']
            [comparison] = described['challengers'] or [None]
            if comparison and comparison['rows'] == 5:
                break
            time.sleep(0.1)
        assert comparison is not None and comparison['rows'] == 5, described
        assert (comparison['challenger'], comparison['version']) == ('v2', '2')
        assert comparison['errors'] == 0 and comparison['calls'] == 2
        assert comparison['agreement'] == 0
        assert comparison['mean_abs_diff'] == pytest.approx(3.0)  # mean of 1..5
        assert comparison['challenger_ms']['p50'] is not None
    finally:
        service.stop()
