"""
Reading model metadata and artifacts from an MLflow tracking server, over its REST API
and with the mlflow client (mage-ml[mlflow]).

The server in services/mlflow stores runs in PostgreSQL and proxies artifacts. It holds
two runs of a linear model, y = 2 * x1 + 3 * x2 + intercept, registered as versions 1
(champion, saved with cloudpickle as MLflow 2 did) and 2 (challenger, saved with skops,
the MLflow 3 default).
"""
import hashlib
import io
import os
import tempfile

import cloudpickle
import pandas as pd
import polars as pl
import pytest
import requests
import yaml
from PIL import Image

from integration_tests.services.mlflow import expected

API = '/api/2.0/mlflow'
ARTIFACTS = '/api/2.0/mlflow-artifacts/artifacts'
INPUT = pd.DataFrame([[1.0, 2.0], [3.0, 0.0], [-1.5, 10.0]], columns=['x1', 'x2'])


def api(mlflow_url, method, path, **kwargs):
    response = requests.request(method, f'{mlflow_url}{API}/{path}', timeout=30, **kwargs)
    response.raise_for_status()
    return response.json()


def runs(mlflow_url):
    experiment = api(mlflow_url, 'GET', 'experiments/get-by-name',
                     params=dict(experiment_name=expected.EXPERIMENT))['experiment']
    found = api(mlflow_url, 'POST', 'runs/search', json=dict(
        experiment_ids=[experiment['experiment_id']], order_by=['attributes.start_time ASC'],
    ))['runs']
    return {run['info']['run_name']: run for run in found}


def proxied(uri: str) -> str:
    """The artifact proxy path of an mlflow-artifacts:/ URI."""
    assert uri.startswith('mlflow-artifacts:/'), uri
    return uri[len('mlflow-artifacts:/'):]


def predictions(intercept):
    rows = INPUT.to_numpy().tolist()
    return expected.target(rows, intercept)


def test_experiment_and_its_tags(mlflow_url):
    experiment = api(mlflow_url, 'GET', 'experiments/get-by-name',
                     params=dict(experiment_name=expected.EXPERIMENT))['experiment']

    assert experiment['lifecycle_stage'] == 'active'
    assert experiment['artifact_location'].startswith('mlflow-artifacts:/')
    assert {t['key']: t['value'] for t in experiment['tags']} == dict(
        team='data', purpose='integration tests',
    )


def test_runs_with_params_metrics_and_tags(mlflow_url):
    found = runs(mlflow_url)

    assert list(found) == list(expected.RUNS)
    for name, run in found.items():
        params = {p['key']: p['value'] for p in run['data']['params']}
        metrics = {m['key']: m['value'] for m in run['data']['metrics']}
        tags = {t['key']: t['value'] for t in run['data']['tags']}
        assert params == expected.PARAMS[name]
        assert metrics['rmse'] == expected.rmse_history(name)[-1]
        assert metrics['r2'] == 1.0
        assert tags['stage'] == name
        assert run['info']['status'] == 'FINISHED'


def test_search_runs_by_param_and_metric(mlflow_url):
    experiment_id = runs(mlflow_url)['train-v1']['info']['experiment_id']

    by_param = api(mlflow_url, 'POST', 'runs/search', json=dict(
        experiment_ids=[experiment_id], filter="params.note = 'día ñ'",
    ))['runs']
    by_metric = api(mlflow_url, 'POST', 'runs/search', json=dict(
        experiment_ids=[experiment_id], filter='metrics.rmse < 0.15',
        order_by=['metrics.rmse ASC'],
    ))['runs']

    assert [r['info']['run_name'] for r in by_param] == ['train-v1']
    assert [r['info']['run_name'] for r in by_metric] == ['train-v1']


def test_metric_history_keeps_every_step(mlflow_url):
    for name, run in runs(mlflow_url).items():
        history = api(mlflow_url, 'GET', 'metrics/get-history', params=dict(
            run_id=run['info']['run_id'], metric_key='rmse',
        ))['metrics']

        assert [(m['step'], m['value']) for m in history] == list(
            enumerate(expected.rmse_history(name))
        )


def test_registered_model_versions_and_aliases(mlflow_url):
    model = api(mlflow_url, 'GET', 'registered-models/get',
                params=dict(name=expected.MODEL))['registered_model']

    assert model['description'] == 'Linear model for tests'
    assert {t['key']: t['value'] for t in model['tags']} == dict(owner='mage')
    assert {a['alias']: int(a['version']) for a in model['aliases']} == expected.ALIASES
    for alias, version in expected.ALIASES.items():
        resolved = api(mlflow_url, 'GET', 'registered-models/alias',
                       params=dict(name=expected.MODEL, alias=alias))['model_version']
        assert resolved['version'] == str(version)
        assert resolved['status'] == 'READY'
        assert {t['key']: t['value'] for t in resolved['tags']} == dict(validated='true')
        assert resolved['run_id'] == runs(mlflow_url)[expected.RUNS[version - 1]]['info']['run_id']


def test_search_model_versions(mlflow_url):
    versions = api(mlflow_url, 'GET', 'model-versions/search',
                   params=dict(filter=f"name = '{expected.MODEL}'"))['model_versions']

    assert sorted(v['version'] for v in versions) == ['1', '2']


def list_artifacts(mlflow_url, run_id, path=''):
    """Every file under a run's artifacts, walked through the listing endpoint."""
    listing = api(mlflow_url, 'GET', 'artifacts/list', params=dict(run_id=run_id, path=path))
    files = {}
    for entry in listing.get('files', []):
        if entry['is_dir']:
            files.update(list_artifacts(mlflow_url, run_id, entry['path']))
        else:
            files[entry['path']] = entry['file_size']
    return files


def test_artifact_listing(mlflow_url):
    run_id = runs(mlflow_url)['train-v1']['info']['run_id']

    files = list_artifacts(mlflow_url, run_id)

    expected_files = set(expected.text_files()) | {
        'large.bin', 'data/sample.parquet', 'plots/swatch.png',
    }
    assert set(files) == expected_files
    assert files['large.bin'] == 5 * 1024 * 1024


@pytest.mark.parametrize('route', ['proxy', 'get-artifact'])
def test_text_artifacts_download_exactly(mlflow_url, route):
    run = runs(mlflow_url)['train-v1']
    root = proxied(run['info']['artifact_uri'])

    for path, content in expected.text_files().items():
        if route == 'proxy':
            response = requests.get(f'{mlflow_url}{ARTIFACTS}/{root}/{path}', timeout=30)
        else:
            response = requests.get(
                f'{mlflow_url}/get-artifact',
                params=dict(path=path, run_uuid=run['info']['run_id']), timeout=30,
            )
        assert response.status_code == 200, (path, response.text)
        assert response.content.decode('utf-8') == content, path


def test_binary_artifacts(mlflow_url):
    root = proxied(runs(mlflow_url)['train-v2']['info']['artifact_uri'])

    large = requests.get(f'{mlflow_url}{ARTIFACTS}/{root}/large.bin', timeout=60).content
    partial = requests.get(f'{mlflow_url}{ARTIFACTS}/{root}/large.bin',
                           headers={'Range': 'bytes=100-199'}, timeout=30)
    with requests.get(f'{mlflow_url}{ARTIFACTS}/{root}/large.bin', stream=True,
                      timeout=60) as streamed:
        digest = hashlib.sha256()
        for chunk in streamed.iter_content(64 * 1024):
            digest.update(chunk)

    assert hashlib.sha256(large).digest() == hashlib.sha256(expected.large_file()).digest()
    assert digest.digest() == hashlib.sha256(expected.large_file()).digest()
    assert partial.status_code == 206
    assert partial.content == expected.large_file()[100:200]


def test_data_and_image_artifacts(mlflow_url):
    root = proxied(runs(mlflow_url)['train-v1']['info']['artifact_uri'])
    parquet = requests.get(f'{mlflow_url}{ARTIFACTS}/{root}/data/sample.parquet', timeout=30)
    image = requests.get(f'{mlflow_url}{ARTIFACTS}/{root}/plots/swatch.png', timeout=30)

    rows = expected.training_rows()
    assert pd.read_parquet(io.BytesIO(parquet.content)).to_numpy().tolist() == rows
    assert pl.read_parquet(io.BytesIO(parquet.content)).rows() == [tuple(r) for r in rows]
    picture = Image.open(io.BytesIO(image.content))
    assert picture.size == (8, 4) and picture.getpixel((0, 0)) == (10, 20, 30)


def model_files(mlflow_url, version):
    uri = api(mlflow_url, 'GET', 'model-versions/get-download-uri',
              params=dict(name=expected.MODEL, version=version))['artifact_uri']
    root = proxied(uri)
    listing = requests.get(f'{mlflow_url}{ARTIFACTS}', params=dict(path=root), timeout=30)
    return root, {f['path'] for f in listing.json()['files']}


def test_model_files_over_http(mlflow_url):
    """MLflow 3 stores models under the logged model, not the run."""
    for version, run_name in ((1, 'train-v1'), (2, 'train-v2')):
        root, files = model_files(mlflow_url, version)
        mlmodel = yaml.safe_load(
            requests.get(f'{mlflow_url}{ARTIFACTS}/{root}/MLmodel', timeout=30).text,
        )

        assert '/models/m-' in f'/{root}'
        assert {'MLmodel', 'requirements.txt', 'input_example.json'} <= files
        flavor = mlmodel['flavors']['sklearn']
        assert flavor['serialization_format'] == expected.SERIALIZATION[run_name]
        assert flavor['sklearn_version'] == '1.9.1'
        signature = yaml.safe_load(mlmodel['signature']['inputs']) if isinstance(
            mlmodel['signature']['inputs'], str,
        ) else mlmodel['signature']['inputs']
        assert [c['name'] for c in signature] == ['x1', 'x2']


def test_pickled_model_downloaded_over_http(mlflow_url):
    root, files = model_files(mlflow_url, 1)
    data = requests.get(f'{mlflow_url}{ARTIFACTS}/{root}/model.pkl', timeout=30).content

    model = cloudpickle.loads(data)

    assert 'model.pkl' in files
    assert model.predict(INPUT).tolist() == pytest.approx(predictions(1.0))


def test_skops_model_downloaded_over_http(mlflow_url):
    """MLflow 3 saves scikit-learn models with skops; there is no model.pkl to download."""
    import skops.io

    root, files = model_files(mlflow_url, 2)
    data = requests.get(f'{mlflow_url}{ARTIFACTS}/{root}/model.skops', timeout=30).content
    missing_pickle = requests.get(f'{mlflow_url}{ARTIFACTS}/{root}/model.pkl', timeout=30)

    model = skops.io.loads(data, trusted=skops.io.get_untrusted_types(data=data))

    assert 'model.pkl' not in files and missing_pickle.status_code == 404
    assert model.predict(INPUT).tolist() == pytest.approx(predictions(-4.0))


@pytest.mark.parametrize('alias, intercept', [('champion', 1.0), ('challenger', -4.0)])
def test_client_loads_models_by_alias(mlflow_url, alias, intercept):
    import mlflow

    mlflow.set_tracking_uri(mlflow_url)
    sklearn_model = mlflow.sklearn.load_model(f'models:/{expected.MODEL}@{alias}')
    pyfunc_model = mlflow.pyfunc.load_model(f'models:/{expected.MODEL}@{alias}')

    assert sklearn_model.predict(INPUT).tolist() == pytest.approx(predictions(intercept))
    assert list(pyfunc_model.predict(INPUT)) == pytest.approx(predictions(intercept))
    # A Polars frame goes through pandas.
    from_polars = pyfunc_model.predict(pl.from_pandas(INPUT).to_pandas())
    assert list(from_polars) == pytest.approx(predictions(intercept))


def test_pyfunc_enforces_the_signature(mlflow_url):
    import mlflow
    from mlflow.exceptions import MlflowException

    mlflow.set_tracking_uri(mlflow_url)
    model = mlflow.pyfunc.load_model(f'models:/{expected.MODEL}@champion')

    with pytest.raises(MlflowException):
        model.predict(pd.DataFrame({'x1': [1.0]}))


def test_client_reads_metadata_and_downloads_artifacts(mlflow_url):
    from mlflow import MlflowClient

    client = MlflowClient(tracking_uri=mlflow_url)
    experiment = client.get_experiment_by_name(expected.EXPERIMENT)
    champion = client.get_model_version_by_alias(expected.MODEL, 'champion')
    history = client.get_metric_history(champion.run_id, 'rmse')
    listed = {a.path for a in client.list_artifacts(champion.run_id, 'config')}

    with tempfile.TemporaryDirectory() as directory:
        local = client.download_artifacts(champion.run_id, 'notes', directory)
        with open(os.path.join(local, 'notas_ñ.txt'), encoding='utf-8') as f:
            note = f.read()

    assert experiment.tags['team'] == 'data'
    assert champion.version == '1' and champion.tags == dict(validated='true')
    assert [m.value for m in sorted(history, key=lambda m: m.step)] == \
        expected.rmse_history('train-v1')
    assert listed == {'config/params.json'}
    assert note == expected.text_files()['notes/notas_ñ.txt']


def test_client_downloads_a_model_with_its_files(mlflow_url):
    import mlflow

    mlflow.set_tracking_uri(mlflow_url)
    with tempfile.TemporaryDirectory() as directory:
        local = mlflow.artifacts.download_artifacts(
            artifact_uri=f'models:/{expected.MODEL}/2', dst_path=directory,
        )
        files = set(os.listdir(local))

    assert {'MLmodel', 'model.skops', 'requirements.txt'} <= files


@pytest.mark.parametrize('path, params, status, code', [
    ('registered-models/get', dict(name='missing'), 404, 'RESOURCE_DOES_NOT_EXIST'),
    ('runs/get', dict(run_id='missing'), 404, 'RESOURCE_DOES_NOT_EXIST'),
    ('model-versions/get', dict(name=expected.MODEL, version='99'), 404,
     'RESOURCE_DOES_NOT_EXIST'),
    # A missing alias is reported as an invalid parameter, not a missing resource.
    ('registered-models/alias', dict(name=expected.MODEL, alias='missing'), 400,
     'INVALID_PARAMETER_VALUE'),
])
def test_missing_resources(mlflow_url, path, params, status, code):
    response = requests.get(f'{mlflow_url}{API}/{path}', params=params, timeout=30)

    assert response.status_code == status
    assert response.json()['error_code'] == code


def test_missing_artifact(mlflow_url):
    root = proxied(runs(mlflow_url)['train-v1']['info']['artifact_uri'])

    response = requests.get(f'{mlflow_url}{ARTIFACTS}/{root}/missing.txt', timeout=30)

    assert response.status_code == 404
    assert response.json()['error_code'] == 'RESOURCE_DOES_NOT_EXIST'


def test_invalid_search_filter(mlflow_url):
    response = requests.post(
        f'{mlflow_url}{API}/runs/search',
        json=dict(experiment_ids=['1'], filter='metrics.rmse <<< 1'), timeout=30,
    )

    assert response.status_code == 400
    assert response.json()['error_code'] == 'INVALID_PARAMETER_VALUE'
