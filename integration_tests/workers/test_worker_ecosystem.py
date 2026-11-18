"""
The SDKs workers exist for, in pipelines with MageML's own blocks: Feast (it needs pandas
2, MageML has pandas 3), MLflow 2 and MLflow 3 in one pipeline, and HTTP APIs called from a
worker. Each runs as people run it: the real package in its own worker, prepared from its
lock, its tables crossing to and from MageML's Python.

The workers, prepared once for the module:

- feast: the newest Feast that fits pandas 2;
- mlflow2: MLflow 2 with scikit-learn, for a model saved in MLflow 2's format;
- mlflow3: MLflow 3 with scikit-learn;
- stdlib: no packages, for the API calls.
"""
import json
import os
import shutil
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from integration_tests.workers.test_worker_pipelines import (
    by_name,
    completed,
    error_of,
    output,
    pipeline_of,
    run,
    write_worker,
)

pytestmark = pytest.mark.skipif(shutil.which('uv') is None, reason='uv is not installed')

SDK_WORKERS = {
    'feast': ('>=3.12', ['feast', 'pandas>=2.2,<3']),
    # pyarrow: tables reach a worker as Arrow (the Workers page adds it next to pandas).
    'mlflow2': ('>=3.12', ['mlflow-skinny>=2.20,<3', 'scikit-learn', 'pandas>=2.2,<3',
                           'pyarrow']),
    # skops: MLflow 3 saves scikit-learn models with it (the block's error says to add it).
    'mlflow3': ('>=3.12', ['mlflow-skinny>=3,<4', 'scikit-learn', 'pandas', 'pyarrow',
                           'skops']),
    'stdlib': ('>=3.12', []),
}


@pytest.fixture(scope='module')
def sdk_workers(mage_project, tmp_path_factory):
    from mage_ai.data_preparation import environments

    previous = os.environ.get(environments.ENVIRONMENTS_DIR_ENV)
    os.environ[environments.ENVIRONMENTS_DIR_ENV] = str(tmp_path_factory.mktemp('prepared'))
    folders = []
    for name, (python, dependencies) in SDK_WORKERS.items():
        folder = Path(mage_project, environments.WORKERS_DIR, name)
        folders.append(folder)
        write_worker(folder, name, python, dependencies)
        environments.prepare(environments.named_workers(mage_project)[name], log=lambda _: None)
    yield mage_project
    for folder in folders:
        shutil.rmtree(folder, ignore_errors=True)
    if previous is None:
        os.environ.pop(environments.ENVIRONMENTS_DIR_ENV, None)
    else:
        os.environ[environments.ENVIRONMENTS_DIR_ENV] = previous


DRIVER_STATS = '''
    import pandas as pd

    @data_loader
    def load(*args, **kwargs):
        return pd.DataFrame({
            'driver_id': pd.array([1, 2, 3, 1], dtype='int64'),
            'trips': pd.array([10, 20, 30, 11], dtype='int64'),
            'rating': [4.5, 3.9, 4.8, 4.6],
            'event_timestamp': pd.to_datetime(
                ['2026-11-17 10:00', '2026-11-17 10:00', '2026-11-17 10:00',
                 '2026-11-17 12:00'], utc=True),
        })
'''


def test_feast_materializes_and_serves_features_from_a_mageml_table(sdk_workers, tmp_path):
    # MageML's pandas 3 table reaches Feast's pandas 2 as Arrow; Feast's answer comes back.
    pipeline = pipeline_of(sdk_workers, [
        ('stats', 'data_loader', None, DRIVER_STATS),
        ('serve', 'transformer', 'feast', '''
            import os
            import textwrap
            from datetime import datetime, timedelta, timezone

            import feast
            import pandas as pd
            from feast import Entity, FeatureStore, FeatureView, Field, FileSource
            from feast.types import Float64, Int64

            @transformer
            def serve(stats, *args, **kwargs):
                assert pd.__version__.startswith('2.'), pd.__version__
                repo = kwargs['feast_repo']
                os.makedirs(repo, exist_ok=True)
                stats.to_parquet(os.path.join(repo, 'stats.parquet'))
                with open(os.path.join(repo, 'feature_store.yaml'), 'w') as file:
                    file.write(textwrap.dedent(f"""
                        project: shop
                        registry: {repo}/registry.db
                        provider: local
                        online_store:
                          type: sqlite
                          path: {repo}/online.db
                        entity_key_serialization_version: 3
                    """))
                store = FeatureStore(repo_path=repo)
                driver = Entity(name='driver', join_keys=['driver_id'])
                view = FeatureView(
                    name='driver_stats', entities=[driver], ttl=timedelta(days=7),
                    schema=[Field(name='trips', dtype=Int64), Field(name='rating', dtype=Float64)],
                    source=FileSource(path=os.path.join(repo, 'stats.parquet'),
                                      timestamp_field='event_timestamp'),
                )
                store.apply([driver, view])
                store.materialize(start_date=datetime(2026, 11, 16, tzinfo=timezone.utc),
                                  end_date=datetime(2026, 11, 18, tzinfo=timezone.utc))
                found = store.get_online_features(
                    features=['driver_stats:trips', 'driver_stats:rating'],
                    entity_rows=[{'driver_id': i} for i in (1, 2, 3)],
                ).to_df()
                return found.assign(feast_version=feast.__version__)
        '''),
        ('check', 'transformer', None, '''
            import pandas as pd

            @transformer
            def check(features, *args, **kwargs):
                assert pd.__version__.startswith('3.'), pd.__version__
                return features.sort_values('driver_id').reset_index(drop=True)
        '''),
    ], variables=dict(feast_repo=str(tmp_path / 'feast_repo')))

    features = output(pipeline, completed(run(pipeline)), 'check')
    # The newest event of driver 1 wins.
    assert features['trips'].tolist() == [11, 20, 30]
    assert features['rating'].tolist() == [4.6, 3.9, 4.8]
    assert features['feast_version'][0] >= '0.50'


TRAINING = '''
    import pandas as pd

    @data_loader
    def load(*args, **kwargs):
        x = [float(i) for i in range(40)]
        return pd.DataFrame({'x': x, 'y': [2.0 * v + 1.0 for v in x]})
'''


def mlflow_block(name: str) -> str:
    """Trains a model, saves it in its MLflow's format, loads it back and scores with it."""
    return f'''
        import os

        import mlflow
        import mlflow.sklearn
        import pandas as pd
        from sklearn.linear_model import LinearRegression

        @transformer
        def {name}(data, *args, **kwargs):
            model = LinearRegression().fit(data[['x']], data['y'])
            path = os.path.join(kwargs['models'], '{name}')
            mlflow.sklearn.save_model(model, path)
            loaded = mlflow.pyfunc.load_model(path)
            scored = pd.DataFrame({{'x': [100.0, 200.0]}})
            return scored.assign(
                prediction=loaded.predict(scored),
                mlflow=mlflow.__version__,
            )
    '''


def test_mlflow_2_and_mlflow_3_score_in_one_pipeline(sdk_workers, tmp_path):
    pipeline = pipeline_of(sdk_workers, [
        ('train', 'data_loader', None, TRAINING),
        ('legacy', 'transformer', 'mlflow2', mlflow_block('legacy'), ['train']),
        ('current', 'transformer', 'mlflow3', mlflow_block('current'), ['train']),
        ('compare', 'transformer', None, '''
            @transformer
            def compare(legacy, current, *args, **kwargs):
                return legacy.merge(current, on='x', suffixes=('_2', '_3'))
        ''', ['legacy', 'current']),
    ], variables=dict(models=str(tmp_path / 'models')))

    compared = output(pipeline, completed(run(pipeline)), 'compare')
    assert compared['mlflow_2'][0].startswith('2.'), compared['mlflow_2'][0]
    assert compared['mlflow_3'][0].startswith('3.'), compared['mlflow_3'][0]
    assert compared['prediction_2'].round(6).tolist() == [201.0, 401.0]
    assert compared['prediction_3'].round(6).tolist() == [201.0, 401.0]


def test_a_model_saved_by_mlflow_2_is_served_by_a_later_block_in_its_worker(sdk_workers,
                                                                            tmp_path):
    # The legacy case: a model in MLflow 2's format loads in an MLflow 2 worker, while the
    # pipeline's other blocks run on MageML's own packages.
    models = tmp_path / 'models'
    pipeline = pipeline_of(sdk_workers, [
        ('train', 'data_loader', None, TRAINING),
        ('save', 'transformer', 'mlflow2', '''
            import os

            import mlflow.sklearn
            from sklearn.linear_model import LinearRegression

            @transformer
            def save(data, *args, **kwargs):
                path = os.path.join(kwargs['models'], 'legacy')
                mlflow.sklearn.save_model(LinearRegression().fit(data[['x']], data['y']), path)
                return {'path': path}
        '''),
        ('score', 'transformer', 'mlflow2', '''
            import mlflow.pyfunc
            import pandas as pd

            @transformer
            def score(saved, *args, **kwargs):
                model = mlflow.pyfunc.load_model(saved['path'])
                return pd.DataFrame({'x': [3.0]}).assign(
                    prediction=model.predict(pd.DataFrame({'x': [3.0]})))
        '''),
        ('report', 'transformer', None, '''
            @transformer
            def report(scored, *args, **kwargs):
                return scored.assign(doubled=scored['prediction'] * 2)
        '''),
    ], variables=dict(models=str(models)))

    report = output(pipeline, completed(run(pipeline)), 'report')
    assert report['doubled'].round(6).tolist() == [14.0]
    assert (models / 'legacy' / 'MLmodel').is_file()


class _Api(BaseHTTPRequestHandler):
    received = []

    def do_GET(self):  # noqa: N802
        self._answer({'orders': [{'id': 1, 'amount': 9.5}, {'id': 2, 'amount': 20.0}],
                      'token': self.headers.get('Authorization')})

    def do_POST(self):  # noqa: N802
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        _Api.received.append(body)
        self._answer({'stored': len(body['rows'])})

    def _answer(self, payload):
        data = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


@pytest.fixture
def api():
    server = ThreadingHTTPServer(('127.0.0.1', 0), _Api)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    _Api.received.clear()
    yield f'http://127.0.0.1:{server.server_address[1]}'
    server.shutdown()


def test_a_worker_calls_an_api_with_the_servers_credentials_not_mageml_s(sdk_workers, api,
                                                                         monkeypatch):
    # The server's own variables reach a worker (an API token the blocks use); MageML's
    # credentials do not.
    monkeypatch.setenv('SHOP_API_TOKEN', 'Bearer shop-123')
    monkeypatch.setenv('MAGE_DATABASE_CONNECTION_URL', 'postgresql://mage:secret@db/mage')
    pipeline = pipeline_of(sdk_workers, [
        ('fetch', 'data_loader', 'stdlib', '''
            import json
            import os
            import urllib.request

            @data_loader
            def fetch(*args, **kwargs):
                request = urllib.request.Request(
                    kwargs['api_url'] + '/orders',
                    headers={'Authorization': os.environ['SHOP_API_TOKEN']})
                with urllib.request.urlopen(request, timeout=10) as answer:
                    payload = json.loads(answer.read())
                payload['saw_mage_database'] = 'MAGE_DATABASE_CONNECTION_URL' in os.environ
                return payload
        '''),
        ('enrich', 'transformer', None, '''
            import pandas as pd

            @transformer
            def enrich(payload, *args, **kwargs):
                orders = pd.DataFrame(payload['orders'])
                return orders.assign(with_tax=orders['amount'] * 1.1,
                                     token=payload['token'],
                                     saw_mage_database=payload['saw_mage_database'])
        '''),
        ('send', 'data_exporter', 'mlflow3', '''
            import json
            import urllib.request

            @data_exporter
            def send(rows, *args, **kwargs):
                body = json.dumps({'rows': rows.to_dict('records')}).encode()
                request = urllib.request.Request(
                    kwargs['api_url'] + '/rows', data=body, method='POST',
                    headers={'Content-Type': 'application/json'})
                with urllib.request.urlopen(request, timeout=10) as answer:
                    assert json.loads(answer.read()) == {'stored': 2}
        '''),
    ], variables=dict(api_url=api))

    pipeline_run = completed(run(pipeline))
    enriched = output(pipeline, pipeline_run, 'enrich')
    assert enriched['token'][0] == 'Bearer shop-123'
    assert not enriched['saw_mage_database'][0]
    [sent] = _Api.received
    assert [round(r['with_tax'], 2) for r in sent['rows']] == [10.45, 22.0]


def test_an_api_that_fails_fails_the_block_with_its_answer(sdk_workers):
    pipeline = pipeline_of(sdk_workers, [
        ('fetch', 'data_loader', 'stdlib', '''
            import urllib.request

            @data_loader
            def fetch(*args, **kwargs):
                urllib.request.urlopen('http://127.0.0.1:9/nothing', timeout=2)
        '''),
    ])
    pipeline_run = run(pipeline)
    message = error_of(by_name(pipeline_run)['fetch'])
    assert 'Connection refused' in message or 'URLError' in message, message
