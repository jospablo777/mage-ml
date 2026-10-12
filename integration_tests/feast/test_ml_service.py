"""
Two pipelines exported as one ML service and run as in production: mage-service serves
them, one call starts the features run, and its after-run trigger starts the scoring run.

driver_features reads the window of drivers added since its last run (a cursor kept in the
analytics database), takes the has_new or none_new branch, reads the new drivers' trips
with SQL files through a helper, saves their features to PostgreSQL and pushes them to
Feast's online store.

driver_scoring reads the drivers without a score, takes the has_cases or no_cases branch,
reads their online features from the feature service the model was trained on (its
feast.feature_service tag), scores them with a CatBoost model logged as a .cbm file and
embedded from MLflow by its alias, and writes the scores with a SQL UPDATE from {{ df_1 }}.
The service never calls MLflow: its tracking URI points nowhere.
"""
import os
import secrets
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import requests

from integration_tests.exported_services import REPO, service_binary

catboost = pytest.importorskip('catboost')

TOKEN = 'driver-service-token'
FEATURES = ['conv_rate', 'acc_rate', 'avg_daily_trips']


@pytest.fixture
def churn_model(mlflow_url, tmp_path):
    """A CatBoost classifier logged as a plain .cbm file, registered with alias champion."""
    from mlflow.tracking import MlflowClient

    from mage_ai.pipeline_services.exports import model_registry

    name = f'driver-churn-{uuid.uuid4().hex[:8]}'
    rng = np.random.default_rng(7)
    frame = pd.DataFrame({
        'conv_rate': rng.random(400),
        'acc_rate': rng.random(400),
        'avg_daily_trips': rng.integers(1, 40, 400).astype('float64'),
    })
    labels = ((frame['conv_rate'] < 0.5) & (frame['avg_daily_trips'] < 20)).astype(int)
    model = catboost.CatBoostClassifier(
        iterations=40, depth=3, verbose=False, allow_writing_files=False,
    ).fit(frame, labels)
    path = tmp_path / 'churn.cbm'
    model.save_model(str(path))
    with model_registry(mlflow_url):
        client = MlflowClient()
        run = client.create_run(client.create_experiment(name))
        client.log_artifact(run.info.run_id, str(path))
        client.set_terminated(run.info.run_id)
        client.create_registered_model(name)
        version = client.create_model_version(
            name, f'{run.info.artifact_uri}/churn.cbm', run_id=run.info.run_id,
        ).version
        client.set_registered_model_alias(name, 'champion', version)
        client.set_model_version_tag(name, version, 'feast.feature_service', 'driver_activity')
    return dict(uri=f'models:/{name}@champion', version=int(version), model=model)


def create_tables(pg, schema: str, drivers: list) -> pd.DataFrame:
    """The warehouse's drivers and trips, the features table and the scores to fill in."""
    trips = pd.DataFrame({
        'driver_id': drivers,
        'trips': [10 + 3 * i for i in range(len(drivers))],
        'accepted': [8 + 2 * i for i in range(len(drivers))],
    })
    trips['offered'] = trips['accepted'] + 5
    with pg.cursor() as cursor:
        cursor.execute(f'CREATE TABLE {schema}.drivers (driver_id BIGINT PRIMARY KEY)')
        cursor.execute(f'CREATE TABLE {schema}.trips '
                       '(driver_id BIGINT, trips INT, accepted INT, offered INT)')
        cursor.execute(f'CREATE TABLE {schema}.driver_features (driver_id BIGINT, '
                       'conv_rate REAL, acc_rate DOUBLE PRECISION, avg_daily_trips BIGINT, '
                       'event_timestamp TIMESTAMPTZ)')
        cursor.execute(f'CREATE TABLE {schema}.churn_scores (driver_id BIGINT PRIMARY KEY, '
                       'score DOUBLE PRECISION, model_version INT, scored_at TIMESTAMPTZ)')
        for row in trips.itertuples(index=False):
            cursor.execute(f'INSERT INTO {schema}.drivers VALUES (%s)', (row.driver_id,))
            cursor.execute(f'INSERT INTO {schema}.trips VALUES (%s, %s, %s, %s)', tuple(row))
            cursor.execute(f'INSERT INTO {schema}.churn_scores (driver_id) VALUES (%s)',
                           (row.driver_id,))
    pg.commit()
    return trips


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class Served:
    def __init__(self, directory: Path, data: Path, env: dict):
        self.url = f'http://127.0.0.1:{free_port()}'
        self.log = data / 'service.log'
        data.mkdir(parents=True, exist_ok=True)
        self.process = subprocess.Popen(
            [str(service_binary()), 'serve'],
            stdout=self.log.open('w'), stderr=subprocess.STDOUT,
            env=dict(
                os.environ, **env,
                MAGE_SERVICE_DIR=str(directory),
                MAGE_SERVICE_DATA=str(data),
                MAGE_SERVICE_PORT=self.url.rsplit(':', 1)[1],
                MAGE_SERVICE_TOKEN=TOKEN,
                MAGE_SERVICE_WORKER=str(
                    directory / 'python/mage_ai/pipeline_services/runtime/worker.py'),
                MAGE_SERVICE_PYTHON=sys.executable,
                PYTHONPATH=os.pathsep.join([str(directory / 'python'), str(REPO)]),
            ),
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                if requests.get(f'{self.url}/healthz', timeout=2).ok:
                    return
            except requests.ConnectionError:
                pass
            if self.process.poll() is not None:
                break
            time.sleep(0.2)
        raise AssertionError(f'The service did not start:\n{self.log.read_text()[-4000:]}')

    def call(self, method: str, route: str, **kwargs):
        response = requests.request(
            method, f'{self.url}{route}', headers={'Authorization': f'Bearer {TOKEN}'},
            timeout=600, **kwargs,
        )
        assert response.ok, f'{response.status_code} {response.text[:2000]}'
        return response.json()

    def run_features(self) -> dict:
        return self.call('POST', '/v1/pipelines/driver_features/runs?wait=300', json={})

    def scoring_after(self, features_run: dict) -> dict:
        """The scoring run the after-run trigger started for this features run."""
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            runs = self.call('GET', '/v1/runs?pipeline=driver_scoring')['runs']
            started = [r for r in runs if r['source'] == 'after:driver_features'
                       and r['created_at'] >= features_run['created_at']]
            done = [r for r in started if r['status'] not in ('queued', 'running')]
            if done:
                return self.call('GET', f"/v1/runs/{done[0]['id']}")
            time.sleep(0.5)
        raise AssertionError(f'No scoring run finished:\n{self.log.read_text()[-4000:]}')

    def stop(self):
        self.process.terminate()
        try:
            self.process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            self.process.kill()


def statuses(run: dict) -> dict:
    return {b['block']: b['status'] for b in run['blocks']}


def test_features_then_scoring_run_as_one_service_with_an_embedded_model(
    mage_project, pg, schema, feast_url, mlflow_url, churn_model, tmp_path, monkeypatch,
):
    from mage_ai.pipeline_services import export
    from mage_ai.pipeline_services.exports import EXPORT_LIMITS, model_registry

    first = 10**12 + secrets.randbelow(10**12)
    drivers = [first + i for i in range(6)]
    trips = create_tables(pg, schema, drivers)

    monkeypatch.setenv('MAGE_TEST_SCHEMA', schema)
    with model_registry(mlflow_url, EXPORT_LIMITS):
        captured = export.capture(
            mage_project, ['driver_features', 'driver_scoring'], name='driver-churn',
            models=[f"churn={churn_model['uri']}"],
        )
        directory = Path(export.write(captured, str(tmp_path / 'service'), force=True))

    # What the export found for pipelines shaped like this.
    files = {str(p.relative_to(captured.project)) for p in captured.files}
    assert {'sql/drivers/trips.sql', 'sql/drivers/pending.sql',
            'utils/driver_sql.py', 'conditionals/driver_has_cases.py'} <= files
    variables = {v['name']: v for v in captured.manifest['environment']}
    assert variables['FEAST_URL']['required'] is True
    assert variables['MAGE_TEST_POSTGRES_HOST']['secret'] is True
    assert 'catboost' in (directory / 'requirements.txt').read_text()
    assert 'requests' in captured.requirements
    [model] = captured.manifest['models']
    assert (model['name'], model['version']) == ('churn', str(churn_model['version']))

    served = Served(directory, tmp_path / 'data', dict(
        FEAST_URL=feast_url,
        MAGE_TEST_SCHEMA=schema,
        MLFLOW_TRACKING_URI='http://127.0.0.1:9',
    ))
    try:
        # First run: six new drivers. Features take the has_new branch, scoring follows.
        features = served.run_features()
        assert features['status'] == 'completed', features
        assert statuses(features)['driver_no_new'] == 'condition_failed'
        assert statuses(features)['driver_push_online'] == 'completed'

        scoring = served.scoring_after(features)
        assert scoring['status'] == 'completed', (scoring['error'], scoring['blocks'])
        assert statuses(scoring)['driver_nothing_to_score'] == 'condition_failed'
        assert statuses(scoring)['driver_store_scores'] == 'completed'

        with pg.cursor() as cursor:
            cursor.execute(f'SELECT driver_id, score, model_version FROM {schema}.churn_scores '
                           'ORDER BY driver_id')
            stored = pd.DataFrame(cursor.fetchall(),
                                  columns=['driver_id', 'score', 'model_version'])
        pg.rollback()
        inputs = pd.DataFrame({
            'conv_rate': (trips['accepted'] / trips['offered']).astype('float32'),
            'acc_rate': trips['accepted'] / trips['trips'],
            'avg_daily_trips': trips['trips'],
        }).astype('float64')
        expected = churn_model['model'].predict_proba(inputs[FEATURES])[:, 1]
        assert stored['driver_id'].tolist() == drivers
        np.testing.assert_allclose(stored['score'].to_numpy(), expected, rtol=1e-6)
        assert set(stored['model_version']) == {churn_model['version']}

        # Second run: no new drivers. Features take the none_new branch, and scoring
        # finds nothing to score; both runs complete.
        features = served.run_features()
        assert features['status'] == 'completed', features
        assert statuses(features)['driver_no_new'] == 'completed'
        skipped = {b['block']: b['error'] for b in features['blocks']
                   if b['status'] == 'condition_failed'}
        assert skipped['driver_window_open'] == 'Condition driver_has_new returned False.'
        assert skipped['driver_push_online'] == \
            'Upstream block driver_save_features was skipped by a condition.'

        scoring = served.scoring_after(features)
        assert scoring['status'] == 'completed', scoring
        assert statuses(scoring)['driver_nothing_to_score'] == 'completed'
        assert statuses(scoring)['driver_score'] == 'condition_failed'
    finally:
        served.stop()
