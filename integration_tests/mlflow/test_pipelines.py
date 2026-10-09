"""
A Mage pipeline that pulls a model from MLflow and pushes its predictions back.

The loader downloads the champion model over HTTP and returns it, so the model crosses
Mage's storage of scikit-learn outputs. The transformer predicts on a Polars frame, and
the exporter logs a run with the predictions as a Parquet artifact through the client.
"""
import io
import uuid

import polars as pl
import pytest
import requests

from integration_tests.mage_runner import run_pipeline
from integration_tests.services.mlflow import expected


def test_predictions_logged_back_to_mlflow(mage_project, mlflow_url):
    label = uuid.uuid4().hex

    run_pipeline('mlflow_models', mlflow_url=mlflow_url, model_name=expected.MODEL,
                 run_label=label)

    api = f'{mlflow_url}/api/2.0/mlflow'
    experiment = requests.get(f'{api}/experiments/get-by-name',
                              params=dict(experiment_name='mage-predictions'), timeout=30).json()
    found = requests.post(f'{api}/runs/search', json=dict(
        experiment_ids=[experiment['experiment']['experiment_id']],
        filter=f"tags.run_label = '{label}'",
    ), timeout=30).json()['runs']
    assert len(found) == 1
    run = found[0]
    metrics = {m['key']: m['value'] for m in run['data']['metrics']}
    root = run['info']['artifact_uri'][len('mlflow-artifacts:/'):]
    artifact = requests.get(
        f'{mlflow_url}/api/2.0/mlflow-artifacts/artifacts/{root}/outputs/predictions.parquet',
        timeout=30,
    )
    frame = pl.read_parquet(io.BytesIO(artifact.content))

    rows = [[float(i), float(i % 4)] for i in range(10)]
    predictions = expected.target(rows, expected.INTERCEPTS['train-v1'])
    assert frame['prediction'].to_list() == pytest.approx(predictions)
    assert frame['id'].to_list() == list(range(10))
    assert metrics['rows'] == 10
    assert metrics['mean_prediction'] == pytest.approx(sum(predictions) / 10)
