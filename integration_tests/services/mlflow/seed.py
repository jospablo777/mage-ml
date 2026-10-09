"""Seed the MLflow server with an experiment, two runs and a registered model."""
import os
import tempfile

import expected
import mlflow
import pandas as pd
from mlflow import MlflowClient
from mlflow.models import infer_signature
from PIL import Image
from sklearn.linear_model import LinearRegression

mlflow.set_tracking_uri('http://127.0.0.1:5000')
client = MlflowClient()

if client.get_experiment_by_name(expected.EXPERIMENT) is None:
    experiment_id = client.create_experiment(
        expected.EXPERIMENT, tags={'team': 'data', 'purpose': 'integration tests'},
    )
    mlflow.set_experiment(expected.EXPERIMENT)
    rows = pd.DataFrame(expected.training_rows(), columns=['x1', 'x2'])

    for version, run_name in enumerate(expected.RUNS, start=1):
        intercept = expected.INTERCEPTS[run_name]
        y = expected.target(expected.training_rows(), intercept)
        model = LinearRegression().fit(rows, y)
        with mlflow.start_run(run_name=run_name) as run:
            mlflow.log_params(expected.PARAMS[run_name])
            for step, value in enumerate(expected.rmse_history(run_name)):
                mlflow.log_metric('rmse', value, step=step)
            mlflow.log_metric('r2', 1.0)
            mlflow.set_tags({'stage': run_name, 'version': str(version)})

            with tempfile.TemporaryDirectory() as directory:
                for path, content in expected.text_files().items():
                    full = os.path.join(directory, path)
                    os.makedirs(os.path.dirname(full), exist_ok=True)
                    with open(full, 'w', encoding='utf-8') as f:
                        f.write(content)
                with open(os.path.join(directory, 'large.bin'), 'wb') as f:
                    f.write(expected.large_file())
                rows.to_parquet(os.path.join(directory, 'data', 'sample.parquet'))
                image = Image.new('RGB', (8, 4), (10, 20, 30))
                os.makedirs(os.path.join(directory, 'plots'))
                image.save(os.path.join(directory, 'plots', 'swatch.png'))
                mlflow.log_artifacts(directory)

            example = rows.head(3)
            mlflow.sklearn.log_model(
                model,
                name='model',
                serialization_format=expected.SERIALIZATION[run_name],
                input_example=example,
                signature=infer_signature(example, model.predict(example)),
                registered_model_name=expected.MODEL,
            )
        client.set_model_version_tag(expected.MODEL, str(version), 'validated', 'true')

    client.update_registered_model(expected.MODEL, description='Linear model for tests')
    client.set_registered_model_tag(expected.MODEL, 'owner', 'mage')
    for alias, version in expected.ALIASES.items():
        client.set_registered_model_alias(expected.MODEL, alias, str(version))

with open('/mlflow/seeded', 'w') as f:
    f.write('ok')
