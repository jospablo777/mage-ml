import os
import tempfile

import mlflow
import polars as pl

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export(frame: pl.DataFrame, **kwargs) -> None:
    mlflow.set_tracking_uri(kwargs['mlflow_url'])
    mlflow.set_experiment('mage-predictions')
    with mlflow.start_run(run_name=kwargs['run_label']):
        mlflow.set_tag('run_label', kwargs['run_label'])
        mlflow.log_param('model', f"{kwargs['model_name']}@champion")
        mlflow.log_metric('mean_prediction', frame['prediction'].mean())
        mlflow.log_metric('rows', frame.height)
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'predictions.parquet')
            frame.write_parquet(path)
            mlflow.log_artifact(path, artifact_path='outputs')
