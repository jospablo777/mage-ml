"""
Experiments: MLflow runs a block starts carry the Mage run's tags, the block run records
them with their metrics and registered model versions, and two pipeline runs compare by
their MLflow metrics.
"""
import os
import shutil
import tempfile
import textwrap
import tomllib
import unittest
from pathlib import Path

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import experiments, run_records
from mage_ai.orchestration.fusion_verify import run_in_process
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import DBTestCase

try:
    import mlflow
except ImportError:  # pragma: no cover
    mlflow = None

TRAIN = '''
import mlflow
import pandas as pd


class Scaler(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input, params=None):
        return model_input


@transformer
def train(*args, **kwargs):
    rate = float(kwargs.get('rate', 0.1))
    mlflow.set_experiment('churn')
    with mlflow.start_run(run_name='train'):
        mlflow.log_param('rate', rate)
        mlflow.log_metric('auc', 0.5 + rate)
        mlflow.pyfunc.log_model(
            name='model', python_model=Scaler(), registered_model_name='churn_model',
            pip_requirements=['pandas'],
        )
    return pd.DataFrame({'rate': [rate]})
'''


@unittest.skipIf(mlflow is None, 'MLflow is not installed')
class ExperimentsTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.tracking = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tracking, True)
        uri = f'sqlite:///{self.tracking}/mlflow.db'
        os.environ['MLFLOW_TRACKING_URI'] = uri
        self.addCleanup(os.environ.pop, 'MLFLOW_TRACKING_URI', None)
        mlflow.set_tracking_uri(uri)
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        block = Block.create(
            f'{self.pipeline.uuid}_train', 'transformer', self.repo_path, pipeline=self.pipeline,
        )
        Path(block.file_path).write_text(textwrap.dedent(TRAIN))
        self.block_uuid = block.uuid

    def run_pipeline(self, **variables):
        run = trigger_pipeline(self.pipeline.uuid, variables=variables)
        return run_in_process(run, log=lambda _line: None)

    def test_a_block_run_records_the_mlflow_runs_it_started(self):
        run = self.run_pipeline(rate=0.25)
        block_run = next(b for b in run.block_runs if b.block_uuid == self.block_uuid)
        recorded = block_run.metrics['mlflow']

        self.assertEqual(len(recorded['runs']), 1)
        tracked = recorded['runs'][0]
        self.assertEqual(tracked['run_name'], 'train')
        self.assertEqual(tracked['metrics'], {'auc': 0.75})
        self.assertEqual(tracked['params'], 1)
        self.assertEqual(tracked['model_versions'], [dict(name='churn_model', version='1')])

        tags = mlflow.get_run(tracked['run_id']).data.tags
        self.assertEqual(tags['mage.pipeline_uuid'], self.pipeline.uuid)
        self.assertEqual(tags['mage.pipeline_run_id'], str(run.id))
        self.assertEqual(tags['mage.block_run_id'], str(block_run.id))
        self.assertEqual(tags['mage.block_uuid'], self.block_uuid)
        self.assertEqual(
            tags['mage.code_digest'], run_records.manifest(self.pipeline, run.id)['code']['digest'],
        )

    def test_two_runs_compare_by_their_mlflow_metrics(self):
        first = self.run_pipeline(rate=0.1)
        second = self.run_pipeline(rate=0.3)

        comparison = run_records.compare(self.pipeline, first, second)

        block = next(o for o in comparison['outputs'] if o['block_uuid'] == self.block_uuid)
        self.assertEqual(block['metrics'], {'auc': [0.6, 0.8]})
        self.assertIn('MLflow metric auc: 0.6 -> 0.8', run_records.format_comparison(comparison))

    def test_runs_outside_a_block_get_no_mage_tags(self):
        with experiments.block_context(dict(block_run_id=7)):
            self.assertEqual(experiments.current_tags(), {'mage.block_run_id': '7'})
        self.assertEqual(experiments.current_tags(), {})
        with mlflow.start_run() as outside:
            pass
        self.assertNotIn('mage.block_run_id', mlflow.get_run(outside.info.run_id).data.tags)

    def test_mlflow_finds_the_provider_through_the_package_entry_point(self):
        pyproject = Path(__file__).resolve().parents[3] / 'pyproject.toml'
        entry_points = tomllib.loads(pyproject.read_text())['project']['entry-points']
        target = entry_points['mlflow.run_context_provider']['mage']
        module, name = target.split(':')
        self.assertEqual(getattr(__import__(module, fromlist=[name]), name).__name__, name)

    def test_a_password_in_the_tracking_uri_is_not_recorded(self):
        self.assertEqual(
            experiments._safe_uri('https://user:secret@mlflow.example.com:5000/path'),
            'https://user@mlflow.example.com:5000/path',
        )
