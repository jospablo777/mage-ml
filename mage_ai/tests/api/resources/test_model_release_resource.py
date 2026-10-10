"""
Model releases through the API: the policy, champion and log of a model, a promotion,
and a promotion refused because the champion changed since the evaluation.
"""
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user

try:
    import mlflow
except ImportError:  # pragma: no cover
    mlflow = None


@unittest.skipIf(mlflow is None, 'MLflow is not installed')
class ModelReleaseResourceTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        self.owner = create_user(_owner=True)
        self.tracking = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tracking, True)
        uri = f'sqlite:///{self.tracking}/mlflow.db'
        os.environ['MLFLOW_TRACKING_URI'] = uri
        self.addCleanup(os.environ.pop, 'MLFLOW_TRACKING_URI', None)
        mlflow.set_tracking_uri(uri)
        self.model = f'api_model_{self.faker.unique.random_int(1, 10**9)}'
        policy = Path(self.repo_path, 'releases', f'{self.model}.yaml')
        policy.parent.mkdir(exist_ok=True)
        policy.write_text('rules:\n  - {metric: auc, min: 0.5}\n')
        self.addCleanup(policy.unlink, missing_ok=True)

        class Identity(mlflow.pyfunc.PythonModel):
            def predict(self, context, model_input, params=None):
                return model_input

        for _ in range(2):
            with mlflow.start_run():
                mlflow.log_metric('auc', 0.9)
                mlflow.pyfunc.log_model(
                    name='model', python_model=Identity(), registered_model_name=self.model,
                    pip_requirements=['pandas'],
                )

    async def call(self, action, **options):
        return await BaseOperation(
            action=action, resource='model_releases', user=self.owner, **options,
        ).execute()

    async def test_a_version_is_promoted_and_a_stale_approval_refused(self):
        shown = (await self.call(OperationType.DETAIL, pk=self.model))['model_release']
        self.assertEqual((shown['champion'], shown['alias']), (None, 'champion'))

        promoted = (await self.call(OperationType.CREATE, payload=dict(model_release=dict(
            action='promote', model=self.model, version='1', expected_champion=None,
        ))))['model_release']
        self.assertEqual(promoted['champion'], '1')
        self.assertEqual(promoted['log'][-1]['actor'], self.owner.email)

        refused = await self.call(OperationType.CREATE, payload=dict(model_release=dict(
            action='promote', model=self.model, version='2', expected_champion=None,
        )))
        self.assertIn('evaluate it again', refused['error']['message'])
