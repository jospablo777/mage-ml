"""
Models embedded in a service: loaded as MLflow pyfunc or as the framework's own object,
and the Feast feature service they were trained with, read from their tags.
"""
import importlib.util
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mage_ai.pipeline_services import model_export, models
from mage_ai.tests.shared.tracking_utils import use_temporary_mlflow


class EmbeddedModelTest(unittest.TestCase):
    def setUp(self):
        import mlflow
        import numpy as np
        from sklearn.linear_model import LogisticRegression

        use_temporary_mlflow(self, experiments=['churn'])
        mlflow.set_experiment('churn')
        features = np.array([[0.0], [1.0], [2.0], [3.0]])
        classifier = LogisticRegression().fit(features, [0, 0, 1, 1])
        with mlflow.start_run():
            # Feast's MLflow integration tags the training run this way.
            mlflow.set_tag('feast.feature_service', 'churn_features_v1')
            mlflow.sklearn.log_model(
                classifier, name='model', registered_model_name='churn-classifier',
            )

        self.models_dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.models_dir, True)
        model_export.download('churn', 'models:/churn-classifier/1', self.models_dir / 'churn')
        patcher = patch.dict(os.environ, {
            models.MODELS_DIR_ENV: str(self.models_dir),
            'MAGE_SERVICE': '1',
        })
        patcher.start()
        self.addCleanup(patcher.stop)
        models._loaded.clear()
        self.addCleanup(models._loaded.clear)

    def test_the_native_flavor_gives_the_frameworks_object(self):
        native = models.load_model('churn', flavor='native')
        pyfunc = models.load_model('churn')

        self.assertEqual(type(native).__name__, 'LogisticRegression')
        self.assertEqual(native.predict_proba([[3.0]]).shape, (1, 2))
        self.assertFalse(hasattr(pyfunc, 'predict_proba'))
        self.assertIs(models.load_model('churn', flavor='sklearn').__class__, native.__class__)

    def test_the_feature_service_comes_from_the_training_runs_tag(self):
        self.assertEqual(models.feature_service('churn'), 'churn_features_v1')

    def test_a_model_the_service_lacks_says_how_to_add_it(self):
        with self.assertRaisesRegex(models.ModelNotFound, '--model other=<uri>'):
            models.load_model('other', flavor='native')


@unittest.skipUnless(importlib.util.find_spec('catboost'), 'catboost is not installed')
class CatBoostFileTest(unittest.TestCase):
    """A CatBoost model logged as a .cbm file, with no MLmodel, as production teams often do."""

    def setUp(self):
        import catboost
        import mlflow
        import numpy as np
        from mlflow.tracking import MlflowClient

        use_temporary_mlflow(self, experiments=['churn'])
        mlflow.set_experiment('churn')
        self.features = np.array([[0.0], [1.0], [2.0], [3.0]] * 5)
        options = dict(iterations=5, verbose=False, allow_writing_files=False)
        classifier = catboost.CatBoostClassifier(**options)
        classifier.fit(self.features, [0, 0, 1, 1] * 5)
        folder = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, folder, True)
        classifier.save_model(str(folder / 'churn.cbm'))
        with mlflow.start_run() as run:
            mlflow.log_artifact(str(folder / 'churn.cbm'))
        client = MlflowClient()
        client.create_registered_model('churn-classifier')
        client.create_model_version(
            'churn-classifier', f'{run.info.artifact_uri}/churn.cbm', run_id=run.info.run_id,
        )
        client.set_registered_model_alias('churn-classifier', 'champion', '1')

        self.models_dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.models_dir, True)
        models._loaded.clear()
        self.addCleanup(models._loaded.clear)

    def test_the_embedded_file_loads_as_a_classifier_with_predict_proba(self):
        model_export.download(
            'churn', 'models:/churn-classifier@champion', self.models_dir / 'churn',
        )
        with patch.dict(os.environ, {models.MODELS_DIR_ENV: str(self.models_dir),
                                     'MAGE_SERVICE': '1'}):
            model = models.load_model('churn', flavor='native')
            metadata = models.model_metadata('churn')

        self.assertEqual(type(model).__name__, 'CatBoostClassifier')
        self.assertEqual(model.predict_proba(self.features[:2]).shape, (2, 2))
        self.assertEqual((metadata['version'], metadata['aliases']), ('1', ['champion']))

    def test_mageml_downloads_the_same_file_from_the_registry(self):
        with patch.dict(os.environ):
            os.environ.pop('MAGE_SERVICE', None)
            os.environ.pop(models.MODELS_DIR_ENV, None)
            model = models.load_model(
                'churn', uri='models:/churn-classifier@champion', flavor='native',
            )
        self.assertTrue(hasattr(model, 'predict_proba'))

    def test_a_regressor_file_loads_as_a_regressor(self):
        import catboost

        regressor = catboost.CatBoostRegressor(
            iterations=5, verbose=False, allow_writing_files=False)
        regressor.fit(self.features, [0.1, 0.2, 0.3, 0.4] * 5)
        path = self.models_dir / 'amount.cbm'
        regressor.save_model(str(path))

        self.assertEqual(type(models.load_artifact(str(path))).__name__, 'CatBoostRegressor')
