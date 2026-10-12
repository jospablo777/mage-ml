import os
import shutil
import tempfile
import unittest

from mage_ai.tests.shared.tracking_utils import use_temporary_mlflow

try:
    import mlflow
except ImportError:  # pragma: no cover
    mlflow = None


@unittest.skipIf(mlflow is None, 'MLflow is not installed')
class TemporaryMlflowTest(unittest.TestCase):
    def test_runs_and_models_stay_out_of_the_working_directory(self):
        """
        The MLflow tests put their store in a temporary folder but their artifacts in
        ./mlruns, and a test run left mlruns and mlflow.db in the checkout.
        """
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, True)
        previous = os.getcwd()
        os.chdir(directory)
        self.addCleanup(os.chdir, previous)

        class Identity(mlflow.pyfunc.PythonModel):
            def predict(self, context, model_input, params=None):
                return model_input

        case = unittest.TestCase()
        uri = use_temporary_mlflow(case, experiments=['churn'])
        self.assertEqual(mlflow.get_tracking_uri(), uri)
        mlflow.set_experiment('churn')
        with mlflow.start_run():
            mlflow.log_metric('auc', 0.9)
            mlflow.pyfunc.log_model(
                name='model', python_model=Identity(), registered_model_name='churn',
                pip_requirements=['pandas'],
            )
        with mlflow.start_run(experiment_id='0'):
            mlflow.log_text('x', 'note.txt')
        case.doCleanups()

        self.assertEqual(os.listdir(directory), [])
        self.assertNotEqual(mlflow.get_tracking_uri(), uri)
