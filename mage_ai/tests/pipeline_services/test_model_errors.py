"""What the export says when a model cannot be downloaded: the cause, not MLflow's header."""
import unittest

from mage_ai.pipeline_services import model_export


class MlflowException(Exception):
    pass


MISSING = MlflowException(
    'The following failures occurred while downloading one or more artifacts from '
    'http://mlflow:5000/api/2.0/mlflow-artifacts/artifacts/5/ce51e0/artifacts:\n'
    '##### File churn.cbm #####\n'
    '404 Client Error: Not Found for url: '
    'http://mlflow:5000/api/2.0/mlflow-artifacts/artifacts/5/ce51e0/artifacts/churn.cbm. '
    'Response text: {"error_code":"RESOURCE_DOES_NOT_EXIST",'
    '"message":"No such artifact: \'5/ce51e0/artifacts/churn.cbm\'"}'
)


class ExplainTest(unittest.TestCase):
    def test_a_version_whose_files_are_gone_names_the_file(self):
        """
        The export said only "...downloading one or more artifacts from <url>:", cutting
        the line that said which file was missing.
        """
        text = model_export.explain(MISSING, 'models:/churn-scorer@champion',
                                    'http://mlflow:5000')

        self.assertIn('churn.cbm', text)
        self.assertIn('missing from the artifact store', text)
        self.assertIn('Log the model again, or pick another version', text)

    def test_an_unknown_error_keeps_the_line_that_says_why(self):
        error = RuntimeError('These steps failed:\n\nthe cache folder is read-only')

        self.assertEqual(
            model_export.explain(error, 'runs:/abc/model'),
            'RuntimeError: These steps failed: the cache folder is read-only',
        )


if __name__ == '__main__':
    unittest.main()
