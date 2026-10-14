"""
MLflow artifacts in COS: runs log to and download from cos:// locations through the plugin
MageML registers, with IAM tokens; s3:// locations on a COS endpoint get the boto3 hook, so
MLflow's own client works without patches. MLflow runs in a subprocess, as a fresh process
loads its plugins.
"""
import json
import os
import subprocess
import sys
import textwrap
import unittest

from mage_ai.tests.services.ibm.fixtures import FakeIBMCloud

SCRIPT = textwrap.dedent('''
    import json, os, sys, tempfile
    import mlflow

    location = sys.argv[1]
    mlflow.set_tracking_uri('sqlite:///' + os.path.join(sys.argv[2], 'mlflow.db'))
    experiment = mlflow.create_experiment('cos', artifact_location=location)
    with mlflow.start_run(experiment_id=experiment) as run:
        path = os.path.join(sys.argv[2], 'model.txt')
        with open(path, 'w') as f:
            f.write('weights: 0.25 \\u00e9')
        mlflow.log_artifact(path, artifact_path='model')
    target = tempfile.mkdtemp()
    local = mlflow.artifacts.download_artifacts(
        run_id=run.info.run_id, artifact_path='model/model.txt', dst_path=target,
    )
    print(json.dumps(dict(uri=run.info.artifact_uri, content=open(local).read())))
''')


class MLflowCOSTest(unittest.TestCase):
    def setUp(self):
        self.cloud = FakeIBMCloud()
        self.addCleanup(self.cloud.close)

    def run_mlflow(self, location, tmp, **env):
        result = subprocess.run(
            [sys.executable, '-c', SCRIPT, location, tmp],
            env=dict(os.environ, MLFLOW_DISABLE_AGENT_HINT='1', IBM_COS_ENDPOINT=self.cloud.url,
                     IBM_CLOUD_API_KEY='key-1', IBM_CLOUD_IAM_URL=self.cloud.url,
                     AWS_ACCESS_KEY_ID='', AWS_SECRET_ACCESS_KEY='', **env),
            capture_output=True, text=True, timeout=180,
        )
        self.assertEqual(result.returncode, 0, result.stderr[-3000:])
        return json.loads(result.stdout.strip().splitlines()[-1])

    def assert_iam_only(self):
        self.assertTrue(self.cloud.requests)
        for method, path, headers, _ in self.cloud.requests:
            auth = {k.lower(): v for k, v in headers.items()}['authorization']
            self.assertTrue(auth.startswith('Bearer token-'), (method, path, auth))

    def test_a_run_logs_and_downloads_artifacts_at_a_cos_location(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            out = self.run_mlflow('cos://models/mlflow', tmp)

        self.assertEqual(out['content'], 'weights: 0.25 é')
        self.assertTrue(out['uri'].startswith('cos://models/mlflow/'))
        self.assertIn('/models/mlflow/', self.cloud.requests[0][1])
        self.assert_iam_only()

    def test_an_s3_location_on_the_cos_endpoint_uses_iam_without_patches(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            out = self.run_mlflow('s3://models/legacy', tmp,
                                  MLFLOW_S3_ENDPOINT_URL=self.cloud.url)

        self.assertEqual(out['content'], 'weights: 0.25 é')
        self.assert_iam_only()
