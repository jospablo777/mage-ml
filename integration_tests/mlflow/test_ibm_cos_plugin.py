"""
The standalone mlflow-ibm-cos package, in an environment without mage-ml, as an MLflow
microservice installs it: MLflow 2 and 3 log and download artifacts at cos:// locations and at
s3:// locations on the COS endpoint, with IAM tokens.
"""
import json
import os
import subprocess
from pathlib import Path

import pytest

from mage_ai.tests.services.ibm.fixtures import FakeIBMCloud
from mage_ai.tests.services.ibm.test_mlflow_cos import SCRIPT

PACKAGE = Path(__file__).resolve().parents[2] / 'packages' / 'mlflow-ibm-cos'


@pytest.fixture(scope='module')
def wheel(tmp_path_factory):
    out = tmp_path_factory.mktemp('dist')
    subprocess.run(['uv', 'build', '--wheel', str(PACKAGE), '-o', str(out)], check=True,
                   capture_output=True)
    return next(out.glob('*.whl'))


def environment(tmp_path, wheel, mlflow):
    venv = tmp_path / 'venv'
    subprocess.run(['uv', 'venv', '-q', '--python', '3.12', str(venv)], check=True)
    python = venv / 'bin' / 'python'
    subprocess.run(['uv', 'pip', 'install', '-q', '--python', str(python), str(wheel),
                    f'mlflow-skinny{mlflow}', 'sqlalchemy<2.1', 'alembic'], check=True)
    found = subprocess.run([str(python), '-I', '-c', 'import mage_ai'], capture_output=True)
    assert found.returncode != 0, 'mage-ml must not be installed'
    return python


@pytest.mark.parametrize('mlflow', ['>=2.22,<3', '>=3,<4'])
@pytest.mark.parametrize('location', ['cos://models/plugin', 's3://models/plugin'])
def test_mlflow_without_mage_stores_artifacts_in_cos(tmp_path, wheel, mlflow, location):
    python = environment(tmp_path, wheel, mlflow)
    cloud = FakeIBMCloud()
    try:
        result = subprocess.run(
            [str(python), '-I', '-c', SCRIPT, location, str(tmp_path)],
            env=dict(os.environ, MLFLOW_DISABLE_AGENT_HINT='1', IBM_COS_ENDPOINT=cloud.url,
                     IBM_CLOUD_API_KEY='key-1', IBM_CLOUD_IAM_URL=cloud.url,
                     MLFLOW_S3_ENDPOINT_URL=cloud.url, AWS_ACCESS_KEY_ID='',
                     AWS_SECRET_ACCESS_KEY=''),
            capture_output=True, text=True, timeout=300,
        )
    finally:
        cloud.close()

    assert result.returncode == 0, result.stderr[-3000:]
    out = json.loads(result.stdout.strip().splitlines()[-1])
    assert out['content'] == 'weights: 0.25 é'
    assert cloud.requests
    for method, path, headers, _ in cloud.requests:
        auth = {k.lower(): v for k, v in headers.items()}['authorization']
        assert auth.startswith('Bearer token-'), (method, path)
