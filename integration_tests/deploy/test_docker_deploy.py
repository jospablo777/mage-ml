"""
mage service deploy to the Docker of this machine, end to end: the image is built, the
container answers with its token, a redeploy keeps the previous one, rollback swaps them,
and status reads the labels. Building the image takes minutes, so the test runs only with
MAGE_TEST_DOCKER_DEPLOY=1.
"""
import json
import os
import shutil
import subprocess
import textwrap
import time
import urllib.request
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.getenv('MAGE_TEST_DOCKER_DEPLOY') != '1' or shutil.which('docker') is None,
    reason='set MAGE_TEST_DOCKER_DEPLOY=1 to build and deploy an image',
)


def free_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def cli(project, *arguments):
    from typer.testing import CliRunner

    from mage_ai.cli.main import app

    result = CliRunner().invoke(app, ['service', *arguments, '--project', project])
    assert result.exit_code == 0, result.output
    return result.output


def run_pipeline(port, token, pipeline):
    request = urllib.request.Request(
        f'http://127.0.0.1:{port}/v1/pipelines/{pipeline}/runs?wait=120', data=b'{}',
        method='POST', headers={'Authorization': f'Bearer {token}',
                                'Content-Type': 'application/json'},
    )
    with urllib.request.urlopen(request, timeout=150) as response:
        return json.loads(response.read())


def test_deploy_redeploy_rollback_and_status(mage_project):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.pipeline_services import lifecycle

    suffix = uuid.uuid4().hex[:6]
    name, pipeline_uuid = f'deploy-{suffix}', f'deploy_{suffix}'
    pipeline = Pipeline.create(pipeline_uuid, repo_path=mage_project)
    block = Block.create(f'{pipeline_uuid}_load', 'data_loader', mage_project,
                         language='python', pipeline=pipeline)
    Path(block.file_path).write_text(textwrap.dedent('''
        @data_loader
        def load(*args, **kwargs):
            import os
            return {'region': os.environ.get('REGION')}
    '''))
    pipeline.add_block(block)
    lifecycle.new(mage_project, name, [pipeline_uuid])
    port = free_port()
    environments = Path(mage_project) / 'services' / name / 'environments'
    (environments / 'prod.yaml').write_text(
        f'target: docker\nport: {port}\nvariables:\n  REGION: eu\n',
    )
    container = f'mage-{name}-prod'
    try:
        cli(mage_project, 'deploy', name, '--env', 'prod', '--timeout', '300')
        token = (Path(mage_project) / '.mage' / 'deploy' / name / 'prod' / 'token').read_text()
        run = run_pipeline(port, token, pipeline_uuid)
        assert run['status'] == 'completed', run
        assert 'running the repository version' in cli(mage_project, 'status', name)

        cli(mage_project, 'deploy', name, '--env', 'prod', '--timeout', '300')
        assert 'runs' in cli(mage_project, 'rollback', name, '--env', 'prod')
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                assert run_pipeline(port, token, pipeline_uuid)['status'] == 'completed'
                break
            except OSError:
                time.sleep(2)
        else:
            raise AssertionError('the rolled back deployment did not answer')
    finally:
        for name_ in (container, f'{container}-previous'):
            subprocess.run(['docker', 'rm', '-f', name_], capture_output=True)
        subprocess.run(['docker', 'volume', 'rm', f'{container}-data'], capture_output=True)
        # Only this test's image.
        images = subprocess.run(
            ['docker', 'images', '-q', f'mage-{name}'], capture_output=True, text=True,
        ).stdout.split()
        if images:
            subprocess.run(['docker', 'rmi', '-f', *images], capture_output=True)
        pipeline.delete()
