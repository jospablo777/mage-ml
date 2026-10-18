"""
mage service deploy to a kind cluster, end to end: the image loaded into kind, the objects
applied, a run through the port-forward, a deploy missing a required variable undone with
its Secret, and rollback. Needs a kind cluster made for the test:

    kind create cluster --name mage-deploy-test
    MAGE_TEST_KIND_CLUSTER=mage-deploy-test pytest integration_tests/deploy

The test makes its own namespace and deletes it with its image afterwards.
"""
import json
import os
import shutil
import subprocess
import textwrap
import urllib.request
import uuid
from pathlib import Path

import pytest

CLUSTER = os.getenv('MAGE_TEST_KIND_CLUSTER')
pytestmark = pytest.mark.skipif(
    not CLUSTER or shutil.which('kubectl') is None or shutil.which('kind') is None,
    reason='set MAGE_TEST_KIND_CLUSTER to a kind cluster made for the test',
)


def kubectl(*arguments, check=True):
    return subprocess.run(['kubectl', '--context', f'kind-{CLUSTER}', *arguments],
                          capture_output=True, text=True, check=check)


def cli(project, *arguments, code=0):
    from typer.testing import CliRunner

    from mage_ai.cli.main import app

    result = CliRunner().invoke(app, ['service', *arguments, '--project', project])
    assert result.exit_code == code, result.output
    return result.output


def run_pipeline(project, name, pipeline):
    from mage_ai.pipeline_services import client, kubernetes, local_run

    settings = local_run.environment_settings(project, name, 'prod')
    api = client.endpoint(project, name, 'prod',
                          target=kubernetes.KubernetesTarget(settings, environment='prod'))
    try:
        request = urllib.request.Request(
            f'{api.url}/v1/pipelines/{pipeline}/runs?wait=120', data=b'{}', method='POST',
            headers={'Authorization': f'Bearer {api.token}',
                     'Content-Type': 'application/json'},
        )
        with urllib.request.urlopen(request, timeout=150) as response:
            return json.loads(response.read())
    finally:
        api.close()


def secret_of(name):
    item = json.loads(kubectl('get', 'deployment', name, '--namespace', NAMESPACE,
                              '--output', 'json').stdout)
    return item['spec']['template']['spec']['containers'][0]['envFrom'][0]['secretRef']['name']


NAMESPACE = f'mage-test-{uuid.uuid4().hex[:6]}'


def test_deploy_undo_and_rollback_on_kind(mage_project):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.pipeline_services import lifecycle

    suffix = uuid.uuid4().hex[:6]
    name, pipeline_uuid = f'kube-{suffix}', f'kube_{suffix}'
    pipeline = Pipeline.create(pipeline_uuid, repo_path=mage_project)
    block = Block.create(f'{pipeline_uuid}_load', 'data_loader', mage_project,
                         language='python', pipeline=pipeline)
    Path(block.file_path).write_text(textwrap.dedent('''
        @data_loader
        def load(*args, **kwargs):
            import os
            return {'region': os.environ['REGION']}
    '''))
    pipeline.add_block(block)
    lifecycle.new(mage_project, name, [pipeline_uuid])
    prod = Path(mage_project) / 'services' / name / 'environments' / 'prod.yaml'

    def environment(**variables):
        prod.write_text(
            f'target: kubernetes\ncontext: kind-{CLUSTER}\nnamespace: {NAMESPACE}\n'
            f'kind_cluster: {CLUSTER}\nvariables: {json.dumps(variables)}\n'
        )

    kubectl('create', 'namespace', NAMESPACE)
    deployment = f'mage-{name}-prod'
    try:
        environment(REGION='eu')
        cli(mage_project, 'deploy', name, '--env', 'prod', '--timeout', '240')
        run = run_pipeline(mage_project, name, pipeline_uuid)
        assert run['status'] == 'completed', run
        assert 'running the repository version' in cli(mage_project, 'status', name)
        assert run['id'] in cli(mage_project, 'runs', name, '--env', 'prod')
        first = secret_of(deployment)

        # REGION is required: the engine refuses to start without it, and the undo brings
        # back the Secret that has it.
        environment()
        failed = cli(mage_project, 'deploy', name, '--env', 'prod', '--timeout', '60', code=1)
        assert 'the previous revision runs again' in failed, failed
        assert 'REGION' in failed, failed
        kubectl('rollout', 'status', f'deployment/{deployment}', '--namespace', NAMESPACE,
                '--timeout=120s')
        assert secret_of(deployment) == first
        assert run_pipeline(mage_project, name, pipeline_uuid)['status'] == 'completed'

        environment(REGION='us')
        cli(mage_project, 'deploy', name, '--env', 'prod', '--timeout', '240')
        second = secret_of(deployment)
        assert second != first
        cli(mage_project, 'rollback', name, '--env', 'prod')
        assert secret_of(deployment) == first
    finally:
        kubectl('delete', 'namespace', NAMESPACE, '--wait=false', check=False)
        images = subprocess.run(['docker', 'images', '-q', f'mage-{name}'],
                                capture_output=True, text=True).stdout.split()
        if images:
            subprocess.run(['docker', 'rmi', '-f', *images], capture_output=True)
        pipeline.delete()
