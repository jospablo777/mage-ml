"""
The Docker deploy adapter against a fake Docker: labels, health, rollback on a failed
deploy, rollback on request, status and drift, and secrets kept off the command line.
"""
import json
import subprocess
from typing import Dict, List

from mage_ai.pipeline_services import deploy
from mage_ai.tests.base_test import TestCase


class FakeDocker:
    """Containers by name: {'image', 'labels', 'running', 'env'}."""

    def __init__(self):
        self.containers: Dict[str, Dict] = {}
        self.commands: List[List[str]] = []

    def __call__(self, arguments):
        self.commands.append(arguments)
        command = arguments[0]

        def ok(stdout=''):
            return subprocess.CompletedProcess(arguments, 0, stdout, '')

        def missing():
            return subprocess.CompletedProcess(arguments, 1, '', 'No such container')

        if command == 'container' and arguments[1] == 'inspect':
            found = [self._inspect(n) for n in arguments[2:] if n in self.containers]
            return ok(json.dumps(found)) if found else missing()
        if command == 'rm':
            self.containers.pop(arguments[-1], None)
            return ok()
        if command in ('stop', 'start'):
            if arguments[1] not in self.containers:
                return missing()
            self.containers[arguments[1]]['running'] = command == 'start'
            return ok()
        if command == 'rename':
            self.containers[arguments[2]] = self.containers.pop(arguments[1])
            return ok()
        if command == 'run':
            name = arguments[arguments.index('--name') + 1]
            labels = dict(
                arguments[i + 1].split('=', 1)
                for i, a in enumerate(arguments) if a == '--label'
            )
            env_file = arguments[arguments.index('--env-file') + 1]
            with open(env_file) as file:
                env = dict(line.strip().split('=', 1) for line in file if line.strip())
            self.containers[name] = dict(image=arguments[-1], labels=labels, running=True,
                                         env=env)
            return ok('id')
        if command == 'ps':
            return ok('\n'.join(self.containers))
        if command == 'logs':
            return ok('engine log line')
        raise AssertionError(arguments)

    def _inspect(self, name):
        container = self.containers[name]
        return dict(Name=f'/{name}', Config=dict(
                        Labels=container['labels'],
                        Env=[f'{k}={v}' for k, v in container['env'].items()]),
                    State=dict(Status='running' if container['running'] else 'exited'))


class DockerTargetTest(TestCase):
    def setUp(self):
        super().setUp()
        self.docker = FakeDocker()
        self.healthy = True
        self.target = deploy.DockerTarget(
            runner=self.docker, health=lambda port, timeout: self.healthy,
        )

    def deploy(self, image, lock):
        return self.target.deploy(
            'shop', 'prod', image, 7001, env=dict(API_TOKEN='s3cr3t'),
            labels=dict(commit='abc123', lock=lock), timeout=1,
        )

    def test_a_deploy_labels_the_container_and_keeps_secrets_off_the_command_line(self):
        deployment = self.deploy('mage-shop:1', 'lock-1')

        self.assertEqual(
            (deployment.state, deployment.image, deployment.commit, deployment.lock),
            ('running', 'mage-shop:1', 'abc123', 'lock-1'),
        )
        self.assertEqual(self.docker.containers['mage-shop-prod']['env'],
                         dict(API_TOKEN='s3cr3t'))
        self.assertFalse(any('s3cr3t' in ' '.join(c) for c in self.docker.commands))

    def test_a_redeploy_keeps_the_previous_one_for_rollback(self):
        self.deploy('mage-shop:1', 'lock-1')
        self.deploy('mage-shop:2', 'lock-2')

        self.assertEqual(self.docker.containers['mage-shop-prod']['image'], 'mage-shop:2')
        self.assertEqual(self.docker.containers['mage-shop-prod-previous']['image'],
                         'mage-shop:1')
        self.assertFalse(self.docker.containers['mage-shop-prod-previous']['running'])

        restored = self.target.rollback('shop', 'prod')

        self.assertEqual(restored.image, 'mage-shop:1')
        self.assertTrue(self.docker.containers['mage-shop-prod']['running'])
        self.assertEqual(self.docker.containers['mage-shop-prod-previous']['image'],
                         'mage-shop:2')

    def test_a_deploy_that_fails_health_brings_the_previous_one_back(self):
        self.deploy('mage-shop:1', 'lock-1')
        self.healthy = False

        with self.assertRaisesRegex(deploy.DeployError, 'previous deployment runs again'):
            self.deploy('mage-shop:2', 'lock-2')

        current = self.docker.containers['mage-shop-prod']
        self.assertEqual((current['image'], current['running']), ('mage-shop:1', True))
        self.assertNotIn('mage-shop-prod-previous', self.docker.containers)

    def test_status_shows_drift_from_the_repository(self):
        self.deploy('mage-shop:1', 'lock-1')
        self.deploy('mage-shop:2', 'lock-2')

        found = self.target.status('shop')

        self.assertEqual([(d.service, d.environment) for d in found], [('shop', 'prod')])
        self.assertEqual(found[0].drift('lock-2'), 'running the repository version')
        self.assertEqual(found[0].drift('lock-3'), 'behind the repository')

    def test_rollback_without_a_previous_deployment_says_so(self):
        self.deploy('mage-shop:1', 'lock-1')

        with self.assertRaisesRegex(deploy.DeployError, 'no previous deployment'):
            self.target.rollback('shop', 'prod')


class DeployCommandTest(TestCase):
    """mage service deploy, status, diff and rollback with a fake Docker."""

    def setUp(self):
        super().setUp()
        import shutil
        from pathlib import Path


        self.project = Path(self.repo_path)
        folder = self.project / 'services' / 'shop' / 'environments'
        folder.mkdir(parents=True, exist_ok=True)
        (folder / 'prod.yaml').write_text('target: docker\nport: 7001\nschedules: on\n'
                                          'variables:\n  REGION: eu\n')
        (folder.parent / '.env.prod').write_text('API_KEY=k3y\n')
        self.addCleanup(shutil.rmtree, self.project / 'services', True)
        self.addCleanup(shutil.rmtree, self.project / '.mage', True)
        self.docker = FakeDocker()
        self.uncommitted = []

    def invoke(self, *arguments):
        from unittest.mock import MagicMock, patch

        from typer.testing import CliRunner

        from mage_ai.cli.main import app
        from mage_ai.pipeline_services import lifecycle

        target = deploy.DockerTarget(runner=self.docker, health=lambda port, timeout: True)
        check = MagicMock(ok=True, problems=[])
        with patch.object(deploy, 'DockerTarget', return_value=target), \
                patch.object(lifecycle, 'check', return_value=check), \
                patch.object(lifecycle, 'build', return_value=self.project / '.mage' / 'b'), \
                patch.object(lifecycle, 'read_lock',
                             return_value=dict(manifest_sha256='a' * 64)), \
                patch('mage_ai.pipeline_services.export.build_image') as build_image, \
                patch.object(lifecycle, 'uncommitted_inputs', return_value=self.uncommitted), \
                patch.object(deploy, 'git_commit', return_value='c0ffee'):
            result = CliRunner().invoke(app, ['service', *arguments, '--project',
                                              self.repo_path])
        return result, build_image

    def test_deploy_builds_runs_with_the_environment_and_reports(self):
        result, build_image = self.invoke('deploy', 'shop', '--env', 'prod')

        self.assertEqual(result.exit_code, 0, result.output)
        build_image.assert_called_once()
        self.assertEqual(build_image.call_args.args[1], 'mage-shop:aaaaaaaaaaaa')
        container = self.docker.containers['mage-shop-prod']
        self.assertEqual(container['env']['REGION'], 'eu')
        self.assertEqual(container['env']['API_KEY'], 'k3y')
        self.assertEqual(container['env']['MAGE_SERVICE_SCHEDULES'], 'on')
        self.assertTrue(container['env']['MAGE_SERVICE_TOKEN'])
        self.assertEqual(container['labels']['ai.mageml.commit'], 'c0ffee')
        self.assertIn('http://127.0.0.1:7001', result.output)

        status, _ = self.invoke('status', 'shop')
        self.assertIn('shop  prod  running the repository version', status.output)

    def test_the_token_survives_a_deploy_from_another_machine(self):
        """CI has no token file: every deploy from it made a new token, breaking clients."""
        self.invoke('deploy', 'shop', '--env', 'prod')
        token = self.docker.containers['mage-shop-prod']['env']['MAGE_SERVICE_TOKEN']
        token_file = deploy.token_file(self.repo_path, 'shop', 'prod')
        self.assertEqual((token_file.parent.parent.parent / '.gitignore').read_text(), '*\n')
        token_file.unlink()

        result, _ = self.invoke('deploy', 'shop', '--env', 'prod')

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(self.docker.containers['mage-shop-prod']['env']['MAGE_SERVICE_TOKEN'],
                         token)
        self.assertEqual(token_file.read_text(), token)

        # A token the environment sets wins.
        (self.project / 'services' / 'shop' / '.env.prod').write_text(
            'API_KEY=k3y\nMAGE_SERVICE_TOKEN=from-ci\n')
        self.invoke('deploy', 'shop', '--env', 'prod')
        self.assertEqual(self.docker.containers['mage-shop-prod']['env']['MAGE_SERVICE_TOKEN'],
                         'from-ci')
        self.assertEqual(token_file.read_text(), 'from-ci')

    def test_if_changed_skips_only_when_the_lock_and_values_are_deployed(self):
        self.invoke('deploy', 'shop', '--env', 'prod')

        same, build_image = self.invoke('deploy', 'shop', '--env', 'prod', '--if-changed')

        self.assertEqual(same.exit_code, 0, same.output)
        self.assertIn('already runs this lock with these values', same.output)
        build_image.assert_not_called()

        prod = self.project / 'services' / 'shop' / 'environments' / 'prod.yaml'
        prod.write_text(prod.read_text().replace('REGION: eu', 'REGION: us'))
        changed, build_image = self.invoke('deploy', 'shop', '--env', 'prod', '--if-changed')
        self.assertEqual(changed.exit_code, 0, changed.output)
        build_image.assert_called_once()
        self.assertEqual(self.docker.containers['mage-shop-prod']['env']['REGION'], 'us')

    def test_uncommitted_changes_need_a_reason_which_the_deployment_records(self):
        self.uncommitted = ['services/shop/service.lock', 'data_loaders/load.py']

        refused, build_image = self.invoke('deploy', 'shop', '--env', 'prod')

        self.assertEqual(refused.exit_code, 1)
        build_image.assert_not_called()
        self.assertIn('services/shop/service.lock (the build updated it)', refused.output)
        self.assertIn('data_loaders/load.py', refused.output)
        self.assertNotIn('mage-shop-prod', self.docker.containers)

        allowed, _ = self.invoke('deploy', 'shop', '--env', 'prod',
                                 '--allow-uncommitted', 'hotfix for the outage')
        self.assertEqual(allowed.exit_code, 0, allowed.output)
        labels = self.docker.containers['mage-shop-prod']['labels']
        self.assertEqual(labels['ai.mageml.reason'], 'hotfix for the outage')
        status, _ = self.invoke('status', 'shop')
        self.assertIn('uncommitted: hotfix for the outage', status.output)

    def test_outside_git_a_deploy_says_it_records_no_commit(self):
        self.uncommitted = None

        result, _ = self.invoke('deploy', 'shop', '--env', 'prod')

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn('records no commit', result.output)

    def test_rollback_and_other_targets(self):
        self.invoke('deploy', 'shop', '--env', 'prod')
        missing, _ = self.invoke('rollback', 'shop', '--env', 'prod')
        self.assertEqual(missing.exit_code, 1)
        self.assertIn('no previous deployment', missing.output)

        environments = self.project / 'services' / 'shop' / 'environments'
        (environments / 'ce.yaml').write_text('target: code_engine\n')
        other, _ = self.invoke('deploy', 'shop', '--env', 'ce')
        self.assertEqual(other.exit_code, 1)
        self.assertIn('supports docker and kubernetes so far', other.output)


class ServicesListDeploymentsTest(TestCase):
    def test_the_services_list_shows_each_deployment_and_its_drift(self):
        """The Services page could not show what runs where."""
        from unittest.mock import patch

        from mage_ai.api.resources import PipelineServiceResource as resource
        from mage_ai.pipeline_services import lifecycle

        docker = FakeDocker()
        target = deploy.DockerTarget(runner=docker, health=lambda port, timeout: True)
        target.deploy('shop', 'prod', 'mage-shop:1', 7001, {}, dict(commit='c1', lock='L1'))
        with patch.object(deploy, 'DockerTarget', return_value=target), \
                patch('shutil.which', return_value='/usr/bin/docker'), \
                patch.object(lifecycle, 'read_lock', return_value=dict(manifest_sha256='L2')):
            found = resource.deployments_by_service(self.repo_path)

        self.assertEqual(found['shop'][0]['environment'], 'prod')
        self.assertEqual(found['shop'][0]['drift'], 'behind the repository')
        with patch('shutil.which', return_value=None):
            self.assertEqual(resource.deployments_by_service(self.repo_path), {})
