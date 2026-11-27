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


class DeploySavepointTest(TestCase):
    """A deploy takes a savepoint of the running deployment's streams before it stops it."""

    def setUp(self):
        super().setUp()
        self.docker = FakeDocker()
        self.calls = []
        self.fail = False

        def savepoints(url, token, name):
            # Taken while the deployment before still runs.
            running = self.docker.containers['mage-shop-prod']['running']
            self.calls.append((url, token, name, running))
            if self.fail:
                raise deploy.DeployError('savepoint deploy-1 of stream orders failed (500)')
            return ['orders'], ['clicks: no savepoint, it keeps no Kafka offsets']

        self.target = deploy.DockerTarget(runner=self.docker, health=lambda *a: True,
                                          savepoints=savepoints)

    def deploy(self, image, savepoint='deploy-1'):
        return self.target.deploy(
            'shop', 'prod', image, 7001, env=dict(MAGE_SERVICE_TOKEN='tok'),
            labels=dict(commit='abc', lock=image), timeout=1, savepoint=savepoint,
        )

    def test_the_savepoint_is_taken_of_the_running_deployment_first(self):
        first = self.deploy('mage-shop:1')
        self.assertEqual((first.savepoints, self.calls), ([], []), 'nothing ran before')

        second = self.deploy('mage-shop:2')

        self.assertEqual(self.calls, [('http://127.0.0.1:7001', 'tok', 'deploy-1', True)])
        self.assertEqual(second.savepoints, ['orders'])
        self.assertEqual(second.savepoint_notes,
                         ['clicks: no savepoint, it keeps no Kafka offsets'])

    def test_a_failed_savepoint_stops_the_deploy_before_anything_changes(self):
        self.deploy('mage-shop:1')
        self.fail = True

        with self.assertRaisesRegex(deploy.DeployError, 'failed'):
            self.deploy('mage-shop:2')

        current = self.docker.containers['mage-shop-prod']
        self.assertEqual((current['image'], current['running']), ('mage-shop:1', True))
        self.assertNotIn('mage-shop-prod-previous', self.docker.containers)

    def test_without_a_savepoint_none_is_taken(self):
        self.deploy('mage-shop:1')
        self.deploy('mage-shop:2', savepoint=None)
        self.assertEqual(self.calls, [])


class TakeSavepointsTest(TestCase):
    """take_savepoints against a service's API."""

    def serve(self, answers):
        import http.server
        import threading

        requests = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def answer(self):
                requests.append((self.command, self.path, self.headers.get('Authorization')))
                code, body = answers[(self.command, self.path)]
                self.send_response(code)
                self.end_headers()
                if body is not None:
                    self.wfile.write(json.dumps(body).encode())

            do_GET = answer

            def do_POST(self):
                self.rfile.read(int(self.headers.get('Content-Length') or 0))
                self.answer()

            def log_message(self, *args):
                pass

        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        return server.server_address[1], requests

    def test_kafka_streams_get_one_and_others_are_noted(self):
        port, requests = self.serve({
            ('GET', '/v1/streams'): (200, dict(streams=[
                dict(pipeline='orders'), dict(pipeline='hooks'), dict(pipeline='old'),
            ])),
            ('POST', '/v1/streams/orders/savepoints'): (200, dict(files=2)),
            ('POST', '/v1/streams/hooks/savepoints'): (
                400, dict(error='Savepoints keep Kafka offsets; stream hooks reads http.')),
            ('POST', '/v1/streams/old/savepoints'): (404, None),
        })

        taken, notes = deploy.take_savepoints(f'http://127.0.0.1:{port}', 'tok', 'deploy-1')

        self.assertEqual(taken, ['orders'])
        self.assertIn('hooks: no savepoint, Savepoints keep Kafka offsets', notes[0])
        self.assertIn('predates them', notes[1])
        self.assertEqual({r[2] for r in requests}, {'Bearer tok'})

    def test_a_savepoint_that_fails_stops_with_the_way_on(self):
        port, _ = self.serve({
            ('GET', '/v1/streams'): (200, dict(streams=[dict(pipeline='orders')])),
            ('POST', '/v1/streams/orders/savepoints'): (
                503, dict(error='Stream orders did not settle within 120 seconds')),
        })
        with self.assertRaisesRegex(deploy.DeployError, 'did not settle.*--no-savepoint'):
            deploy.take_savepoints(f'http://127.0.0.1:{port}', 'tok', 'deploy-1')

    def test_a_deployment_that_does_not_answer_does_not_hold_up_the_deploy(self):
        import socket

        with socket.socket() as unused:
            unused.bind(('127.0.0.1', 0))
            port = unused.getsockname()[1]
        taken, notes = deploy.take_savepoints(f'http://127.0.0.1:{port}', 'tok', 'deploy-1',
                                              timeout=2)
        self.assertEqual(taken, [])
        self.assertIn('did not answer', notes[0])


class DeployCommandTest(TestCase):
    """mage service deploy, status, diff and rollback with a fake Docker."""

    def setUp(self):
        super().setUp()
        # The service's tests pass unless a test says otherwise.
        self.tests = []
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
                patch.object(lifecycle, 'test', return_value=self.tests), \
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

    def test_failing_service_tests_stop_the_deploy_unless_skipped_with_a_reason(self):
        from mage_ai.pipeline_services import lifecycle

        self.tests = [lifecycle.SampleTest(
            pipeline='orders', sample='example', block='orders_parse', messages=3,
            error='orders_parse: KeyError: amount',
        )]

        refused, build_image = self.invoke('deploy', 'shop', '--env', 'prod')

        self.assertEqual(refused.exit_code, 1)
        build_image.assert_not_called()
        self.assertIn('orders_parse: KeyError: amount', refused.output)
        self.assertIn('--skip-tests', refused.output)

        skipped, _ = self.invoke('deploy', 'shop', '--env', 'prod',
                                 '--skip-tests', 'the sample is out of date')
        self.assertEqual(skipped.exit_code, 0, skipped.output)
        labels = self.docker.containers['mage-shop-prod']['labels']
        self.assertEqual(labels['ai.mageml.tests_skipped'], 'the sample is out of date')

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
        unset, _ = self.invoke('deploy', 'shop', '--env', 'ce')
        self.assertEqual(unset.exit_code, 1)
        self.assertIn('Set region, project_id, image in environments/ce.yaml', unset.output)
        (environments / 'nomad.yaml').write_text('target: nomad\n')
        other, _ = self.invoke('deploy', 'shop', '--env', 'nomad')
        self.assertEqual(other.exit_code, 1)
        self.assertIn('supports docker, kubernetes and code_engine', other.output)


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
