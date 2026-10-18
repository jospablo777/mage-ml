"""
The Code Engine deploy adapter against a fake Code Engine API v2: the app and its Secret,
the wait for a new ready revision, a failed deploy patched back, rollback from what the app
records, and the settings it needs.
"""
import json
import re
from typing import Dict, List

from mage_ai.pipeline_services import code_engine, deploy
from mage_ai.tests.base_test import TestCase

SETTINGS = dict(target='code_engine', region='eu-de', project_id='p-1',
                image='icr.io/team/shop', cpu='1', memory='2G')
BASE = 'https://api.eu-de.codeengine.cloud.ibm.com/v2/projects/p-1'


class FakeCodeEngine:
    """Apps with revisions and generic secrets, as Code Engine keeps them."""

    def __init__(self):
        self.calls: List[tuple] = []
        self.apps: Dict[str, Dict] = {}
        self.secrets: Dict[str, Dict] = {}
        self.clock = 0
        # A revision whose Secret lacks REGION never becomes ready.
        self.healthy = lambda app: any(
            'REGION' in self.secrets.get(v.get('reference'), {}).get('data', {})
            for v in app['run_env_variables'] if v['type'] == 'secret_full_reference'
        )

    def __call__(self, method, url, body, headers):
        assert url.startswith(BASE), url
        assert headers['Authorization'] == 'Bearer t0ken'
        path = url[len(BASE):]
        self.calls.append((method, path, body, headers))
        if method == 'POST' and path == '/secrets':
            if body['name'] in self.secrets:
                return 409, dict(message='already exists')
            self.clock += 1
            created = f'2026-10-17T00:00:{self.clock:02d}Z'
            self.secrets[body['name']] = dict(body, created_at=created)
            return 201, self.secrets[body['name']]
        if method == 'GET' and path.startswith('/secrets?'):
            return 200, dict(secrets=list(self.secrets.values()))
        found = re.match(r'^/secrets/(.+)$', path)
        if found and method == 'GET':
            item = self.secrets.get(found.group(1))
            return (200, item) if item else (404, dict(message='not found'))
        if found and method == 'DELETE':
            self.secrets.pop(found.group(1), None)
            return 202, {}
        if method == 'POST' and path == '/apps':
            app = dict(body, entity_tag='1', status='deploying', status_details={})
            self.apps[body['name']] = app
            self._revise(app)
            return 201, app
        found = re.match(r'^/apps/(.+)$', path)
        if found:
            app = self.apps.get(found.group(1))
            if app is None:
                return 404, dict(message='not found')
            if method == 'GET':
                self._settle(app)
                return 200, json.loads(json.dumps(app))
            if method == 'PATCH':
                assert headers['Content-Type'] == 'application/merge-patch+json'
                assert headers['If-Match'] == app['entity_tag'], 'stale entity tag'
                changed = any(app.get(k) != v for k, v in body.items()
                              if k in ('image_reference', 'run_env_variables'))
                app.update(body)
                app['entity_tag'] = str(int(app['entity_tag']) + 1)
                if changed:
                    self._revise(app)
                return 200, app
        raise AssertionError((method, path))

    def _revise(self, app):
        details = app['status_details']
        number = int((details.get('latest_created_revision') or '-0').rsplit('-', 1)[1]) + 1
        details['latest_created_revision'] = f'{app["name"]}-{number:05d}'
        app['status'] = 'deploying'
        app['_pending'] = True

    def _settle(self, app):
        if not app.pop('_pending', False):
            return
        details = app['status_details']
        if self.healthy(app):
            details['latest_ready_revision'] = details['latest_created_revision']
            app['status'] = 'ready'
            app['endpoint'] = f'https://{app["name"]}.example.codeengine.appdomain.cloud'
            details.pop('reason', None)
        else:
            app['status'] = 'failed'
            details['reason'] = 'ContainerHealthCheckFailed'


class CodeEngineTargetTest(TestCase):
    def setUp(self):
        super().setUp()
        self.api = FakeCodeEngine()
        self.target = self.make(SETTINGS)

    def make(self, settings):
        return code_engine.CodeEngineTarget(settings, request=self.api,
                                            token=lambda: 't0ken', sleep=lambda s: None)

    def deploy(self, image='icr.io/team/shop:aaa', env=None, **annotations):
        return self.target.deploy(
            'shop', 'prod', image, env or dict(REGION='eu', PGPASSWORD='s3cret'),
            {'commit': 'c0ffee', 'lock': 'l1', 'values': 'v1', **annotations}, timeout=60)

    def app(self):
        return self.api.apps['mage-shop-prod']

    def secret_of(self):
        return next(v['reference'] for v in self.app()['run_env_variables']
                    if v['type'] == 'secret_full_reference')

    def test_a_first_deploy_creates_the_secret_and_an_always_on_app(self):
        deployment = self.deploy()

        self.assertEqual((deployment.state, deployment.image, deployment.commit),
                         ('running', 'icr.io/team/shop:aaa', 'c0ffee'))
        app = self.app()
        self.assertEqual((app['scale_min_instances'], app['scale_max_instances']), (1, 1))
        self.assertEqual(app['image_port'], 8080)
        self.assertEqual((app['scale_cpu_limit'], app['scale_memory_limit']), ('1', '2G'))
        self.assertEqual(app['managed_domain_mappings'], 'local_public')
        self.assertEqual(self.api.secrets[self.secret_of()]['data']['PGPASSWORD'], 's3cret')
        self.assertTrue(all('s3cret' not in path for _, path, _, _ in self.api.calls))
        self.assertEqual(self.target.api_url('shop', 'prod'),
                         'https://mage-shop-prod.example.codeengine.appdomain.cloud')

    def test_an_unchanged_redeploy_patches_nothing(self):
        self.deploy()
        calls = len(self.api.calls)

        self.deploy()

        self.assertFalse(any(m == 'PATCH' for m, *_ in self.api.calls[calls:]))

    def test_a_deploy_that_fails_is_patched_back_with_its_secret(self):
        self.deploy()
        first = self.secret_of()

        with self.assertRaisesRegex(deploy.DeployError, 'patched back') as raised:
            self.deploy(image='icr.io/team/shop:bbb', env=dict(PGPASSWORD='s3cret'),
                        commit='d00d')

        self.assertIn('ContainerHealthCheckFailed', str(raised.exception))
        self.assertEqual(self.app()['image_reference'], 'icr.io/team/shop:aaa')
        self.assertEqual(self.secret_of(), first)
        self.assertEqual(self.target.inspect('shop', 'prod').commit, 'c0ffee')

    def test_rollback_swaps_with_the_recorded_previous_deployment(self):
        self.deploy()
        with self.assertRaisesRegex(deploy.DeployError, 'no previous deployment'):
            self.target.rollback('shop', 'prod')
        first = self.secret_of()
        self.deploy(image='icr.io/team/shop:bbb', env=dict(REGION='us'), commit='d00d')
        second = self.secret_of()

        back = self.target.rollback('shop', 'prod')

        self.assertEqual((back.image, back.commit), ('icr.io/team/shop:aaa', 'c0ffee'))
        self.assertEqual(self.secret_of(), first)
        again = self.target.rollback('shop', 'prod')
        self.assertEqual((again.image, again.commit), ('icr.io/team/shop:bbb', 'd00d'))
        self.assertEqual(self.secret_of(), second)

    def test_the_token_and_old_secrets(self):
        self.deploy(env=dict(REGION='eu', MAGE_SERVICE_TOKEN='tok'))
        self.assertEqual(self.target.current_token('shop', 'prod'), 'tok')
        for number in range(8):
            self.deploy(env=dict(REGION=str(number)), commit=str(number))

        # The running one, the previous one and three spare.
        self.assertEqual(len(self.api.secrets), 2 + code_engine.SPARE_SECRETS)
        self.assertIn(self.secret_of(), self.api.secrets)

    def test_settings_it_needs(self):
        with self.assertRaisesRegex(deploy.DeployError, 'Set project_id, image'):
            code_engine.CodeEngineTarget(dict(target='code_engine', region='eu-de'),
                                         environment='prod')
        with self.assertRaisesRegex(deploy.DeployError, 'visibility is one of'):
            self.make(dict(SETTINGS, visibility='internal'))
        self.assertEqual(self.make(dict(SETTINGS, visibility='project')).visibility, 'local')

    def test_images_are_pushed_for_amd64(self):
        calls = []
        self.target.publish('box', self.target.image('shop', 'abc'),
                            lambda box, tag, platforms=None, push=False:
                            calls.append((tag, platforms, push)))
        self.assertEqual(calls, [('icr.io/team/shop:abc', ['linux/amd64'], True)])

    def test_the_engine_log_uses_a_cli_home_of_its_own(self):
        command = self.target.log_command('shop', 'prod', tail=20, follow=True)

        self.assertEqual(command[:2], ['sh', '-c'])
        self.assertIn('IBMCLOUD_HOME="$(mktemp -d)"', command[2])
        self.assertIn('ibmcloud ce project select --id p-1', command[2])
        self.assertIn('--application mage-shop-prod --tail 20 --follow', command[2])
        self.assertNotIn('apikey', command[2])

    def test_a_settings_change_alone_makes_a_revision(self):
        self.deploy()
        revision = self.app()['status_details']['latest_ready_revision']
        self.target = self.make(dict(SETTINGS, memory='4G'))

        deployment = self.deploy()

        self.assertEqual(deployment.state, 'running')
        self.assertEqual(self.app()['scale_memory_limit'], '4G')
        self.assertNotEqual(self.app()['status_details']['latest_ready_revision'], revision)


class CodeEngineCommandTest(TestCase):
    """mage service deploy, status and runs with a Code Engine environment."""

    def setUp(self):
        super().setUp()
        import shutil
        from pathlib import Path

        self.project = Path(self.repo_path)
        folder = self.project / 'services' / 'shop' / 'environments'
        folder.mkdir(parents=True, exist_ok=True)
        (folder.parent / 'service.yaml').write_text('pipelines: [orders]\n')
        (folder / 'prod.yaml').write_text(
            'target: code_engine\nregion: eu-de\nproject_id: p-1\nimage: icr.io/team/shop\n'
            'variables:\n  REGION: eu\n')
        self.addCleanup(shutil.rmtree, self.project / 'services', True)
        self.addCleanup(shutil.rmtree, self.project / '.mage', True)
        self.api = FakeCodeEngine()

    def invoke(self, *arguments):
        from unittest.mock import MagicMock, patch

        from typer.testing import CliRunner

        from mage_ai.cli.main import app
        from mage_ai.pipeline_services import lifecycle

        check = MagicMock(ok=True, problems=[])
        with patch.object(code_engine, '_http', self.api), \
                patch.object(code_engine, '_token', lambda: 't0ken'), \
                patch.object(code_engine.time, 'sleep', lambda s: None), \
                patch.object(lifecycle, 'check', return_value=check), \
                patch.object(lifecycle, 'build', return_value=self.project / '.mage' / 'b'), \
                patch.object(lifecycle, 'read_lock',
                             return_value=dict(manifest_sha256='a' * 64)), \
                patch.object(lifecycle, 'uncommitted_inputs', return_value=[]), \
                patch('mage_ai.pipeline_services.export.build_image') as build_image, \
                patch.object(deploy, 'DockerTarget', return_value=_NoDocker()), \
                patch.object(deploy, 'git_commit', return_value='c0ffee'):
            result = CliRunner().invoke(app, ['service', *arguments, '--project',
                                              self.repo_path])
        return result, build_image

    def test_deploy_from_ci_keeps_the_token_and_status_reads_the_app(self):
        result, build_image = self.invoke('deploy', 'shop', '--env', 'prod')

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(build_image.call_args.args[1], 'icr.io/team/shop:aaaaaaaaaaaa')
        self.assertIn('https://mage-shop-prod.example.codeengine.appdomain.cloud',
                      result.output)
        token_file = deploy.token_file(self.repo_path, 'shop', 'prod')
        token = token_file.read_text()
        token_file.unlink()

        again, build_image = self.invoke('deploy', 'shop', '--env', 'prod', '--if-changed')

        self.assertIn('already runs this lock with these values', again.output)
        build_image.assert_not_called()
        self.assertEqual(token_file.read_text(), token)
        status, _ = self.invoke('status', 'shop')
        self.assertIn('shop  prod  running the repository version', status.output)


class _NoDocker:
    def status(self, service=None):
        return []
