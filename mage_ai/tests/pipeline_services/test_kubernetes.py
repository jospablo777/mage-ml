"""
The Kubernetes deploy adapter against a fake kubectl: the objects applied, secrets kept off
the command line and named by content, the rollout wait and its undo, status from the pod
template, and the settings that must be explicit.
"""
import copy
import json
import subprocess
from typing import Dict, List

from mage_ai.pipeline_services import deploy, kubernetes
from mage_ai.tests.base_test import TestCase

SETTINGS = dict(target='kubernetes', context='kind-test', namespace='shop',
                image='registry.example.com/team/shop')


class FakeKubectl:
    """Deployments with their revisions, secrets and claims, as a cluster keeps them."""

    def __init__(self):
        self.commands: List[List[str]] = []
        self.deployments: Dict[str, Dict] = {}
        self.revisions: Dict[str, List[Dict]] = {}
        self.secrets: Dict[str, Dict] = {}
        self.ready = True
        self.clock = 0

    def __call__(self, arguments, input=None):
        self.commands.append(arguments)
        assert arguments[:6] == ['--context', 'kind-test', '--namespace', 'shop',
                                 '--request-timeout', arguments[5]], arguments
        args = arguments[6:]

        def done(stdout='', code=0, stderr=''):
            return subprocess.CompletedProcess(arguments, code, stdout, stderr)

        if args[:2] == ['get', 'deployment']:
            item = self.deployments.get(args[2])
            if item is None:
                return done(code=1, stderr=f'Error from server (NotFound): "{args[2]}" not found')
            return done(json.dumps(item))
        if args[:2] == ['get', 'secrets']:
            return done(json.dumps(dict(items=list(self.secrets.values()))))
        if args[:2] == ['delete', 'secret']:
            self.secrets.pop(args[2], None)
            return done()
        if args[0] == 'apply':
            for item in json.loads(input)['items']:
                if item['kind'] == 'Secret':
                    self.clock += 1
                    item['metadata']['creationTimestamp'] = f'2026-10-17T00:00:{self.clock:02d}Z'
                    self.secrets[item['metadata']['name']] = item
                if item['kind'] == 'Deployment':
                    self._roll(item['metadata']['name'], item['spec']['template'])
            return done('applied')
        if args[:2] == ['rollout', 'status']:
            return done('successfully rolled out') if self.ready else \
                done(code=1, stderr='error: timed out waiting for the condition')
        if args[:2] == ['rollout', 'undo']:
            name = args[2].split('/')[1]
            history = self.revisions.get(name, [])
            if len(history) < 2:
                return done(code=1, stderr='error: no rollout history found')
            self._roll(name, history[-2])
            return done('rolled back')
        if args[0] == 'logs':
            return done('Error: PGHOST is not set')
        raise AssertionError(args)

    def _roll(self, name, template):
        self.revisions.setdefault(name, []).append(copy.deepcopy(template))
        ready = 1 if self.ready else 0
        self.deployments[name] = dict(
            metadata=dict(name=name), spec=dict(replicas=1, template=copy.deepcopy(template)),
            status=dict(updatedReplicas=1, readyReplicas=ready, replicas=1),
        )


class KubernetesTargetTest(TestCase):
    def setUp(self):
        super().setUp()
        self.kubectl = FakeKubectl()
        self.target = kubernetes.KubernetesTarget(SETTINGS, runner=self.kubectl)

    def deploy(self, image='registry.example.com/team/shop:aaa', env=None, **annotations):
        return self.target.deploy('shop', 'prod', image, env or dict(PGPASSWORD='s3cret'),
                                  {'commit': 'c0ffee', 'lock': 'l1', **annotations}, timeout=60)

    def test_a_deploy_applies_the_objects_with_secrets_on_stdin_only(self):
        deployment = self.deploy()

        self.assertEqual((deployment.state, deployment.commit, deployment.lock),
                         ('running', 'c0ffee', 'l1'))
        self.assertEqual(deployment.image, 'registry.example.com/team/shop:aaa')
        self.assertFalse(any('s3cret' in ' '.join(c) for c in self.kubectl.commands))
        [secret] = self.kubectl.secrets.values()
        self.assertEqual(secret['stringData'], dict(PGPASSWORD='s3cret'))
        self.assertTrue(secret['metadata']['name'].startswith('mage-shop-prod-env-'))
        pod = self.kubectl.deployments['mage-shop-prod']['spec']['template']['spec']
        container = pod['containers'][0]
        self.assertEqual(container['envFrom'], [dict(secretRef=dict(
            name=secret['metadata']['name']))])
        self.assertIn(dict(name='MAGE_SERVICE_INSTANCE_ID', valueFrom=dict(
            fieldRef=dict(fieldPath='metadata.name'))), container['env'])
        self.assertEqual(pod['volumes'][0]['persistentVolumeClaim']['claimName'],
                         'mage-shop-prod-data')

    def test_objects_follow_the_settings(self):
        target = kubernetes.KubernetesTarget(dict(
            SETTINGS, storage='20Gi', storage_class='ibmc-block-gold',
            resources=dict(limits=dict(memory='1Gi')), image_pull_secret='icr',
        ), runner=self.kubectl)

        objects = {o['kind']: o for o in target.manifests('shop', 'prod', 'i:1', {}, {})}

        claim = objects['PersistentVolumeClaim']['spec']
        self.assertEqual(claim['resources']['requests']['storage'], '20Gi')
        self.assertEqual(claim['storageClassName'], 'ibmc-block-gold')
        pod = objects['Deployment']['spec']['template']['spec']
        self.assertEqual(pod['containers'][0]['resources'], dict(limits=dict(memory='1Gi')))
        self.assertEqual(pod['imagePullSecrets'], [dict(name='icr')])
        self.assertEqual(objects['Deployment']['spec']['strategy'], dict(type='Recreate'))
        self.assertEqual(objects['Service']['spec']['ports'][0]['targetPort'], 'http')

    def test_a_failed_rollout_is_undone_with_its_previous_secret(self):
        self.deploy(env=dict(PGHOST='db'))
        first_secret = self.kubectl.deployments['mage-shop-prod']['spec']['template']['spec'][
            'containers'][0]['envFrom'][0]['secretRef']['name']
        self.kubectl.ready = False

        with self.assertRaisesRegex(deploy.DeployError, 'previous revision runs again') as raised:
            self.deploy(image='registry.example.com/team/shop:bbb', env=dict(PGHOST=''))

        self.assertIn('PGHOST is not set', str(raised.exception))
        template = self.kubectl.deployments['mage-shop-prod']['spec']['template']
        self.assertEqual(template['metadata']['annotations']['ai.mageml/image'],
                         'registry.example.com/team/shop:aaa')
        self.assertEqual(template['spec']['containers'][0]['envFrom'][0]['secretRef']['name'],
                         first_secret)
        self.assertIn(first_secret, self.kubectl.secrets)

    def test_a_first_deploy_that_fails_is_left_to_inspect(self):
        self.kubectl.ready = False

        with self.assertRaisesRegex(deploy.DeployError, 'it is left as it is'):
            self.deploy()

        self.assertNotIn(['rollout', 'undo'], [c[6:8] for c in self.kubectl.commands])

    def test_rollback_restores_the_previous_revision_and_its_annotations(self):
        with self.assertRaisesRegex(deploy.DeployError, 'not deployed in prod'):
            self.target.rollback('shop', 'prod')
        self.deploy()
        with self.assertRaisesRegex(deploy.DeployError, 'no previous deployment'):
            self.target.rollback('shop', 'prod')
        self.deploy(image='registry.example.com/team/shop:bbb', commit='d00d')

        deployment = self.target.rollback('shop', 'prod')

        self.assertEqual((deployment.image, deployment.commit),
                         ('registry.example.com/team/shop:aaa', 'c0ffee'))

    def test_old_secrets_are_pruned_beyond_the_kept_revisions(self):
        for number in range(kubernetes.REVISIONS + 4):
            self.deploy(env=dict(N=str(number)))

        self.assertEqual(len(self.kubectl.secrets), kubernetes.REVISIONS + 1)
        current = kubernetes.secret_name('mage-shop-prod', dict(N=str(kubernetes.REVISIONS + 3)))
        self.assertIn(current, self.kubectl.secrets)
        self.assertNotIn(kubernetes.secret_name('mage-shop-prod', dict(N='0')),
                         self.kubectl.secrets)

    def test_the_engine_log_comes_from_the_deployment(self):
        with self.assertRaisesRegex(deploy.DeployError, 'not deployed in prod'):
            self.target.log_command('shop', 'prod')
        self.deploy()

        self.assertEqual(
            self.target.log_command('shop', 'prod', tail=5, follow=True),
            ['kubectl', '--context', 'kind-test', '--namespace', 'shop', 'logs',
             'deployment/mage-shop-prod', '--tail', '5', '--follow'],
        )

    def test_settings_that_must_be_explicit(self):
        with self.assertRaisesRegex(deploy.DeployError, 'Name the kubectl context'):
            kubernetes.KubernetesTarget(dict(target='kubernetes'), environment='prod')
        with self.assertRaisesRegex(deploy.DeployError, 'one replica'):
            kubernetes.KubernetesTarget(dict(SETTINGS, replicas=3))
        with self.assertRaisesRegex(deploy.DeployError, 'not a Kubernetes name'):
            self.target.manifests('shop', 'Prod_EU', 'i:1', {}, {})

    def test_images_go_to_the_registry_or_stay_local_with_a_note(self):
        calls = []

        def build_image(box, tag, platforms=None, push=False):
            calls.append((tag, platforms, push))

        target = kubernetes.KubernetesTarget(dict(SETTINGS, platforms=['linux/amd64']),
                                             runner=self.kubectl)
        image = target.image('shop', 'abc')
        self.assertEqual(target.publish('box', image, build_image), [])
        self.assertEqual(calls[-1], ('registry.example.com/team/shop:abc', ['linux/amd64'], True))

        local = kubernetes.KubernetesTarget(dict(SETTINGS, image=None), runner=self.kubectl)
        notes = local.publish('box', local.image('shop', 'abc'), build_image)
        self.assertEqual(calls[-1], ('mage-shop:abc', None, False))
        self.assertIn('stays in this Docker', notes[0])

    def test_status_reads_every_kubernetes_environment(self):
        import shutil
        from pathlib import Path

        self.deploy()
        folder = Path(self.repo_path) / 'services' / 'shop' / 'environments'
        folder.mkdir(parents=True, exist_ok=True)
        self.addCleanup(shutil.rmtree, Path(self.repo_path) / 'services', True)
        (Path(self.repo_path) / 'services' / 'shop' / 'service.yaml').write_text(
            'pipelines: [orders]\n')
        (folder / 'prod.yaml').write_text(json.dumps(SETTINGS))
        (folder / 'stage.yaml').write_text(json.dumps(dict(SETTINGS, context='')))

        found, problems = deploy.deployments(
            self.repo_path, docker=_NoDocker(), kubernetes_runner=self.kubectl,
        )

        self.assertEqual([(d.service, d.environment, d.state) for d in found],
                         [('shop', 'prod', 'running')])
        self.assertEqual(len(problems), 1)
        self.assertIn('shop stage: Name the kubectl context', problems[0])


class _NoDocker:
    def status(self, service=None):
        return []


class KubernetesCommandTest(TestCase):
    """mage service deploy, status, runs and logs --engine with a Kubernetes environment."""

    def setUp(self):
        super().setUp()
        import shutil
        from pathlib import Path

        self.project = Path(self.repo_path)
        folder = self.project / 'services' / 'shop' / 'environments'
        folder.mkdir(parents=True, exist_ok=True)
        (folder.parent / 'service.yaml').write_text('pipelines: [orders]\n')
        (folder / 'prod.yaml').write_text(
            'target: kubernetes\ncontext: kind-test\nnamespace: shop\n'
            'image: registry.example.com/team/shop\nschedules: on\nvariables:\n  REGION: eu\n')
        (folder.parent / '.env.prod').write_text('API_KEY=k3y\n')
        self.addCleanup(shutil.rmtree, self.project / 'services', True)
        self.addCleanup(shutil.rmtree, self.project / '.mage', True)
        self.kubectl = FakeKubectl()

    def invoke(self, *arguments):
        from unittest.mock import MagicMock, patch

        from typer.testing import CliRunner

        from mage_ai.cli.main import app
        from mage_ai.pipeline_services import lifecycle

        real = kubernetes.KubernetesTarget

        def target(settings, **options):
            return real(settings, **dict(options, runner=self.kubectl))

        check = MagicMock(ok=True, problems=[])
        with patch.object(kubernetes, 'KubernetesTarget', side_effect=target), \
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

    def test_deploy_pushes_applies_and_status_reads_the_cluster(self):
        result, build_image = self.invoke('deploy', 'shop', '--env', 'prod')

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(build_image.call_args.args[1],
                         'registry.example.com/team/shop:aaaaaaaaaaaa')
        self.assertTrue(build_image.call_args.kwargs['push'])
        self.assertIn('deployment/mage-shop-prod in kind-test, namespace shop', result.output)
        [secret] = self.kubectl.secrets.values()
        self.assertEqual(secret['stringData']['REGION'], 'eu')
        self.assertEqual(secret['stringData']['API_KEY'], 'k3y')
        self.assertEqual(secret['stringData']['MAGE_SERVICE_SCHEDULES'], 'on')
        self.assertTrue(secret['stringData']['MAGE_SERVICE_TOKEN'])

        status, _ = self.invoke('status', 'shop')
        self.assertIn('shop  prod  running the repository version', status.output)
