"""
Managing Python workers for the Workers page (data_preparation/python_workers.py): where each
is used, changing its packages only when uv locks the change, updates and fixes previewed
and applied as shown, and removing a worker no block uses. Needs uv; packages come from its
cache.
"""
import json
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

from mage_ai.data_preparation import environments, python_workers
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.shared import vulnerabilities
from mage_ai.tests.base_test import DBTestCase

PYPROJECT = '''\
[project]
name = "mageml-worker-{name}"
version = "0"
requires-python = ">=3.12"
# Kept when the Workers page edits the file.
dependencies = {dependencies}

[tool.mageml.worker]
label = "{label}"
'''


@unittest.skipIf(shutil.which('uv') is None, 'uv is not installed')
class PythonWorkersTest(DBTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.cache = tempfile.mkdtemp()
        cls.patcher = patch.dict('os.environ', {environments.ENVIRONMENTS_DIR_ENV: cls.cache})
        cls.patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls.patcher.stop()
        shutil.rmtree(cls.cache, ignore_errors=True)
        super().tearDownClass()

    def worker(self, name='tools', dependencies=('six==1.16.0',), label='Tools worker',
               upgrade_package=None):
        folder = Path(self.repo_path, environments.WORKERS_DIR, name)
        folder.mkdir(parents=True, exist_ok=True)
        self.addCleanup(shutil.rmtree, folder, True)
        (folder / 'pyproject.toml').write_text(PYPROJECT.format(
            name=name, dependencies=json.dumps(list(dependencies)), label=label))
        command = ['uv', 'lock', '--project', str(folder)]
        if upgrade_package:
            command += ['--upgrade-package', upgrade_package]
        subprocess.run(command, check=True, capture_output=True)
        return folder

    def pipeline(self, block_worker=None, default=None):
        pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        if default:
            pipeline.environment = dict(worker=default)
            pipeline.save()
        block = Block.create(
            f'{pipeline.uuid}_load', 'data_loader', self.repo_path, pipeline=pipeline,
            configuration=dict(python_worker=block_worker) if block_worker else None,
        )
        Path(block.file_path).write_text(textwrap.dedent('''
            @data_loader
            def load(*args, **kwargs):
                return {}
        '''))
        return pipeline, block.uuid

    def test_the_overview_says_where_each_worker_runs_blocks_and_what_it_installs(self):
        self.worker()
        self.worker('spare', label='Spare worker')
        own, load = self.pipeline(block_worker='tools')
        default, default_load = self.pipeline(default='Tools')
        workers = {w['name']: w for w in python_workers.overview(self.repo_path)}
        tools = workers['tools']
        self.assertEqual((tools['label'], tools['dependencies']), ('Tools worker', ['six==1.16.0']))
        self.assertGreater(tools['size_bytes'], 0)
        self.assertEqual(tools['security'], dict(state='unchecked'))
        self.assertEqual(sorted((u['pipeline'], u['blocks'], u['default'])
                                for u in tools['used_by']),
                         sorted([(own.uuid, [load], False), (default.uuid, [default_load], True)]))
        self.assertEqual(workers['spare']['used_by'], [])

        [six] = python_workers.detail(self.repo_path, 'tools')['packages']
        self.assertEqual((six['name'], six['version'], six['declared']), ('six', '1.16.0', True))

    def test_packages_change_only_when_uv_locks_them(self):
        folder = self.worker()
        before = {n: (folder / n).read_text() for n in ('pyproject.toml', 'uv.lock')}
        for dependencies, message in (
            (['six==1.16.0', 'pandas>=='], 'is not a package requirement'),
            (['six==99.0'], 'uv could not lock the change, so nothing changed'),
        ):
            with self.assertRaisesRegex(environments.PipelineEnvironmentError, message):
                python_workers.update(self.repo_path, 'tools', dependencies=dependencies)
            self.assertEqual({n: (folder / n).read_text() for n in before}, before)

        result = python_workers.update(self.repo_path, 'tools',
                                       dependencies=['six==1.17.0', 'packaging==24.1'])
        self.assertEqual(result['changes'], [
            dict(package='packaging', **{'from': None, 'to': '24.1'}),
            dict(package='six', **{'from': '1.16.0', 'to': '1.17.0'}),
        ])
        self.assertEqual(result['job']['name'], 'tools')
        text = (folder / 'pyproject.toml').read_text()
        self.assertIn('# Kept when the Workers page edits the file.', text)
        self.assertIn('"six==1.17.0"', text)
        self.assertIn('label = "Tools worker"', text)

    def test_a_name_change_needs_no_lock_and_keeps_the_environment(self):
        folder = self.worker()
        lock = (folder / 'uv.lock').read_text()
        digest = environments.named_workers(self.repo_path)['tools'].digest
        result = python_workers.update(self.repo_path, 'tools', label='Feature tools',
                                       description='Shared helpers', combine=False)
        self.assertIsNone(result['job'])
        worker = environments.named_workers(self.repo_path)['tools']
        self.assertEqual((worker.label, worker.description, worker.combine),
                         ('Feature tools', 'Shared helpers', False))
        self.assertEqual((folder / 'uv.lock').read_text(), lock)
        self.assertEqual(worker.digest, digest)

    def test_a_project_named_like_a_new_package_is_renamed(self):
        folder = self.worker()
        text = (folder / 'pyproject.toml').read_text().replace(
            'mageml-worker-tools', 'packaging')
        (folder / 'pyproject.toml').write_text(text)
        python_workers.update(self.repo_path, 'tools', dependencies=['packaging==24.1'])
        self.assertIn('name = "mageml-worker-tools"', (folder / 'pyproject.toml').read_text())

    def test_an_update_is_previewed_then_applied_as_shown(self):
        folder = self.worker(dependencies=['six>=1.15'], upgrade_package='six==1.15.0')
        shown = python_workers.preview(self.repo_path, 'tools')
        [change] = shown['changes']
        self.assertEqual((change['package'], change['from']), ('six', '1.15.0'))
        self.assertIn('version = "1.15.0"', (folder / 'uv.lock').read_text(),
                      'a preview changes nothing')
        applied = python_workers.apply(self.repo_path, shown['id'])
        self.assertEqual(applied['job']['name'], 'tools')
        self.assertIn(f'version = "{change["to"]}"', (folder / 'uv.lock').read_text())
        with self.assertRaisesRegex(environments.PipelineEnvironmentError, 'expired'):
            python_workers.apply(self.repo_path, shown['id'])

        nothing = python_workers.preview(self.repo_path, 'tools')
        self.assertIsNone(nothing['id'])
        self.assertIn('newest release its ranges allow', nothing['message'])

    def test_a_preview_of_a_worker_that_changed_since_is_refused(self):
        folder = self.worker(dependencies=['six>=1.15'], upgrade_package='six==1.15.0')
        shown = python_workers.preview(self.repo_path, 'tools', packages=['six'])
        subprocess.run(['uv', 'lock', '--project', str(folder), '--upgrade-package',
                        'six==1.16.0'], check=True, capture_output=True)
        with self.assertRaisesRegex(environments.PipelineEnvironmentError,
                                    'changed since the preview'):
            python_workers.apply(self.repo_path, shown['id'])

    def test_vulnerabilities_are_fixed_to_the_release_that_fixes_them_or_held(self):
        free = self.worker('free', dependencies=['six>=1.15'], upgrade_package='six==1.15.0')
        self.worker('pinned', dependencies=['six==1.16.0'])
        found = [vulnerabilities.VulnerablePackage('six', '1.15.0', [
            vulnerabilities.Advisory('GHSA-1', [], 'First', None, ['1.16.0']),
            vulnerabilities.Advisory('GHSA-2', ['CVE-2'], 'Second', None, ['1.17.0']),
        ])]
        with patch.object(python_workers.vulnerabilities, 'audit', return_value=found):
            report = python_workers.audit(self.repo_path, 'free')
            # Both advisories: the fix of one alone would leave the other open.
            self.assertEqual((report['state'], report['packages'][0]['fix']),
                             ('vulnerable', '1.17.0'))
            self.assertEqual(python_workers.cached_audit(
                environments.named_workers(self.repo_path)['free'])['advisories'], 2)
            shown = python_workers.preview(self.repo_path, 'free', fix_vulnerabilities=True)
            self.assertEqual(shown['changes'][0]['to'], '1.17.0')
            python_workers.apply(self.repo_path, shown['id'])
            self.assertIn('version = "1.17.0"', (free / 'uv.lock').read_text())

        # A range that excludes the fix holds it back, with uv's reason.
        pinned = [vulnerabilities.VulnerablePackage('six', '1.16.0', [
            vulnerabilities.Advisory('GHSA-2', [], 'Second', None, ['1.17.0'])])]
        with patch.object(python_workers.vulnerabilities, 'audit', return_value=pinned):
            held = python_workers.preview(self.repo_path, 'pinned', fix_vulnerabilities=True)
        self.assertIsNone(held['id'])
        self.assertEqual(held['held'][0]['package'], 'six')
        self.assertIn('six==1.16.0', held['held'][0]['reason'])

    def test_an_audit_that_cannot_reach_osv_is_unchecked_and_not_kept(self):
        self.worker()
        error = vulnerabilities.AuditError('error sending request', offline=True)
        with patch.object(python_workers.vulnerabilities, 'audit', side_effect=error):
            report = python_workers.audit(self.repo_path, 'tools')
        self.assertEqual(report['state'], 'unchecked')
        self.assertIn('could not reach the vulnerability database', report['reason'])
        worker = environments.named_workers(self.repo_path)['tools']
        self.assertEqual(python_workers.cached_audit(worker), dict(state='unchecked'))

    def test_a_worker_is_deleted_only_when_no_block_runs_in_it(self):
        folder = self.worker()
        pipeline, load = self.pipeline(block_worker='tools')
        with self.assertRaisesRegex(environments.PipelineEnvironmentError,
                                    f'runs blocks of 1 pipeline: {pipeline.uuid} \\({load}\\)'):
            python_workers.delete(self.repo_path, 'tools')
        self.assertTrue(folder.is_dir())

        pipeline.get_block(load).update(dict(configuration={}))
        environments.prepare(environments.named_workers(self.repo_path)['tools'],
                             log=lambda _: None)
        prepared = environments.named_workers(self.repo_path)['tools'].directory
        result = python_workers.delete(self.repo_path, 'tools')
        self.assertEqual(result['environments_removed'], 1)
        self.assertFalse(folder.exists() or prepared.exists())

    def test_the_api_manages_workers_for_editors_and_shows_them_to_viewers(self):
        import asyncio

        from mage_ai.api.operations.base import BaseOperation
        from mage_ai.api.operations.constants import OperationType
        from mage_ai.orchestration.db.models.oauth import Role
        from mage_ai.tests.factory import create_user

        folder = self.worker(dependencies=['six>=1.15'], upgrade_package='six==1.15.0')
        Role.create_default_roles()
        editor = create_user(roles_new=[Role.get_role('Editor')])
        viewer = create_user(roles_new=[Role.get_role('Viewer')])

        def call(user, action, resource, pk=None, payload=None, query=None):
            return asyncio.run(BaseOperation(
                action=action, pk=pk, payload=payload or {}, query=query or {},
                resource=resource, user=user,
            ).execute())

        shown = call(viewer, OperationType.DETAIL, 'python_workers', 'tools')['python_worker']
        self.assertEqual(shown['packages'][0]['name'], 'six')
        self.assertEqual(shown['used_by'], [])
        listed = call(viewer, OperationType.LIST, 'python_workers')['python_workers']
        self.assertEqual([w['name'] for w in listed], ['tools'])

        rename = dict(python_worker=dict(label='Renamed'))
        self.assertEqual(
            call(viewer, OperationType.UPDATE, 'python_workers', 'tools', rename)['error']['code'],
            403)
        renamed = call(editor, OperationType.UPDATE, 'python_workers', 'tools', rename)
        self.assertEqual(renamed['python_worker']['label'], 'Renamed')
        bad = call(editor, OperationType.UPDATE, 'python_workers', 'tools',
                   dict(python_worker=dict(dependencies=['six==99'])))
        self.assertIn('nothing changed', bad['error']['message'])

        preview = call(editor, OperationType.CREATE, 'python_worker_updates',
                       payload=dict(python_worker_update=dict(worker='tools')))
        update = preview['python_worker_update']
        self.assertEqual(update['changes'][0]['package'], 'six')
        applied = call(editor, OperationType.UPDATE, 'python_worker_updates', update['id'],
                       dict(python_worker_update={}))
        self.assertEqual(applied['python_worker_update']['job']['name'], 'tools')
        self.assertNotIn('version = "1.15.0"', (folder / 'uv.lock').read_text())

        found = [vulnerabilities.VulnerablePackage('six', '1.17.0', [
            vulnerabilities.Advisory('GHSA-1', [], 'Bad', 'https://osv.dev/GHSA-1', [])])]
        with patch.object(python_workers.vulnerabilities, 'audit', return_value=found):
            report = call(viewer, OperationType.DETAIL, 'python_worker_audits', 'tools',
                          query=dict(refresh=['1']))['python_worker_audit']
        self.assertEqual((report['state'], report['packages'][0]['unfixed']),
                         ('vulnerable', ['GHSA-1']))

        self.assertEqual(
            call(viewer, OperationType.DELETE, 'python_workers', 'tools')['error']['code'], 403)
        call(editor, OperationType.DELETE, 'python_workers', 'tools')
        self.assertFalse(folder.exists())
