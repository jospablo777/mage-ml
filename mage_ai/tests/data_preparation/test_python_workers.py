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

    def test_a_new_worker_exists_only_once_uv_locked_its_packages(self):
        folder = Path(self.repo_path, environments.WORKERS_DIR, 'scoring')
        self.addCleanup(shutil.rmtree, folder, True)
        with self.assertRaisesRegex(environments.PipelineEnvironmentError,
                                    'so no worker was added'):
            python_workers.create(self.repo_path, 'scoring', dependencies=['six==99.0'])
        self.assertFalse(folder.exists())
        with self.assertRaisesRegex(environments.PipelineEnvironmentError,
                                    'not a package requirement'):
            python_workers.create(self.repo_path, 'scoring', dependencies=['six=='])

        created = python_workers.create(
            self.repo_path, 'Scoring', label='Scoring worker', dependencies=['six==1.16.0'],
            python='3.12',
        )
        self.assertEqual(created['name'], 'scoring')
        self.assertEqual(sorted(p.name for p in folder.iterdir()),
                         ['.python-version', 'pyproject.toml', 'uv.lock'])
        worker = environments.named_workers(self.repo_path)['scoring']
        self.assertEqual((worker.label, worker.python), ('Scoring worker', '3.12'))
        self.assertIn('name = "mageml-worker-scoring"', (folder / 'pyproject.toml').read_text())
        with self.assertRaisesRegex(environments.PipelineEnvironmentError, 'exists already'):
            python_workers.create(self.repo_path, 'scoring', dependencies=['six==1.16.0'])

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

    def test_a_package_without_its_version_gets_the_newest_release(self):
        folder = self.worker(dependencies=['six==1.16.0', 'packaging==24.1'])
        result = python_workers.update(self.repo_path, 'tools',
                                       dependencies=['six', 'packaging==24.1'])
        [change] = result['changes']
        self.assertEqual((change['package'], change['from']), ('six', '1.16.0'))
        self.assertNotEqual(change['to'], '1.16.0', 'uv kept the locked six')
        self.assertIn('version = "24.1"', (folder / 'uv.lock').read_text(),
                      'an unchanged line keeps its version')

    def test_the_pyproject_is_edited_as_text_and_locked_first(self):
        folder = self.worker()
        before = {n: (folder / n).read_text() for n in ('pyproject.toml', 'uv.lock')}
        self.assertEqual(python_workers.detail(self.repo_path, 'tools')['pyproject'],
                         before['pyproject.toml'])
        for text, message in (
            ('[project\nname = "x"', 'is not valid TOML'),
            ('[tool.uv]\n', 'needs a \\[project\\] table'),
            ('[project]\n', 'needs a name, such as name = "mageml-worker-tools"'),
            ('[project]\nname = "x"\ndependencies = "six"\n', 'is a list of packages'),
            (before['pyproject.toml'].replace('mageml-worker-tools', 'six'),
             'like the package it needs'),
            (before['pyproject.toml'].replace('six==1.16.0', 'six==99.0'), 'nothing changed'),
        ):
            with self.assertRaisesRegex(environments.PipelineEnvironmentError, message):
                python_workers.save_pyproject(self.repo_path, 'tools', text)
            self.assertEqual({n: (folder / n).read_text() for n in before}, before)

        edited = before['pyproject.toml'].replace(
            '"six==1.16.0"', '"six==1.17.0"').replace('# Kept', '# Still kept')
        result = python_workers.save_pyproject(self.repo_path, 'tools', edited)
        self.assertEqual(result['changes'][0]['to'], '1.17.0')
        self.assertEqual((folder / 'pyproject.toml').read_text(), edited)

    def test_uv_output_in_a_message_has_no_color_codes(self):
        # FORCE_COLOR (Playwright sets it for the server it starts) made uv color its errors,
        # and the Workers page showed the escape codes.
        folder = self.worker()
        text = (folder / 'pyproject.toml').read_text().replace('six==1.16.0', 'six==99.0')

        with patch.dict('os.environ', {'FORCE_COLOR': '1'}), \
                self.assertRaises(environments.PipelineEnvironmentError) as raised:
            python_workers.save_pyproject(self.repo_path, 'tools', text)

        self.assertIn('there is no version of six==99.0', str(raised.exception))
        self.assertNotIn('\x1b', str(raised.exception))
        self.assertEqual(environments.plain('\x1b[1m\x1b[31merror\x1b[39m\x1b[0m: no'), 'error: no')

    def releases(self, latest='1.17.0'):
        from mage_ai.data_preparation import package_index

        return patch.object(package_index, 'releases', return_value=dict(
            name='six', latest=latest, versions=['1.17.0', '1.16.0', '1.15.0'],
            prereleases=[], uploaded={'1.16.0': '2021-05-05T14:18:17Z'},
        ))

    def test_a_package_the_other_packages_keep_old_is_named_with_why(self):
        # As feast and pandas 3: the newest feast needs pandas<3, so uv locks feast 0.20.0
        # from 2022, whose pandas>=1 was written before pandas 3. Here a constraint holds six.
        folder = self.worker(dependencies=('six',))
        with (folder / 'pyproject.toml').open('a') as file:
            file.write('\n[tool.uv]\nconstraint-dependencies = ["six<1.17"]\n')
        subprocess.run(['uv', 'lock', '--project', str(folder)], check=True,
                       capture_output=True)

        with self.releases():
            report = python_workers.held_back(self.repo_path, 'tools')

        self.assertEqual(report['state'], 'held')
        [package] = report['packages']
        self.assertEqual((package['package'], package['locked'], package['latest'],
                          package['state'], package['behind']),
                         ('six', '1.16.0', '1.17.0', 'held', 1))
        self.assertEqual(package['locked_uploaded'], '2021-05-05T14:18:17Z')
        self.assertIn('six<1.17', package['uv_reason'])
        self.assertTrue(package['reason'].startswith('six 1.17.0 does not fit: Because'))
        worker = environments.named_workers(self.repo_path)['tools']
        self.assertEqual(python_workers.cached_held_back(worker), ['six'])
        self.assertEqual(python_workers.overview(self.repo_path)[0]['held_back'], ['six'])

    def test_a_change_that_would_lock_an_older_release_asks_first(self):
        # No version chosen, and the newest does not fit: nothing changes until people say
        # to go on ("uv would lock feast 0.20.0 from April 2022, 4 years old...").
        folder = self.worker(dependencies=('six==1.15.0',))
        with (folder / 'pyproject.toml').open('a') as file:
            file.write('\n[tool.uv]\nconstraint-dependencies = ["six<1.17"]\n')
        subprocess.run(['uv', 'lock', '--project', str(folder)], check=True,
                       capture_output=True)
        before = {n: (folder / n).read_text() for n in ('pyproject.toml', 'uv.lock')}

        with self.releases():
            asked = python_workers.update(self.repo_path, 'tools', dependencies=['six'])
        self.assertIsNone(asked['job'])
        [package] = asked['confirm']
        self.assertEqual((package['package'], package['locked'], package['latest']),
                         ('six', '1.16.0', '1.17.0'))
        self.assertRegex(package['question'],
                         r'^uv would lock six 1\.16\.0 from May 2021, \d+ years old; the '
                         r'newest is 1\.17\.0\. six 1\.17\.0 does not fit: Because')
        self.assertEqual({n: (folder / n).read_text() for n in before}, before)

        with self.releases():
            done = python_workers.update(self.repo_path, 'tools', dependencies=['six'],
                                         accept_older=True)
        self.assertNotIn('confirm', done)
        self.assertIn('"six"', (folder / 'pyproject.toml').read_text())
        # Held at the same release as before the change: accepted once, not asked again.
        with self.releases():
                again = python_workers.update(self.repo_path, 'tools',
                                          dependencies=['six', 'pyarrow'])
        self.assertNotIn('confirm', again)

    def test_versions_that_do_not_fit_are_refused_in_one_sentence(self):
        with self.assertRaisesRegex(
            environments.PipelineEnvironmentError,
            r'^uv could not lock these packages, so no worker was added: There is no release '
            r'six==99\.0 on the package index\.\n\n',
        ):
            python_workers.create(self.repo_path, 'other', dependencies=['six==99.0'])
        self.assertFalse(Path(self.repo_path, environments.WORKERS_DIR, 'other').exists())
        uv = ('Because feast==0.68.0 depends on pandas>=1.4.3,<3 and your project depends on '
              'feast==0.68.0 and pandas==3.1.1, we can conclude that your project\'s '
              'requirements are unsatisfiable.')
        self.assertEqual(
            python_workers._conflict(
                f'  × No solution found when resolving dependencies:\n  ╰─▶ {uv}\n',
                '[project]\nname = "w"\ndependencies = ["feast==0.68.0", "pandas==3.1.1"]\n'),
            'feast 0.68.0 needs pandas>=1.4.3,<3, and the worker lists pandas==3.1.1.')

    def test_a_worker_that_cannot_be_read_is_listed_with_why_and_fixed_from_its_file(self):
        self.worker()
        folder = Path(self.repo_path, environments.WORKERS_DIR, 'broken')
        folder.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, folder, True)
        (folder / 'pyproject.toml').write_text('[project\nname =')

        listed = {w['name']: w for w in python_workers.overview(self.repo_path)}
        self.assertTrue(listed['tools'].get('locked'), 'the other workers still work')
        self.assertIn('cannot be read', listed['broken']['broken'])
        shown = python_workers.detail(self.repo_path, 'broken')
        self.assertEqual((shown['pyproject'], shown['packages']), ('[project\nname =', []))
        with self.assertRaisesRegex(environments.PipelineEnvironmentError,
                                    'Python worker broken cannot be used'):
            python_workers.update(self.repo_path, 'broken', dependencies=['six'])

        python_workers.save_pyproject(self.repo_path, 'broken', PYPROJECT.format(
            name='broken', dependencies='["six==1.16.0"]', label='Fixed'))
        self.assertIn('broken', environments.named_workers(self.repo_path))
        self.assertTrue((folder / 'uv.lock').is_file())

    def test_a_uv_lock_that_cannot_be_read_is_shown_and_locked_anew_on_save(self):
        folder = self.worker()
        (folder / 'uv.lock').write_text('garbage [[[')

        self.assertIn('uv.lock cannot be read', python_workers.detail(
            self.repo_path, 'tools')['lock_problem'])
        self.assertEqual(python_workers.held_back(self.repo_path, 'tools')['state'],
                         'unchecked')
        result = python_workers.update(self.repo_path, 'tools', dependencies=['six==1.17.0'])
        self.assertEqual(result['changes'], [{'package': 'six', 'from': None, 'to': '1.17.0'}])
        self.assertIn('version = "1.17.0"', (folder / 'uv.lock').read_text())

    def test_a_lock_that_takes_too_long_says_so_and_changes_nothing(self):
        folder = self.worker()
        before = (folder / 'pyproject.toml').read_text()

        def slow(command, **kwargs):
            raise subprocess.TimeoutExpired(command, kwargs.get('timeout'))

        with patch.object(python_workers.subprocess, 'run', side_effect=slow), \
                self.assertRaisesRegex(environments.PipelineEnvironmentError,
                                       'uv took over 10 minutes to lock'):
            python_workers.update(self.repo_path, 'tools', dependencies=['six==1.17.0'])
        self.assertEqual((folder / 'pyproject.toml').read_text(), before)

    def test_a_package_only_locked_before_its_newest_release_is_behind(self):
        self.worker(dependencies=('six',), upgrade_package='six==1.16.0')

        with self.releases():
            report = python_workers.held_back(self.repo_path, 'tools')

        self.assertEqual(report['state'], 'current')
        self.assertEqual([(p['package'], p['state']) for p in report['packages']],
                         [('six', 'behind')])

    def test_a_package_pinned_below_its_newest_release_is_kept_as_asked(self):
        self.worker(dependencies=('six==1.16.0',))

        with self.releases():
            report = python_workers.held_back(self.repo_path, 'tools')

        self.assertEqual((report['state'], report['packages']), ('current', []))

    def test_why_names_what_the_newest_release_needs_and_the_line_that_rules_it_out(self):
        uv_reason = (
            'Because only feast<0.69.0.dev156 is available and feast==0.68.0 depends on '
            'pandas>=1.4.3,<3, we can conclude that feast>=0.68.0 depends on pandas>=1.4.3,<3. '
            'And because your project depends on feast>=0.68.0 and pandas==3.1.1, we can '
            "conclude that your project's requirements are unsatisfiable.")
        listed = ['pyarrow', 'feast>=0.68.0', 'pandas==3.1.1']

        self.assertEqual(
            python_workers._held_reason('feast', '0.68.0', uv_reason, listed),
            'feast 0.68.0 needs pandas>=1.4.3,<3, and the worker lists pandas==3.1.1.')
        self.assertEqual(
            python_workers._held_reason(
                'feast', '0.68.0', 'Because feast==0.68.0 depends on Python>=3.15 and ...',
                listed),
            'feast 0.68.0 needs Python>=3.15, and the worker runs another Python.')

    def test_pandas_gets_pyarrow_and_the_page_says_why(self):
        # A worker with pandas and no pyarrow failed its first block that took a table:
        # tables cross as Arrow, which pandas reads with pyarrow.
        folder = self.worker()
        with patch.object(python_workers, '_older_releases', return_value=[]):
            result = python_workers.update(self.repo_path, 'tools',
                                           dependencies=['six==1.16.0', 'pandas'])
        self.assertEqual(result['notes'], [python_workers.PYARROW_NOTE])
        self.assertIn('"pyarrow"', (folder / 'pyproject.toml').read_text())

        self.assertEqual(python_workers._with_pyarrow(['polars']), (['polars'], None))
        self.assertEqual(python_workers._with_pyarrow(['pandas', 'PyArrow>=17']),
                         (['pandas', 'PyArrow>=17'], None))

    def test_a_memory_limit_is_set_checked_and_cleared(self):
        folder = self.worker()
        python_workers.update(self.repo_path, 'tools', memory='3 GiB')
        self.assertEqual(environments.named_workers(self.repo_path)['tools'].memory_limit,
                         3 * 2 ** 30)
        with self.assertRaisesRegex(environments.PipelineEnvironmentError,
                                    'Memory: "plenty" is not a size'):
            python_workers.update(self.repo_path, 'tools', memory='plenty')
        python_workers.update(self.repo_path, 'tools', memory='')
        self.assertIsNone(environments.named_workers(self.repo_path)['tools'].memory_limit)
        self.assertNotIn('memory', (folder / 'pyproject.toml').read_text())

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

        templates = call(viewer, OperationType.LIST, 'python_worker_templates')
        feast = next(t for t in templates['python_worker_templates'] if t['id'] == 'feast')
        self.assertEqual(feast['dependencies'],
                         environments.WORKER_TEMPLATES['feast']['dependencies'])

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


class MagemlEnvironmentTest(unittest.TestCase):
    """MageML's own Python is shown with how to change it, for the way it is installed."""

    def environment(self, prefix, cwd=None):
        with patch.object(python_workers.sys, 'prefix', str(prefix)), \
                patch.object(python_workers.Path, 'cwd', return_value=Path(cwd or prefix)):
            return python_workers.mageml_environment()

    def test_a_uv_project_is_changed_with_uv_add_in_its_folder(self):
        project = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, project, True)
        (project / 'pyproject.toml').write_text(
            '[project]\nname = "shop"\ndependencies = ["mage-ml[postgres]"]\n')

        found = self.environment(project / '.venv')

        self.assertEqual((found['name'], found['builtin'], found['kind']), ('mage', True,
                                                                          'project'))
        self.assertEqual(found['project'], str(project / 'pyproject.toml'))
        self.assertEqual(found['commands'][:2], [f'cd "{project}"', 'uv add scikit-learn'])
        names = [p['name'] for p in found['main_packages']]
        self.assertIn('pandas', names)
        self.assertEqual(len(names), len(set(names)), 'each package once')

    def test_a_uv_tool_and_another_environment_say_their_own_command(self):
        tool = Path(tempfile.mkdtemp()) / 'uv' / 'tools' / 'mage-ml'
        elsewhere = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, elsewhere, True)

        self.assertEqual(self.environment(tool, cwd=elsewhere)['commands'],
                         ['uv tool install mage-ml --with scikit-learn --reinstall'])
        other = self.environment(elsewhere / 'env', cwd=elsewhere)
        self.assertEqual(other['kind'], 'environment')
        self.assertTrue(other['commands'][0].startswith('uv pip install --python '))
