"""
The lifecycle of a service saved in the project: create, check, build with a lock that
pins what was built, export, and the services a pipeline belongs to.
"""
import shutil
import tempfile
from pathlib import Path

import yaml
from typer.testing import CliRunner

from mage_ai.cli.main import app
from mage_ai.pipeline_services import definitions, lifecycle
from mage_ai.tests.pipeline_services.test_export import ExportTestCase


class LifecycleTest(ExportTestCase):
    def setUp(self):
        super().setUp()
        root = Path(self.repo_path)
        for path in (root / 'services', root / '.mage'):
            self.addCleanup(shutil.rmtree, path, True)
        gitignore = root / '.gitignore'
        before = gitignore.read_text() if gitignore.exists() else None
        self.addCleanup(lambda: gitignore.write_text(before) if before is not None
                        else gitignore.unlink(missing_ok=True))
        self.chain()

    def test_a_new_service_has_its_definition_and_a_dev_environment(self):
        written = lifecycle.new(self.repo_path, 'sales', [self.pipeline.uuid], 'Daily sales')

        names = [str(p.relative_to(self.repo_path)) for p in written]
        self.assertEqual(names, ['services/sales/service.yaml',
                                 'services/sales/environments/dev.yaml'])
        saved = definitions.load_definition(self.repo_path, 'sales')
        self.assertEqual(saved['pipelines'], [self.pipeline.uuid])
        self.assertEqual(saved['description'], 'Daily sales')
        self.assertEqual(lifecycle.services_using(self.repo_path, self.pipeline.uuid), ['sales'])

    def test_a_new_service_names_unknown_pipelines(self):
        with self.assertRaisesRegex(definitions.DefinitionError, 'has no pipeline nope'):
            lifecycle.new(self.repo_path, 'sales', ['nope'])
        with self.assertRaisesRegex(definitions.DefinitionError, 'already exists'):
            lifecycle.new(self.repo_path, 'sales', [self.pipeline.uuid])
            lifecycle.new(self.repo_path, 'sales', [self.pipeline.uuid])

    def test_check_says_what_stops_a_build(self):
        definitions.save_definition(self.repo_path, 'broken', dict(pipelines=['missing']))

        result = lifecycle.check(self.repo_path, 'broken')

        self.assertFalse(result.ok)
        self.assertTrue(any('missing' in p for p in result.problems), result.problems)
        self.assertTrue(lifecycle.check(self.repo_path, 'nope').problems)

    def test_a_build_writes_the_box_outside_git_and_a_lock_that_changes_only_with_its_inputs(self):
        lifecycle.new(self.repo_path, 'sales', [self.pipeline.uuid])

        box = lifecycle.build(self.repo_path, 'sales')

        expected = Path(self.repo_path) / '.mage' / 'build' / 'sales'
        self.assertEqual(box.resolve(), expected.resolve())
        self.assertTrue((box / 'service.json').is_file())
        self.assertIn('.mage/build/', (Path(self.repo_path) / '.gitignore').read_text())
        first = lifecycle.lock_path(self.repo_path, 'sales').read_text()
        self.assertTrue(first.startswith('# Written by `mage service build`; commit it.'))
        locked = yaml.safe_load(first)
        self.assertEqual(locked['service'], 'sales')
        for key in ('manifest_sha256', 'source_sha256', 'requirements_sha256', 'engine_sha256'):
            self.assertEqual(len(locked[key]), 64, key)

        # The same inputs build the same lock, whenever they are built.
        lifecycle.build(self.repo_path, 'sales')
        self.assertEqual(lifecycle.lock_path(self.repo_path, 'sales').read_text(), first)

        # A changed block changes it.
        block_file = next((Path(self.repo_path) / 'transformers').glob('*summary.py'))
        block_file.write_text(block_file.read_text() + '\n# changed\n')
        lifecycle.build(self.repo_path, 'sales')
        changed = lifecycle.read_lock(self.repo_path, 'sales')
        self.assertNotEqual(changed['source_sha256'], locked['source_sha256'])
        self.assertNotEqual(changed['manifest_sha256'], locked['manifest_sha256'])

    def test_export_writes_the_box_where_asked(self):
        lifecycle.new(self.repo_path, 'sales', [self.pipeline.uuid])
        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)

        lifecycle.export_to(self.repo_path, 'sales', str(out), force=True)

        self.assertTrue((out / 'Dockerfile').is_file())
        self.assertIsNone(lifecycle.read_lock(self.repo_path, 'sales'))

    def test_the_commands_create_check_and_build(self):
        runner = CliRunner()
        project = ['--project', self.repo_path]

        created = runner.invoke(app, ['service', 'new', 'sales', '--pipelines',
                                      self.pipeline.uuid, *project])
        self.assertEqual(created.exit_code, 0, created.output)
        self.assertIn('Wrote services/sales/service.yaml', created.output)

        listed = runner.invoke(app, ['service', 'list', *project])
        self.assertIn('sales', listed.output)
        self.assertIn('services/sales/service.yaml', listed.output)

        checked = runner.invoke(app, ['service', 'check', 'sales', *project])
        self.assertEqual(checked.exit_code, 0, checked.output)
        self.assertIn('ready to build', checked.output)

        built = runner.invoke(app, ['service', 'build', 'sales', *project])
        self.assertEqual(built.exit_code, 0, built.output)
        self.assertIn('services/sales/service.lock (commit it)', built.output)

        locked = runner.invoke(app, ['service', 'check', 'sales', '--locked', *project])
        self.assertEqual(locked.exit_code, 0, locked.output)
        self.assertIn('with its lock committed', locked.output)
        # A committed lock that is not the build's: CI says to build and commit it.
        lock = lifecycle.lock_path(self.repo_path, 'sales')
        lock.write_text(lock.read_text().replace('manifest_sha256: ', 'manifest_sha256: 0'))
        stale = runner.invoke(app, ['service', 'check', 'sales', '--locked', *project])
        self.assertEqual(stale.exit_code, 1)
        self.assertIn('service.lock is not the build', stale.output)

        failed = runner.invoke(app, ['service', 'check', 'nope', *project])
        self.assertEqual(failed.exit_code, 1)


class AffectedServicesTest(ExportTestCase):
    def setUp(self):
        super().setUp()
        root = Path(self.repo_path)
        for path in (root / 'services', root / '.mage'):
            self.addCleanup(shutil.rmtree, path, True)
        gitignore = root / '.gitignore'
        before = gitignore.read_text() if gitignore.exists() else None
        self.addCleanup(lambda: gitignore.write_text(before) if before is not None
                        else gitignore.unlink(missing_ok=True))
        self.load, self.summary, _ = self.chain()
        lifecycle.new(self.repo_path, 'sales', [self.pipeline.uuid])

    def test_a_change_affects_the_services_that_read_it(self):
        """CI had no way to find the services a pull request changes."""
        block = Path(self.load.file_path).resolve().relative_to(
            Path(self.repo_path).resolve()).as_posix()

        self.assertEqual(lifecycle.affected(self.repo_path, [block]), ['sales'])
        self.assertEqual(lifecycle.affected(self.repo_path, ['utils/rates.py']), ['sales'])
        self.assertEqual(lifecycle.affected(self.repo_path, ['io_config.yaml']), ['sales'])
        self.assertEqual(
            lifecycle.affected(self.repo_path, ['services/sales/environments/dev.yaml']),
            ['sales'],
        )
        self.assertEqual(lifecycle.affected(self.repo_path, ['notes/README.md']), [])

    def test_changed_paths_lists_committed_and_uncommitted_changes(self):
        import subprocess

        work = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, work, True)

        def git(*args):
            subprocess.run(['git', *args], cwd=work, check=True, capture_output=True)

        git('init', '-q', '-b', 'main')
        git('config', 'user.email', 'test@example.com')
        git('config', 'user.name', 'test')
        (work / 'a.py').write_text('a = 1\n')
        git('add', '.')
        git('commit', '-q', '-m', 'base')
        git('checkout', '-q', '-b', 'change')
        (work / 'b.py').write_text('b = 1\n')
        git('add', '.')
        git('commit', '-q', '-m', 'b')
        (work / 'a.py').write_text('a = 2\n')

        self.assertEqual(lifecycle.changed_paths(str(work), 'main'), ['a.py', 'b.py'])

    def test_uncommitted_inputs_name_what_a_deploy_would_ship_uncommitted(self):
        """A deploy could ship uncommitted code, and nothing recorded what ran."""
        import subprocess

        root = Path(self.repo_path)
        captured = lifecycle.capture(self.repo_path, 'sales')
        self.assertIsNone(lifecycle.uncommitted_inputs(self.repo_path, 'sales', captured))

        self.addCleanup(shutil.rmtree, root / '.git', True)

        def git(*args):
            subprocess.run(['git', *args], cwd=root, check=True, capture_output=True)

        git('init', '-q', '-b', 'main')
        git('config', 'user.email', 'test@example.com')
        git('config', 'user.name', 'test')
        # Every untracked file counts, not only the folder holding them.
        uncommitted = lifecycle.uncommitted_inputs(self.repo_path, 'sales', captured)
        self.assertIn('services/sales/service.yaml', uncommitted)
        self.assertNotIn('services', uncommitted)

        git('add', '.')
        git('commit', '-q', '-m', 'base')
        self.assertEqual(lifecycle.uncommitted_inputs(self.repo_path, 'sales', captured), [])

        block = Path(self.load.file_path).resolve().relative_to(root.resolve()).as_posix()
        Path(self.load.file_path).write_text(Path(self.load.file_path).read_text() + '\n')
        (root / 'notes.md').write_text('not an input\n')
        self.assertEqual(lifecycle.uncommitted_inputs(self.repo_path, 'sales', captured),
                         [block])
