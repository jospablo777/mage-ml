import json
import os
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

from mage_ai.data_preparation.models.block.r import runtime
from mage_ai.tests.base_test import TestCase

PROJECT = '''[project]
name = "p"
r_version = "4.6"
repositories = []
dependencies = [
    "tidyverse",
    {name = "arrow", repository = "PPM"},
    "bit64",
    "jsonlite",
    "tibble",
]
'''


class RuntimeTestCase(TestCase):
    def setUp(self):
        super().setUp()
        runtime._prepared.clear()
        self.repo = Path(tempfile.mkdtemp())
        self.project = self.repo / 'r'
        self.library = self.project / 'rv' / 'library' / '4.6' / 'arm64'
        self.library.mkdir(parents=True)
        for name in runtime.EXCHANGE_PACKAGES:
            (self.library / name).mkdir()
            (self.library / name / 'DESCRIPTION').write_text(f'Package: {name}')
        (self.project / 'rproject.toml').write_text(PROJECT)
        (self.project / 'rv.lock').write_text('')
        self.environ = patch.dict(os.environ, {}, clear=False)
        self.environ.start()
        for name in (
            'MAGE_R_PROJECT_DIR', 'MAGE_RSCRIPT', 'MAGE_RV', 'MAGE_R_SYNC', 'MAGE_R_TIMEOUT',
        ):
            os.environ.pop(name, None)
        self.cache = Path(tempfile.mkdtemp())
        os.environ['MAGE_R_CACHE_DIR'] = str(self.cache)
        self.which = patch.object(runtime.shutil, 'which', side_effect=lambda name: f'/bin/{name}')
        self.which.start()
        self.calls = []
        self.pending = {'installed': [], 'removed': []}
        self.r_version = '4.6.1'
        self.install_fails = False
        self.install = patch.object(runtime.subprocess, 'run', side_effect=self.fake_install)
        self.install.start()

    def tearDown(self):
        self.install.stop()
        self.which.stop()
        self.environ.stop()
        super().tearDown()

    def fake_install(self, args, **kwargs):
        assert args[3] == runtime.INSTALL_MAGEML, args
        self.calls.append('install')
        self.install_args = args[4:]
        if self.install_fails:
            return subprocess.CompletedProcess(args, 1, 'ERROR: dependency arrow missing', '')
        staging = Path(args[5])
        (staging / 'mageml').mkdir()
        (staging / 'mageml' / 'DESCRIPTION').write_text('Package: mageml')
        return subprocess.CompletedProcess(args, 0, '', '')

    def fake_run(self, args, cwd=None, timeout=600):
        self.calls.append(args[1] if args[1] != '--vanilla' else 'version')
        if args[1] == '--vanilla':
            return self.r_version
        if args[1] == 'plan':
            return json.dumps(self.pending)
        if args[1] == 'library':
            return json.dumps({'directory': str(self.library)})
        if args[1] == 'sync':
            self.pending = {'installed': [], 'removed': []}
            return ''
        raise AssertionError(args)

    def prepare(self, **settings):
        os.environ.update(settings)
        with patch.object(runtime, '_run', side_effect=self.fake_run):
            return runtime.prepare(runtime.r_config(str(self.repo)))


class ConfigTest(RuntimeTestCase):
    def test_project_directory(self):
        self.assertEqual(runtime.r_config(str(self.repo)).project_dir, self.project.resolve())

        os.environ['MAGE_R_PROJECT_DIR'] = 'missing'
        with self.assertRaisesRegex(runtime.REnvironmentError, 'has no rproject.toml'):
            runtime.r_config(str(self.repo))

    def test_without_rv_project(self):
        config = runtime.r_config(tempfile.mkdtemp())

        self.assertFalse(config.uses_rv)
        with patch.object(runtime, '_run', side_effect=self.fake_run):
            libraries = runtime.prepare(config)
        self.assertIsNone(libraries.rv)
        self.assertTrue((libraries.mageml / 'mageml' / 'DESCRIPTION').exists())
        # mageml is installed with the R installation's own library.
        self.assertEqual(len(self.install_args), 2)

    def test_settings(self):
        os.environ.update(MAGE_R_SYNC='AUTO', MAGE_R_TIMEOUT='2.5', MAGE_RSCRIPT='/opt/R/Rscript')
        config = runtime.r_config(str(self.repo))

        self.assertEqual(
            (config.sync, config.timeout, config.rscript), ('auto', 2.5, '/opt/R/Rscript'),
        )

        os.environ['MAGE_R_SYNC'] = 'sometimes'
        with self.assertRaisesRegex(runtime.REnvironmentError, 'MAGE_R_SYNC must be one of'):
            runtime.r_config(str(self.repo))

    def test_dependencies(self):
        self.assertEqual(
            runtime.project_dependencies(self.project),
            ['tidyverse', 'arrow', 'bit64', 'jsonlite', 'tibble'],
        )


class PrepareTest(RuntimeTestCase):
    def test_returns_the_rv_library_and_installs_mageml(self):
        libraries = self.prepare()

        self.assertEqual(libraries.rv, self.library)
        self.assertEqual(libraries.mageml.parent, self.cache)
        self.assertTrue(libraries.mageml.name.endswith('-R4.6'))
        self.assertEqual(self.install_args[2], str(self.library))

    def test_mageml_is_installed_once_per_version(self):
        self.prepare()
        runtime._prepared.clear()
        self.prepare()

        self.assertEqual(self.calls.count('install'), 1)

        with patch.object(runtime, 'mageml_hash', return_value='0123456789abcdef'):
            runtime._prepared.clear()
            self.assertEqual(self.prepare().mageml.name, 'mageml-0123456789abcdef-R4.6')
        self.assertEqual(self.calls.count('install'), 2)
        self.assertEqual([p for p in self.cache.iterdir() if p.name.startswith('.')], [])

    def test_a_removed_mageml_library_is_installed_again(self):
        import shutil

        shutil.rmtree(self.prepare().mageml)
        self.prepare()

        self.assertEqual(self.calls.count('install'), 2)

    def test_a_failed_install_shows_what_r_said(self):
        self.install_fails = True

        with self.assertRaisesRegex(runtime.REnvironmentError, 'R said:\nERROR: dependency'):
            self.prepare()
        self.assertEqual([p for p in self.cache.iterdir()], [])

    def test_exchange_packages_must_be_installed(self):
        import shutil

        shutil.rmtree(self.library / 'arrow')

        with self.assertRaisesRegex(runtime.REnvironmentError, '`rv add arrow`'):
            self.prepare()

    def test_r_version_must_match(self):
        self.r_version = '4.5.2'

        with self.assertRaisesRegex(runtime.REnvironmentError, 'is for R 4.6, but /bin/Rscript'):
            self.prepare()

    def test_out_of_sync_library(self):
        self.pending = {'installed': [{'name': 'dplyr'}], 'removed': [{'name': 'old'}]}

        with self.assertRaisesRegex(runtime.REnvironmentError, 'dplyr, old would change'):
            self.prepare()
        self.assertNotIn('sync', self.calls)

    def test_auto_sync(self):
        self.pending = {'installed': [{'name': 'dplyr'}], 'removed': []}

        self.assertEqual(self.prepare(MAGE_R_SYNC='auto').rv, self.library)
        self.assertIn('sync', self.calls)

    def test_sync_off_skips_the_plan(self):
        self.pending = {'installed': [{'name': 'dplyr'}], 'removed': []}

        self.assertEqual(self.prepare(MAGE_R_SYNC='off').rv, self.library)
        self.assertNotIn('plan', self.calls)

    def test_checks_are_cached_until_the_files_change(self):
        self.prepare()
        self.prepare()
        self.assertEqual(self.calls.count('plan'), 1)

        stat = (self.project / 'rv.lock').stat()
        os.utime(self.project / 'rv.lock', ns=(stat.st_atime_ns, stat.st_mtime_ns + 10**9))
        self.prepare()
        self.assertEqual(self.calls.count('plan'), 2)

        (self.library / 'newpackage').mkdir()
        stat = self.library.stat()
        os.utime(self.library, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10**9))
        self.prepare()
        self.assertEqual(self.calls.count('plan'), 3)

    def test_failed_checks_are_not_cached(self):
        self.pending = {'installed': [{'name': 'dplyr'}], 'removed': []}
        with self.assertRaises(runtime.REnvironmentError):
            self.prepare()
        self.pending = {'installed': [], 'removed': []}

        self.assertEqual(self.prepare().rv, self.library)

    def test_missing_library(self):
        import shutil

        shutil.rmtree(self.library)

        with self.assertRaisesRegex(runtime.REnvironmentError, 'does not exist'):
            self.prepare()


class StatusTest(RuntimeTestCase):
    def test_reports_problems(self):
        (self.project / 'rproject.toml').write_text(PROJECT.replace('    "bit64",\n', ''))
        self.pending = {'installed': [{'name': 'bit64'}], 'removed': []}
        self.r_version = '4.5.0'

        with patch.object(runtime, '_run', side_effect=self.fake_run):
            report = runtime.status(runtime.r_config(str(self.repo)))

        self.assertEqual(len(report['problems']), 3)
        self.assertIn('R 4.6, but Rscript runs R 4.5.0', report['problems'][0])
        self.assertIn('`rv add bit64`', report['problems'][1])
        self.assertIn('bit64 would change', report['problems'][2])


class MagemlHashTest(TestCase):
    def test_changes_with_the_source_but_not_the_tests(self):
        import shutil

        source = Path(tempfile.mkdtemp()) / 'mageml'
        shutil.copytree(runtime.MAGEML_DIRECTORY, source)
        with patch.object(runtime, 'MAGEML_DIRECTORY', source):
            before = runtime.mageml_hash()
            (source / 'tests' / 'testthat' / 'test-new.R').write_text('# a test')
            self.assertEqual(runtime.mageml_hash(), before)
            (source / 'R' / 'run.R').write_text('# changed')
            self.assertNotEqual(runtime.mageml_hash(), before)
