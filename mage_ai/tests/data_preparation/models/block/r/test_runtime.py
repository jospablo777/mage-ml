import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
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
            'MAGE_RIG',
        ):
            os.environ.pop(name, None)
        self.cache = Path(tempfile.mkdtemp())
        os.environ['MAGE_R_CACHE_DIR'] = str(self.cache)
        self.which = patch.object(runtime.shutil, 'which', side_effect=lambda name: f'/bin/{name}')
        self.which.start()
        self.calls = []
        self.pending = {'installed': [], 'removed': []}
        self.r_version = '4.6.1'
        self.rig_versions = []
        runtime._rscripts.clear()
        self.install_fails = False
        self.install = patch.object(runtime.subprocess, 'run', side_effect=self.fake_install)
        self.install.start()

    def tearDown(self):
        self.install.stop()
        self.which.stop()
        self.environ.stop()
        super().tearDown()

    def fake_install(self, args, **kwargs):
        if args[0].endswith('rig'):
            return subprocess.CompletedProcess(args, 0, json.dumps(self.rig_versions), '')
        if args[1:3] == ['--vanilla', '-e'] and 'R.version' in args[3]:
            # The version check that chooses Rscript.
            return subprocess.CompletedProcess(args, 0, self.r_version, '')
        assert args[3] == runtime.INSTALL_MAGEML, args
        self.calls.append('install')
        self.install_args = args[4:]
        if self.install_fails:
            return subprocess.CompletedProcess(args, 1, 'ERROR: dependency arrow missing', '')
        staging = Path(args[5])
        (staging / 'mageml').mkdir()
        (staging / 'mageml' / 'DESCRIPTION').write_text('Package: mageml')
        return subprocess.CompletedProcess(args, 0, '', '')

    def fake_run(self, args, cwd=None, timeout=600, env=None):
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


class RigTest(TestCase):
    """The Rscript R blocks run with, chosen from PATH and the R versions of rig."""

    def setUp(self):
        super().setUp()
        runtime._rscripts.clear()
        self.bin = Path(tempfile.mkdtemp())
        self.versions = []
        # The system directories have the commands of the fake executables, and no Rscript.
        path = os.pathsep.join([str(self.bin), '/usr/bin', '/bin'])
        self.environ = patch.dict(os.environ, {'PATH': path}, clear=False)
        self.environ.start()
        for name in ('MAGE_RSCRIPT', 'MAGE_RIG'):
            os.environ.pop(name, None)

    def tearDown(self):
        self.environ.stop()
        runtime._rscripts.clear()
        super().tearDown()

    def executable(self, path: Path, body: str) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f'#!/bin/sh\n{body}\n')
        path.chmod(0o755)
        return path

    def rscript(self, path: Path, version: str) -> Path:
        return self.executable(path, f'printf "{version}"')

    def rig_version(self, version: str, default: bool = False, runs: str = None) -> Path:
        """An R version of rig, whose Rscript runs R runs, or version itself."""
        directory = Path(tempfile.mkdtemp()) / version / 'bin'
        rscript = self.rscript(directory / 'Rscript', runs or version)
        self.executable(directory / 'R', 'exit 0')
        self.versions.append(dict(
            name=version, version=version, default=default, binary=str(directory / 'R'),
        ))
        self.executable(self.bin / 'rig', f"cat <<'EOF'\n{json.dumps(self.versions)}\nEOF")
        return rscript

    def test_the_rscript_on_path_when_it_matches(self):
        self.rscript(self.bin / 'Rscript', '4.6.1')
        self.rig_version('4.6.0')

        self.assertEqual(runtime.choose_rscript('4.6'), 'Rscript')

    def test_the_newest_matching_version_of_rig(self):
        self.rscript(self.bin / 'Rscript', '4.5.3')
        self.rig_version('4.6.0')
        newest = self.rig_version('4.6.1')
        self.rig_version('4.4.2')

        self.assertEqual(runtime.choose_rscript('4.6'), str(newest))
        self.assertTrue(runtime.has_r_version('4.6'))

    def test_mage_rscript_wins(self):
        self.rig_version('4.6.1')
        os.environ['MAGE_RSCRIPT'] = '/opt/R/bin/Rscript'

        self.assertEqual(runtime.choose_rscript('4.6'), '/opt/R/bin/Rscript')

    def test_without_rig_or_a_matching_version(self):
        self.rscript(self.bin / 'Rscript', '4.5.3')

        self.assertEqual(runtime.choose_rscript('4.6'), 'Rscript')
        self.assertFalse(runtime.has_r_version('4.6'))
        self.assertIn('rig add 4.6', runtime._r_version_hint('4.6'))
        with self.assertRaisesRegex(runtime.REnvironmentError, 'needs rig'):
            runtime.install_r('4.6')

    def test_rv_and_rscript_run_with_the_chosen_r_first_on_path(self):
        self.rscript(self.bin / 'Rscript', '4.5.3')
        chosen = self.rig_version('4.6.1')
        config = runtime.RConfig(
            project_dir=None, rscript=runtime.choose_rscript('4.6'), rv='rv',
        )

        env = runtime.tool_env(config, {'PATH': '/usr/bin'})

        self.assertEqual(env['PATH'].split(os.pathsep)[0], str(chosen.parent))

    def test_arrow_builds_with_s3_unless_the_user_sets_otherwise(self):
        config = runtime.RConfig(project_dir=None, rscript='Rscript', rv='rv')

        self.assertEqual(runtime.tool_env(config, {})['LIBARROW_MINIMAL'], 'false')
        self.assertEqual(
            runtime.tool_env(config, {'LIBARROW_MINIMAL': 'true'})['LIBARROW_MINIMAL'], 'true',
        )

    def test_a_failing_rig_counts_as_none(self):
        self.executable(self.bin / 'rig', 'echo broken; exit 1')

        self.assertEqual(runtime.rig_versions(), [])

    def test_a_version_that_runs_only_as_the_default_is_not_used(self):
        """On macOS, every framework Rscript runs the default R version."""
        self.rscript(self.bin / 'Rscript', '4.6.1')
        self.rig_version('4.5.3', runs='4.6.1')

        self.assertEqual(runtime.choose_rscript('4.5'), 'Rscript')
        self.assertFalse(runtime.has_r_version('4.5'))
        self.assertIn('rig default 4.5', runtime._r_version_hint('4.5'))

    def test_setup_steps_when_everything_is_installed(self):
        self.rscript(self.bin / 'Rscript', '4.6.1')
        self.rig_version('4.6.1')
        self.executable(self.bin / 'rv', 'exit 0')

        steps = runtime.setup_steps('4.6')

        self.assertTrue(all(step['found'] for step in steps), steps)

    def test_setup_steps_give_the_commands_of_the_platform(self):
        with patch.object(runtime.sys, 'platform', 'linux'):
            steps = {step['name'].split(',')[0]: step for step in runtime.setup_steps('4.6')}

        self.assertIsNone(steps['rv']['found'])
        self.assertIn('install.sh', steps['rv']['commands'][0])
        # Without rig, installing R starts with installing rig.
        self.assertIn('rig-linux', steps['R 4.6']['commands'][0])
        self.assertEqual(steps['R 4.6']['commands'][-1], 'rig add 4.6')

        with patch.object(runtime.sys, 'platform', 'darwin'):
            steps = {step['name'].split(',')[0]: step for step in runtime.setup_steps('4.6')}
        self.assertEqual(steps['rv']['commands'], ['brew install rv-r'])
        self.assertEqual(steps['rig']['commands'], ['brew install r-rig'])

    def test_setup_suggests_rig_default_for_a_version_that_runs_only_as_default(self):
        self.rscript(self.bin / 'Rscript', '4.6.1')
        self.rig_version('4.5.3', runs='4.6.1')

        r_step = runtime.setup_steps('4.5')[1]

        self.assertIsNone(r_step['found'])
        self.assertEqual(r_step['commands'], ['rig default 4.5'])

    def test_setup_lists_the_missing_build_libraries_on_debian_and_ubuntu(self):
        # dpkg-query prints the packages it knows; cmake is half removed.
        self.executable(self.bin / 'dpkg-query', '\n'.join([
            "echo 'libuv1-dev:arm64 installed'",
            "echo 'cmake config-files'",
            *[f"echo '{p} installed'" for p in runtime.LINUX_BUILD_LIBRARIES[4:]],
        ]))

        with patch.object(runtime.sys, 'platform', 'linux'):
            step = runtime.setup_steps('4.6')[-1]

        self.assertIsNone(step['found'])
        self.assertEqual(step['commands'], [
            'sudo apt-get install -y cmake libcurl4-openssl-dev libfontconfig1-dev '
            'libfreetype6-dev',
        ])

    def test_setup_has_no_library_step_without_dpkg(self):
        with patch.object(runtime.sys, 'platform', 'linux'):
            names = [step['name'] for step in runtime.setup_steps('4.6')]

        self.assertFalse(any('libraries' in name for name in names))


# Writes its pid, prints a line and computes for a minute.
SLEEPING_SCRIPT = """
args <- commandArgs(trailingOnly = TRUE)
writeLines(as.character(Sys.getpid()), paste0(args[1], ".tmp"))
file.rename(paste0(args[1], ".tmp"), args[1])
cat("started\\n")
Sys.sleep(60)
"""


def _alive(pid: int) -> bool:
    import psutil

    try:
        return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def _wait_for(condition, timeout: float = 30) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.1)
    return False


@unittest.skipUnless(shutil.which('Rscript'), 'needs Rscript')
class RunRscriptTest(TestCase):
    def setUp(self):
        super().setUp()
        self.directory = Path(tempfile.mkdtemp())
        self.script = self.directory / 'script.R'
        self.script.write_text(SLEEPING_SCRIPT)
        self.pid_file = self.directory / 'pid'
        self.config = runtime.RConfig(project_dir=None, rscript='Rscript', rv='rv')

    def r_pid(self) -> int:
        self.assertTrue(_wait_for(self.pid_file.exists), 'R did not start')
        return int(self.pid_file.read_text())

    def test_output_and_exit_code(self):
        script = self.directory / 'fails.R'
        script.write_text('cat("one\\ntwo\\n"); quit(status = 4)')
        output = io.StringIO()
        with patch.object(runtime.sys, 'stdout', output):
            code = runtime.run_rscript(self.config, script, [], env={}, cwd=self.directory)
        self.assertEqual((code, output.getvalue()), (4, 'one\ntwo\n'))

    def test_a_timeout_stops_r(self):
        config = runtime.RConfig(project_dir=None, rscript='Rscript', rv='rv', timeout=3)
        with self.assertRaisesRegex(TimeoutError, 'longer than 3 seconds'):
            runtime.run_rscript(
                config, self.script, [str(self.pid_file)], env={}, cwd=self.directory,
            )
        self.assertTrue(_wait_for(lambda: not _alive(int(self.pid_file.read_text())), 10))

    def test_an_interrupt_stops_r(self):
        """Interrupting an R block in the notebook left R computing."""
        class Interrupting(io.StringIO):
            def write(self, text):
                raise KeyboardInterrupt

        with patch.object(runtime.sys, 'stdout', Interrupting()):
            with self.assertRaises(KeyboardInterrupt):
                runtime.run_rscript(
                    self.config, self.script, [str(self.pid_file)], env={}, cwd=self.directory,
                )
        self.assertTrue(_wait_for(lambda: not _alive(self.r_pid()), 10))

    def test_r_stops_when_the_process_running_the_block_is_killed(self):
        """A killed kernel or worker left its R block running."""
        code = (
            'import sys\n'
            'from pathlib import Path\n'
            'from mage_ai.data_preparation.models.block.r import runtime\n'
            "config = runtime.RConfig(project_dir=None, rscript='Rscript', rv='rv')\n"
            f'runtime.run_rscript(config, Path({str(self.script)!r}), '
            f'[{str(self.pid_file)!r}], env={{}}, cwd=Path({str(self.directory)!r}))\n'
        )
        process = subprocess.Popen([sys.executable, '-c', code], stdout=subprocess.DEVNULL)
        pid = self.r_pid()
        self.assertTrue(_alive(pid))

        process.kill()
        process.wait()

        self.assertTrue(_wait_for(lambda: not _alive(pid), 10), 'R is still running')
