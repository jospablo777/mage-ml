"""
Pipeline environments: a pipeline's Python blocks run in a virtual environment with the
packages its requirements file lists. Builds with uv, from its cache when it has the
packages; skipped where uv is not installed.
"""
import asyncio
import shutil
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import psutil

from mage_ai.data_preparation import environments
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.tests.base_test import DBTestCase

LOAD = '''
import sys

import pandas as pd
import six

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(**kwargs):
    print('loading with six', six.__version__)
    return pd.DataFrame({
        'id': pd.Series([1, 2, None], dtype='Int64'),
        'six': six.__version__,
        'python': sys.executable,
        'rows': kwargs.get('rows'),
    })


@test
def has_rows(output, *args):
    assert len(output) == 3
'''

TRANSFORM = '''
@transformer
def transform(frame, *args, **kwargs):
    frame['doubled'] = frame['id'] * 2
    return frame
'''


HANGING_BUILD = '''
import subprocess, sys, time
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
open(sys.argv[1], 'w').write(str(child.pid))
print('resolving', flush=True)
time.sleep(60)
'''


class BuildCommandTest(unittest.TestCase):
    def test_a_build_command_that_stalls_is_stopped_with_its_children(self):
        """
        The timeout started only once the command closed its output, so a uv install that
        stalled on the network kept the run waiting; a timeout left uv's children running.
        """
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory, True)
        pid_file = Path(directory, 'child.pid')
        started = time.monotonic()
        with patch.object(environments, 'BUILD_TIMEOUT_SECONDS', 2):
            with self.assertRaisesRegex(
                environments.PipelineEnvironmentError, 'longer than 2 seconds',
            ):
                environments._run(
                    [sys.executable, '-c', HANGING_BUILD, str(pid_file)], log=lambda _line: None,
                )
        self.assertLess(time.monotonic() - started, 30)

        child = int(pid_file.read_text())
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                if psutil.Process(child).status() == psutil.STATUS_ZOMBIE:
                    break
            except psutil.NoSuchProcess:
                break
            time.sleep(0.1)
        else:
            psutil.Process(child).kill()
            self.fail('The build command\'s child process kept running.')


@unittest.skipIf(shutil.which('uv') is None, 'uv is not installed')
class PipelineEnvironmentTest(DBTestCase):
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

    def pipeline_with_environment(self):
        pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        # Mage has six 1.17.0.
        Path(pipeline.dir_path, 'requirements.txt').write_text('six==1.16.0\n')
        pipeline.environment = dict(requirements='requirements.txt')
        pipeline.save()
        load = Block.create(
            f'{pipeline.uuid}_load', 'data_loader', self.repo_path, pipeline=pipeline,
        )
        Path(load.file_path).write_text(textwrap.dedent(LOAD))
        transform = Block.create(
            f'{pipeline.uuid}_transform', 'transformer', self.repo_path, pipeline=pipeline,
            upstream_block_uuids=[load.uuid],
        )
        Path(transform.file_path).write_text(textwrap.dedent(TRANSFORM))
        return Pipeline.get(pipeline.uuid, repo_path=self.repo_path), load.uuid, transform.uuid

    def test_blocks_run_with_the_environments_packages_and_pass_frames(self):
        pipeline, load, transform = self.pipeline_with_environment()
        environment = environments.pipeline_environment(pipeline)
        self.assertIn('six==1.16.0', environment.requirements)
        self.assertTrue(any(p.startswith('pandas==') for p in environment.pins))

        pipeline.get_block(load).execute_sync(global_vars=dict(rows=3))
        pipeline.get_block(transform).execute_sync()

        manager = pipeline.variable_manager
        frame = manager.get_variable(pipeline.uuid, transform, 'output_0')
        self.assertIsInstance(frame, pd.DataFrame)
        self.assertEqual(frame['six'].tolist(), ['1.16.0'] * 3)
        self.assertTrue(frame['python'][0].startswith(self.cache))
        self.assertEqual(str(frame['id'].dtype), 'Int64')
        self.assertEqual(frame['doubled'].tolist()[:2], [2, 4])
        self.assertEqual(frame['rows'].tolist(), [3, 3, 3])
        self.assertTrue((environment.directory / environments.MARKER).is_file())

    def test_a_block_runs_from_the_engines_job_worker(self):
        """The job worker's MAGE_WORKER_REQUEST_FD names its own descriptor, not ours."""
        pipeline, load, _ = self.pipeline_with_environment()
        with patch.dict('os.environ', {'MAGE_WORKER_REQUEST_FD': '4'}):
            pipeline.get_block(load).execute_sync(global_vars=dict(rows=3))
        frame = pipeline.variable_manager.get_variable(pipeline.uuid, load, 'output_0')
        self.assertEqual(frame['six'].tolist(), ['1.16.0'] * 3)

    def test_the_environment_is_built_once_and_reused(self):
        pipeline, _, _ = self.pipeline_with_environment()
        environment = environments.pipeline_environment(pipeline)
        first = environments.ensure(environment)
        with patch.object(environments, '_build') as build:
            self.assertEqual(environments.ensure(environment), first)
        build.assert_not_called()

    def test_a_failing_block_reports_its_error_and_traceback(self):
        pipeline, load, _ = self.pipeline_with_environment()
        block = pipeline.get_block(load)
        Path(block.file_path).write_text(textwrap.dedent('''
            @data_loader
            def load(**kwargs):
                raise ValueError('nothing to load')
        '''))
        block = Pipeline.get(pipeline.uuid, repo_path=self.repo_path).get_block(load)
        with self.assertRaises(Exception) as caught:
            block.execute_sync()
        self.assertIn('nothing to load', str(caught.exception))

    def test_a_missing_requirements_file_is_explained(self):
        pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        pipeline.environment = dict(requirements='missing.txt')
        pipeline.save()
        with self.assertRaises(environments.PipelineEnvironmentError) as caught:
            environments.pipeline_environment(
                Pipeline.get(pipeline.uuid, repo_path=self.repo_path),
            )
        self.assertIn('missing.txt', str(caught.exception))

    def test_a_triggered_run_completes_with_its_blocks_in_the_environment(self):
        from mage_ai.orchestration import pipeline_scheduler_original
        from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun
        from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
        from mage_ai.orchestration.triggers.api import trigger_pipeline

        pipeline, load, transform = self.pipeline_with_environment()
        run = trigger_pipeline(pipeline.uuid, variables=dict(rows=3))
        job_manager = pipeline_scheduler_original.get_job_manager()
        pending = []

        def add_job(job_type, uid, target, *args, **kwargs):
            pending.append((target, args, kwargs))

        with patch.object(job_manager, 'add_job', side_effect=add_job), \
                patch.object(job_manager, 'has_block_run_job', return_value=True):
            for _ in range(10):
                run.refresh()
                scheduler = PipelineScheduler(run)
                if run.status == PipelineRun.PipelineRunStatus.INITIAL:
                    scheduler.start(should_schedule=False)
                scheduler.schedule()
                run.refresh()
                if run.status not in (
                    PipelineRun.PipelineRunStatus.INITIAL, PipelineRun.PipelineRunStatus.RUNNING,
                ):
                    break
                while pending:
                    target, args, kwargs = pending.pop(0)
                    target(*args, **kwargs)

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        statuses = {b.block_uuid: b.status for b in BlockRun.query.filter(
            BlockRun.pipeline_run_id == run.id,
        )}
        self.assertEqual(set(statuses.values()), {BlockRun.BlockRunStatus.COMPLETED})
        frame = pipeline.variable_manager.get_variable(
            pipeline.uuid, transform, 'output_0', partition=run.execution_partition,
        )
        self.assertEqual(frame['six'].tolist(), ['1.16.0'] * 3)

    def test_an_export_pins_the_environments_packages_and_python(self):
        from mage_ai.pipeline_services import export

        pipeline, _, _ = self.pipeline_with_environment()
        captured = export.capture(self.repo_path, [pipeline.uuid])

        self.assertEqual(captured.requirements['six'], '1.16.0')
        self.assertIn('pandas', captured.requirements)
        self.assertTrue(any('pipeline environment' in note for note in captured.notes))

    def test_an_export_refuses_pipelines_in_different_environments(self):
        from mage_ai.pipeline_services import export

        pipeline, _, _ = self.pipeline_with_environment()
        plain = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        Block.create(f'{plain.uuid}_load', 'data_loader', self.repo_path, pipeline=plain)

        with self.assertRaises(export.ExportError) as caught:
            export.capture(self.repo_path, [pipeline.uuid, plain.uuid])

        problems = '\n'.join(caught.exception.problems)
        self.assertIn('different Python environments', problems)
        self.assertIn(f"{plain.uuid} (Mage's packages)", problems)

    def test_a_dynamic_block_is_refused_with_the_reason(self):
        pipeline, load, _ = self.pipeline_with_environment()
        block = pipeline.get_block(load)
        block.configuration = dict(dynamic=True)

        with self.assertRaises(environments.PipelineEnvironmentError) as caught:
            block.execute_sync(global_vars=dict(rows=3))

        self.assertIn('dynamic or replicated', str(caught.exception))

    def test_the_settings_page_saves_an_environment_and_clears_an_empty_one(self):
        pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)

        asyncio.run(pipeline.update(dict(
            environment=dict(requirements=' requirements.txt ', python=''),
        )))
        reloaded = Pipeline.get(pipeline.uuid, repo_path=self.repo_path)
        self.assertEqual(reloaded.environment, dict(requirements='requirements.txt'))

        asyncio.run(reloaded.update(dict(environment=dict(requirements='', python=''))))
        reloaded = Pipeline.get(pipeline.uuid, repo_path=self.repo_path)
        self.assertIsNone(reloaded.environment)
        self.assertNotIn('environment', Path(reloaded.config_path).read_text())


WORKER_LOAD = '''
import sys

import pandas as pd
import six


@data_loader
def load(**kwargs):
    return pd.DataFrame({'six': [six.__version__], 'python': [sys.executable]})
'''

WORKER_TRANSFORM = '''
import sys

import six


@transformer
def transform(frame, *args, **kwargs):
    frame['next_six'] = six.__version__
    frame['next_python'] = sys.executable
    return frame
'''


@unittest.skipUnless(shutil.which('uv'), 'needs uv')
class NamedWorkerTest(DBTestCase):
    """
    Python workers: uv projects in python_workers/<name>/ that blocks select. A worker is
    prepared from its lock before runs use it; frames cross between workers as Arrow.
    """

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

    def worker(self, name='legacy', dependencies=None, lock=True):
        import importlib.metadata
        import subprocess

        folder = Path(self.repo_path, environments.WORKERS_DIR, name)
        folder.mkdir(parents=True, exist_ok=True)
        self.addCleanup(shutil.rmtree, folder, True)
        # Mage has six 1.17.0; the frame packages at Mage's versions, from uv's cache.
        dependencies = dependencies or [
            'six==1.16.0',
            f'pandas=={importlib.metadata.version("pandas")}',
            f'pyarrow=={importlib.metadata.version("pyarrow")}',
        ]
        folder.joinpath('pyproject.toml').write_text(textwrap.dedent(f'''
            [project]
            name = "{name}"
            version = "0"
            requires-python = ">=3.12"
            dependencies = {dependencies!r}
        ''').replace("'", '"'))
        if lock:
            subprocess.run(['uv', 'lock', '--project', str(folder)], check=True,
                           capture_output=True)
        return folder

    def pipeline(self, environment=None, load_worker=None, transform_worker=None):
        pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        if environment:
            pipeline.environment = environment
            pipeline.save()
        load = Block.create(
            f'{pipeline.uuid}_load', 'data_loader', self.repo_path, pipeline=pipeline,
            configuration=dict(python_worker=load_worker) if load_worker else None,
        )
        Path(load.file_path).write_text(textwrap.dedent(WORKER_LOAD))
        transform = Block.create(
            f'{pipeline.uuid}_transform', 'transformer', self.repo_path, pipeline=pipeline,
            upstream_block_uuids=[load.uuid],
            configuration=dict(python_worker=transform_worker) if transform_worker else None,
        )
        Path(transform.file_path).write_text(textwrap.dedent(WORKER_TRANSFORM))
        return Pipeline.get(pipeline.uuid, repo_path=self.repo_path), load.uuid, transform.uuid

    def test_a_block_runs_in_its_prepared_worker_and_passes_a_frame_to_mages_python(self):
        self.worker()
        pipeline, load, transform = self.pipeline(load_worker='Legacy')
        [worker] = environments.named_workers(self.repo_path).values()
        self.assertEqual((worker.alias, worker.prepared), ('legacy', False))
        with self.assertRaisesRegex(Exception, 'not prepared.*mage workers prepare legacy'):
            pipeline.get_block(load).execute_sync()

        environments.prepare(worker, log=lambda line: None)
        pipeline.get_block(load).execute_sync()
        pipeline.get_block(transform).execute_sync()

        frame = pipeline.variable_manager.get_variable(pipeline.uuid, transform, 'output_0')
        self.assertEqual(frame['six'].tolist(), ['1.16.0'])
        self.assertTrue(frame['python'][0].startswith(self.cache))
        # The transformer selects no worker: MageML's own Python and six.
        self.assertEqual(frame['next_six'].tolist(), ['1.17.0'])
        self.assertEqual(frame['next_python'].tolist(), [sys.executable])

    def test_the_pipeline_selects_a_worker_and_a_block_can_choose_mages_python(self):
        environments.prepare(
            environments.named_workers(str(self.worker().parent.parent))['legacy'],
            log=lambda line: None,
        )
        pipeline, load, transform = self.pipeline(
            environment=dict(worker='legacy'), transform_worker='mage',
        )
        pipeline.get_block(load).execute_sync()
        pipeline.get_block(transform).execute_sync()
        frame = pipeline.variable_manager.get_variable(pipeline.uuid, transform, 'output_0')
        self.assertEqual((frame['six'][0], frame['next_six'][0]), ('1.16.0', '1.17.0'))

    def test_an_unknown_worker_names_the_projects_workers(self):
        self.worker()
        pipeline, load, _ = self.pipeline(load_worker='faest')
        with self.assertRaisesRegex(
            Exception, 'selects Python worker "faest", which does not exist.*legacy',
        ):
            pipeline.get_block(load).execute_sync()

    def test_a_lock_that_does_not_match_its_project_says_to_lock_again(self):
        folder = self.worker(name='stale')
        pyproject = folder / 'pyproject.toml'
        pyproject.write_text(pyproject.read_text().replace('"six==1.16.0"', '"six==1.15.0"'))
        worker = environments.named_workers(self.repo_path)['stale']
        with self.assertRaisesRegex(
            environments.PipelineEnvironmentError,
            'uv.lock does not match pyproject.toml. Run "uv lock" in python_workers/stale',
        ):
            environments.prepare(worker, log=lambda line: None)
        self.assertFalse(worker.prepared)

    def test_a_worker_without_a_lock_is_refused(self):
        self.worker(name='unlocked', lock=False)
        worker = environments.named_workers(self.repo_path)['unlocked']
        with self.assertRaisesRegex(environments.PipelineEnvironmentError, 'has no uv.lock'):
            environments.prepare(worker, log=lambda line: None)

    def test_an_export_takes_the_worker_its_blocks_share_and_refuses_a_mixture(self):
        from mage_ai.pipeline_services import export

        self.worker()
        shared, _, _ = self.pipeline(environment=dict(worker='legacy'))
        captured = export.capture(self.repo_path, [shared.uuid])
        self.assertEqual(captured.requirements['six'], '1.16.0')

        mixed, load, transform = self.pipeline(load_worker='legacy')
        with self.assertRaises(export.ExportError) as caught:
            export.capture(self.repo_path, [mixed.uuid])
        problems = '\n'.join(caught.exception.problems)
        self.assertIn('run in different Python workers', problems)
        self.assertIn(f'{load} (legacy), {transform} (MageML)', problems)


class WorkersCommandTest(unittest.TestCase):
    """mage workers list and prepare."""

    def test_the_list_says_which_workers_are_prepared(self):
        from typer.testing import CliRunner

        from mage_ai.cli.main import app

        project = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, project, True)
        runner = CliRunner()
        result = runner.invoke(app, ['workers', 'list', '--project', str(project)])
        self.assertIn('No Python workers', result.output)
        folder = project / environments.WORKERS_DIR / 'Feast_Store'
        folder.mkdir(parents=True)
        (folder / 'pyproject.toml').write_text('[project]\nname = "Feast_Store"\n')
        with patch.dict('os.environ', {environments.ENVIRONMENTS_DIR_ENV: str(project / 'envs')}):
            result = runner.invoke(app, ['workers', 'list', '--project', str(project)])
            self.assertIn('feast-store', result.output)
            self.assertIn('not prepared', result.output)
            result = runner.invoke(app, ['workers', 'prepare', 'other', '--project', str(project)])
        self.assertEqual(result.exit_code, 1)
        self.assertIn('There is no Python worker other; workers: feast-store.', result.output)

    def test_two_workers_with_one_name_are_refused(self):
        project = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, project, True)
        for folder, name in (('a', 'Feast_Store'), ('b', 'feast-store')):
            path = project / environments.WORKERS_DIR / folder
            path.mkdir(parents=True)
            (path / 'pyproject.toml').write_text(f'[project]\nname = "{name}"\n')
        with self.assertRaisesRegex(
            environments.PipelineEnvironmentError, 'both named feast-store',
        ):
            environments.named_workers(str(project))


class RunRecordWorkersTest(DBTestCase):
    """A run's record names the worker each block selects and how it was prepared."""

    def test_the_record_holds_each_blocks_worker(self):
        from mage_ai.orchestration import run_records

        folder = Path(self.repo_path, environments.WORKERS_DIR, 'feast')
        folder.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, folder, True)
        (folder / 'pyproject.toml').write_text('[project]\nname = "feast"\n')
        pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        Block.create(f'{pipeline.uuid}_features', 'data_loader', self.repo_path,
                     pipeline=pipeline, configuration=dict(python_worker='feast'))
        Block.create(f'{pipeline.uuid}_plain', 'data_loader', self.repo_path, pipeline=pipeline)
        pipeline = Pipeline.get(pipeline.uuid, repo_path=self.repo_path)

        record = run_records.environment(pipeline)['python_workers']

        self.assertEqual(list(record), [f'{pipeline.uuid}_features'])
        entry = record[f'{pipeline.uuid}_features']
        self.assertEqual(
            (entry['worker'], entry['definition'], entry['prepared']),
            ('feast', 'python_workers/feast', False),
        )


class PythonWorkerApiTest(DBTestCase):
    """GET /api/python_workers and POST to prepare one, as the Settings page uses them."""

    def setUp(self):
        super().setUp()
        from mage_ai.tests.factory import create_user

        self.owner = create_user(_owner=True)
        folder = Path(self.repo_path, environments.WORKERS_DIR, 'feast')
        folder.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, folder, True)
        (folder / 'pyproject.toml').write_text(
            '[project]\nname = "feast"\ndependencies = ["feast[redis]>=0.50"]\n'
        )

    def call(self, action, pk=None, payload=None):
        from mage_ai.api.operations.base import BaseOperation

        return asyncio.run(BaseOperation(
            action=action, pk=pk, payload=payload or {}, resource='python_workers',
            user=self.owner,
        ).execute())

    def test_workers_are_listed_and_a_prepare_reports_its_lines_and_outcome(self):
        from mage_ai.api.operations.constants import OperationType

        [worker] = self.call(OperationType.LIST)['python_workers']
        self.assertEqual(
            (worker['name'], worker['dependencies'], worker['locked'], worker['prepared']),
            ('feast', ['feast[redis]>=0.50'], False, False),
        )

        def fake_prepare(worker, log=print):
            log('Resolved 3 packages')
            raise environments.PipelineEnvironmentError('the lock is missing')

        with patch.object(environments, 'prepare', side_effect=fake_prepare):
            started = self.call(OperationType.CREATE,
                                payload=dict(python_worker=dict(name='Feast')))
            job = started['python_worker']['job']
            for _ in range(100):
                if job['state'] != 'running':
                    break
                time.sleep(0.05)
                job = self.call(OperationType.DETAIL, pk='feast')['python_worker']['job']

        self.assertEqual(job['state'], 'failed')
        self.assertEqual(job['lines'], ['Resolved 3 packages'])
        self.assertEqual(job['problems'], ['the lock is missing'])

        missing = self.call(OperationType.DETAIL, pk='other')
        self.assertEqual(missing['error']['code'], 404)
        self.assertIn('workers: feast', missing['error']['message'])
