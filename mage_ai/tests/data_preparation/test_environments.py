"""
Pipeline environments: a pipeline's Python blocks run in a virtual environment with the
packages its requirements file lists. Builds with uv, from its cache when it has the
packages; skipped where uv is not installed.
"""
import asyncio
import importlib.metadata
import json
import shutil
import subprocess
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

    def test_the_settings_table_sets_and_clears_where_a_block_runs(self):
        # What the pipeline settings' Save sends for the blocks whose worker changed.
        pipeline, load, transform = self.pipeline(
            environment=dict(worker='feast'), load_worker='mlflow',
        )

        def update(block, worker):
            configuration = dict(pipeline.get_block(block).configuration or {})
            asyncio.run(pipeline.update(dict(blocks=[dict(
                configuration={**configuration, 'python_worker': worker}, uuid=block,
            )]), update_content=True))
            saved = Pipeline.get(pipeline.uuid, repo_path=self.repo_path)
            # Only that block is sent: the other one stays, in its place.
            self.assertEqual(list(saved.blocks_by_uuid), [load, transform])
            return environments.worker_selection(saved.get_block(block))

        self.assertEqual(update(transform, 'mage'), 'mage')
        # Cleared with null: the configuration is not empty, so it is saved, and the block
        # runs in the pipeline's default.
        self.assertEqual(update(load, None), 'feast')
        self.assertEqual(update(transform, None), 'feast')

    def test_a_worker_has_the_name_and_description_people_give_it(self):
        folder = self.worker('feast', lock=False)
        before = environments.named_workers(self.repo_path)['feast']
        self.assertEqual((before.label, before.display_name), (None, 'feast'))
        pyproject = folder / 'pyproject.toml'
        pyproject.write_text(pyproject.read_text() + textwrap.dedent('''
            [tool.mageml.worker]
            label = "Feast worker"
            description = "Feature retrieval with Feast; pandas 2"
            combine = false
        '''))
        worker = environments.named_workers(self.repo_path)['feast']
        self.assertEqual(
            (worker.label, worker.description, worker.combine, worker.display_name),
            ('Feast worker', 'Feature retrieval with Feast; pandas 2', False, 'Feast worker'),
        )
        # Naming it is not a change of packages: the prepared environment stays valid.
        self.assertEqual(worker.digest, before.digest)
        [described] = environments.describe_workers(self.repo_path)
        self.assertEqual((described['name'], described['label']), ('feast', 'Feast worker'))

    def test_several_blocks_get_a_worker_at_once_and_nothing_changes_on_a_bad_one(self):
        self.worker('feast', lock=False)
        pipeline, load, transform = self.pipeline()
        changed = environments.assign_worker(pipeline, [load, transform], 'Feast')
        self.assertEqual(changed, [load, transform])
        pipeline = Pipeline.get(pipeline.uuid, repo_path=self.repo_path)
        self.assertEqual(
            [pipeline.get_block(u).configuration.get('python_worker') for u in (load, transform)],
            ['feast', 'feast'],
        )
        # Already there: nothing to change.
        self.assertEqual(environments.assign_worker(pipeline, [load], 'feast'), [])
        with self.assertRaisesRegex(environments.PipelineEnvironmentError, 'no Python worker'):
            environments.assign_worker(pipeline, [load], 'spark')
        with self.assertRaisesRegex(environments.PipelineEnvironmentError, 'has no block'):
            environments.assign_worker(pipeline, [load, 'missing'], None)
        self.assertEqual(
            pipeline.get_block(load).configuration.get('python_worker'), 'feast',
            'a bad block in the list changes none of them',
        )
        self.assertEqual(environments.assign_worker(pipeline, [load], None), [load])
        self.assertEqual(environments.assign_worker(pipeline, [transform], 'mage'), [transform])
        pipeline = Pipeline.get(pipeline.uuid, repo_path=self.repo_path)
        self.assertNotIn('python_worker', pipeline.get_block(load).configuration or {})
        self.assertEqual(pipeline.get_block(transform).configuration['python_worker'], 'mage')

    def test_workers_combine_with_the_same_versions_with_changes_or_not_at_all(self):
        environments._combine_cache.clear()
        self.worker('old-six', dependencies=['six==1.16.0'])
        # Not named packaging: a project cannot depend on itself.
        self.worker('pack-tools', dependencies=['packaging==24.1', 'six==1.16.0'])
        self.worker('any-six', dependencies=['six>=1.16'])
        self.worker('new-six', dependencies=['six==1.17.0'])

        same = environments.combine_check(self.repo_path, ['old-six', 'pack-tools'])
        self.assertEqual(same['state'], 'same')
        # six, which both install, once.
        self.assertGreater(same['saved_bytes'], 0)
        self.assertEqual(same['saved_bytes'], same['sizes']['old-six'])
        self.assertEqual(same['combined_size'] + same['saved_bytes'], sum(same['sizes'].values()))

        changes = environments.combine_check(self.repo_path, ['old-six', 'any-six'])
        self.assertEqual(changes['state'], 'changes')
        self.assertEqual(changes['changes']['any-six'],
                         [dict(package='six', **{'from': '1.17.0', 'to': '1.16.0'})])
        self.assertEqual(changes['changes']['old-six'], [])

        conflict = environments.combine_check(self.repo_path, ['old-six', 'new-six'])
        self.assertEqual(conflict['state'], 'conflict')
        self.assertIn('six==1.16.0', conflict['reason'])
        self.assertIn('unsatisfiable', conflict['reason'])

        # Answers are kept by the locks: the same question again runs no resolution.
        with patch.object(environments.subprocess, 'run', side_effect=AssertionError):
            self.assertEqual(
                environments.combine_check(self.repo_path, ['pack-tools', 'old-six'])['state'],
                'same',
            )

    def test_a_check_says_same_when_no_version_moves_and_unchecked_without_a_lock(self):
        environments._combine_cache.clear()
        self.worker('old-six', dependencies=['six==1.16.0'])
        self.worker('pack-tools', dependencies=['packaging==24.1', 'six==1.16.0'])
        real = subprocess.run

        def pinned_fails(command, **kwargs):
            # CI (cda7afdcc): the resolution at the locked versions failed and the free one
            # moved nothing; the page said "can run as one worker if 0 packages change".
            if 'compile' in command and '-c' in command:
                return subprocess.CompletedProcess(command, 2, '', 'error: no reason')
            return real(command, **kwargs)

        with patch.object(environments.subprocess, 'run', side_effect=pinned_fails):
            result = environments.combine_check(self.repo_path, ['old-six', 'pack-tools'])
        self.assertEqual(result['state'], 'same')
        self.assertEqual(result['changes'], {'old-six': [], 'pack-tools': []})
        self.assertGreater(result['saved_bytes'], 0)

        environments._combine_cache.clear()

        def export_fails(command, **kwargs):
            if 'export' in command:
                return subprocess.CompletedProcess(
                    command, 2, '', 'warning: x\nerror: The lockfile is not valid\n')
            return real(command, **kwargs)

        with patch.object(environments.subprocess, 'run', side_effect=export_fails):
            result = environments.combine_check(self.repo_path, ['old-six', 'pack-tools'])
        self.assertEqual(result['state'], 'unchecked')
        self.assertEqual(result['reason'], 'uv could not read the uv.lock of old-six: '
                                           'error: The lockfile is not valid')
        # Not kept: the next check runs uv again.
        self.assertFalse(environments._combine_cache)

    def test_the_api_answers_whether_workers_combine_for_editors(self):
        from mage_ai.api.operations.base import BaseOperation
        from mage_ai.api.operations.constants import OperationType
        from mage_ai.tests.factory import create_user

        environments._combine_cache.clear()
        self.worker('old-six', dependencies=['six==1.16.0'])
        self.worker('new-six', dependencies=['six==1.17.0'])

        def ask(user, workers):
            return asyncio.run(BaseOperation(
                action=OperationType.CREATE, payload=dict(python_worker_combination=dict(
                    workers=workers)),
                resource='python_worker_combinations', user=user,
            ).execute())

        from mage_ai.orchestration.db.models.oauth import Role

        Role.create_default_roles()
        editor = create_user(roles_new=[Role.get_role('Editor')])
        result = ask(editor, ['old-six', 'new-six'])
        self.assertIn('python_worker_combination', result, result)
        answer = result['python_worker_combination']
        self.assertEqual((answer['id'], answer['state']), ('old-six+new-six', 'conflict'))
        self.assertEqual(ask(editor, ['old-six'])['error']['code'], 400)
        viewer = create_user(roles_new=[Role.get_role('Viewer')])
        self.assertEqual(ask(viewer, ['old-six', 'new-six'])['error']['code'], 403)

    def test_combining_writes_one_worker_and_moves_the_blocks_to_it(self):
        self.worker('feast', dependencies=['six==1.16.0'], lock=False)
        self.worker('mlflow', dependencies=['packaging==24.1'], lock=False)
        pipeline, load, transform = self.pipeline(
            environment=dict(worker='feast'), transform_worker='mlflow',
        )
        folder, moved = environments.combine_into(pipeline, ['feast', 'mlflow'], 'features')
        self.assertEqual(sorted(moved), sorted([load, transform]))
        pipeline = Pipeline.get(pipeline.uuid, repo_path=self.repo_path)
        self.assertEqual(pipeline.environment['worker'], 'features')
        self.assertEqual(pipeline.get_block(transform).configuration['python_worker'],
                         'features')
        # load inherits the pipeline's worker, so it has none of its own.
        self.assertNotIn('python_worker', pipeline.get_block(load).configuration or {})
        worker = environments.named_workers(self.repo_path)['features']
        self.assertEqual(worker.label, 'feast + mlflow')
        text = (folder / 'pyproject.toml').read_text()
        self.assertIn('dependencies = ["six==1.16.0", "packaging==24.1", "pyarrow"]', text)
        # The old workers stay until someone deletes them.
        self.assertIn('feast', environments.named_workers(self.repo_path))

    def test_a_worker_kept_apart_is_not_combined(self):
        environments._combine_cache.clear()
        folder = self.worker('gpu', dependencies=['six==1.16.0'])
        self.worker('tools', dependencies=['six==1.16.0'])
        pyproject = folder / 'pyproject.toml'
        pyproject.write_text(pyproject.read_text() + '\n[tool.mageml.worker]\ncombine = false\n')
        result = environments.combine_check(self.repo_path, ['gpu', 'tools'])
        self.assertEqual((result['state'], result['reason']),
                         ('kept_apart', 'gpu says combine = false.'))

    def test_the_digest_of_a_worker_without_mageml_settings_is_unchanged(self):
        # Workers prepared before labels existed keep their environments.
        text = b'[project]\nname = "x"\n\n[tool.uv]\nindex-url = "https://example.com"\n'
        self.assertEqual(environments.environment_bytes(text), text)
        named = text + b'\n[tool.mageml.worker]\nlabel = "X"\n'
        self.assertEqual(environments.environment_bytes(named), text)

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

    def test_an_export_takes_the_worker_its_blocks_share(self):
        from mage_ai.pipeline_services import export

        self.worker()
        shared, _, _ = self.pipeline(environment=dict(worker='legacy'))
        captured = export.capture(self.repo_path, [shared.uuid])
        self.assertEqual(captured.requirements['six'], '1.16.0')
        self.assertNotIn('python_workers', captured.manifest)

    def test_an_export_of_blocks_in_several_workers_builds_each_into_the_image(self):
        from mage_ai.pipeline_services import export

        self.worker()
        mixed, load, transform = self.pipeline(load_worker='legacy')
        captured = export.capture(self.repo_path, [mixed.uuid])
        # The transformer selects no worker: the image's Python has MageML's packages.
        self.assertEqual(captured.requirements['six'], importlib.metadata.version('six'))
        self.assertEqual(captured.manifest['python_workers'],
                         {'legacy': 'workers/legacy/.venv/bin/python'})
        blocks = {b['uuid']: b for b in captured.manifest['pipelines'][0]['blocks']}
        self.assertEqual(blocks[load].get('python_worker'), 'legacy')
        self.assertNotIn('python_worker', blocks[transform])

        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)
        with patch.object(export, '_write_python_lock'):
            export.write(captured, str(out), force=True)
        self.assertTrue((out / 'workers' / 'legacy' / 'uv.lock').is_file())
        dockerfile = (out / 'Dockerfile').read_text()
        self.assertIn('COPY workers/legacy/ /srv/mage-service/workers/legacy/', dockerfile)
        self.assertIn('uv sync --locked --no-dev', dockerfile)

    def test_an_extra_worker_needs_a_lock_and_gets_its_own_python_in_the_image(self):
        from mage_ai.pipeline_services import export

        folder = self.worker(name='other', lock=False)
        (folder / '.python-version').write_text('3.13\n')
        mixed, load, _ = self.pipeline(load_worker='other')
        with self.assertRaises(export.ExportError) as caught:
            export.capture(self.repo_path, [mixed.uuid])
        self.assertIn('Python worker other has no uv.lock', '\n'.join(caught.exception.problems))

        # Locked, it exports: the image installs Python 3.13 with uv for it, and the
        # worker keeps its .python-version.
        import subprocess

        subprocess.run(['uv', 'lock', '--project', str(folder)], check=True,
                       capture_output=True)
        captured = export.capture(self.repo_path, [mixed.uuid])
        self.assertIn('Python worker other runs on Python 3.13', '\n'.join(captured.notes))
        dockerfile = export._worker_installs(captured, '0.11.29')
        self.assertIn('uv python install 3.13', dockerfile)
        self.assertIn('uv venv .venv --python 3.13', dockerfile)
        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)
        directory = Path(export.write(captured, out))
        self.assertEqual((directory / 'workers' / 'other' / '.python-version').read_text(),
                         '3.13\n')


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
        for folder in ('Feast_Store', 'feast-store'):
            path = project / environments.WORKERS_DIR / folder
            path.mkdir(parents=True)
            (path / 'pyproject.toml').write_text('[project]\nname = "same"\n')
        with self.assertRaisesRegex(
            environments.PipelineEnvironmentError, 'both named feast-store',
        ):
            environments.named_workers(str(project))

    def test_a_worker_runs_on_the_python_its_requires_python_allows(self):
        own = f'{sys.version_info.major}.{sys.version_info.minor}'
        # uv sync gave requires-python >= 3.13 the newest Python while the worker said 3.12.
        cases = {
            None: own, f'>={own}': own, '>=3.13': '3.13', '==3.11.*': '3.11',
            '<3.12': '3.11', '~=3.13.1': '3.13', 'not a specifier': own,
        }
        for requires, python in cases.items():
            self.assertEqual(environments._worker_python(requires), python, requires)

        project = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, project, True)
        folder = project / environments.WORKERS_DIR / 'newer'
        folder.mkdir(parents=True)
        (folder / 'pyproject.toml').write_text('[project]\nrequires-python = ">=3.13"\n')
        worker = environments.named_workers(str(project))['newer']
        self.assertEqual(worker.python, '3.13')
        (folder / '.python-version').write_text('3.14\n')
        worker = environments.named_workers(str(project))['newer']
        self.assertEqual(worker.python, '3.14')

        environment = project / 'environment'
        environment.mkdir()
        (environment / 'pyvenv.cfg').write_text('home = /x\nversion_info = 3.14.6\n')
        environments._check_python(worker, environment)
        (environment / 'pyvenv.cfg').write_text('home = /x\nversion_info = 3.12.11\n')
        with self.assertRaisesRegex(
            environments.PipelineEnvironmentError,
            'asks for Python 3.14, and its requires-python made uv use Python 3.12.11',
        ):
            environments._check_python(worker, environment)

    def test_a_prepare_removes_the_older_environments_of_its_worker_only(self):
        cache = Path(tempfile.mkdtemp())
        project = Path(tempfile.mkdtemp())
        for path in (cache, project):
            self.addCleanup(shutil.rmtree, path, True)
        folder = project / environments.WORKERS_DIR / 'feast'
        folder.mkdir(parents=True)
        (folder / 'pyproject.toml').write_text('[project]\nname = "mageml-worker-feast"\n')
        other = str(project / environments.WORKERS_DIR / 'other')

        def environment(name, source, prepared_at):
            directory = cache / name
            directory.mkdir()
            (directory / environments.MARKER).write_text(json.dumps(dict(
                source=source, prepared_at=prepared_at)))
            return directory

        with patch.dict('os.environ', {environments.ENVIRONMENTS_DIR_ENV: str(cache)}):
            worker = environments.named_workers(str(project))['feast']
            source = str(folder.resolve())
            oldest = environment('oldest', source, 1)
            older = environment('older', source, 2)
            previous = environment('previous', source, 3)
            current = environment(worker.directory.name, source, 4)
            not_ours = environment('not_ours', other, 0)
            before_markers = cache / 'unmarked'
            (before_markers / environments.MARKER).parent.mkdir()
            (before_markers / environments.MARKER).write_text('{}')
            lines = []
            environments._remove_superseded(worker, lines.append)
        self.assertEqual(sorted(p.name for p in cache.iterdir()),
                         sorted([current.name, previous.name, not_ours.name, 'unmarked']))
        self.assertFalse(oldest.exists() or older.exists())
        self.assertEqual(len(lines), 2)

    def test_a_worker_is_named_by_its_folder_and_found_by_its_old_project_name(self):
        from types import SimpleNamespace

        project = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, project, True)
        # A worker made before workers were named by their folder.
        path = project / environments.WORKERS_DIR / 'features'
        path.mkdir(parents=True)
        (path / 'pyproject.toml').write_text('[project]\nname = "feast-store"\n')
        self.assertEqual(list(environments.named_workers(str(project))), ['features'])

        def block(selected):
            return SimpleNamespace(configuration=dict(python_worker=selected),
                                   repo_path=str(project), pipeline=None)

        self.assertEqual(environments.worker_selection(block('Feast_Store')), 'features')
        self.assertEqual(environments.worker_selection(block('features')), 'features')
        self.assertEqual(environments.worker_selection(block('other')), 'other')

    def test_a_worker_named_like_the_package_it_needs_is_written_and_checked(self):
        project = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, project, True)
        # uv takes a project "feast" for the feast it depends on: the lock fails on the
        # version, or the package is never installed.
        folder = environments.new_worker(str(project), 'feast', 'feast')
        self.assertIn('name = "mageml-worker-feast"', (folder / 'pyproject.toml').read_text())
        worker = environments.named_workers(str(project))['feast']
        environments._check_project_name(worker)

        own = project / environments.WORKERS_DIR / 'pandas'
        own.mkdir(parents=True)
        (own / 'pyproject.toml').write_text(
            '[project]\nname = "Pandas"\ndependencies = ["pandas>=2", "pyarrow"]\n')
        with self.assertRaisesRegex(
            environments.PipelineEnvironmentError,
            'its project is named "Pandas", like the package it needs.*'
            'name = "mageml-worker-pandas"',
        ):
            environments.prepare(environments.named_workers(str(project))['pandas'],
                                 log=lambda _: None)


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


class NewWorkerTest(DBTestCase):
    """A Python worker added from a template: written, then locked and prepared in the
    background; its name checked first."""

    def setUp(self):
        super().setUp()
        from mage_ai.tests.factory import create_user

        self.owner = create_user(_owner=True)
        self.addCleanup(shutil.rmtree, Path(self.repo_path, environments.WORKERS_DIR), True)

    def call(self, payload):
        from mage_ai.api.operations.base import BaseOperation
        from mage_ai.api.operations.constants import OperationType

        return asyncio.run(BaseOperation(
            action=OperationType.CREATE, payload=dict(python_worker=payload),
            resource='python_workers', user=self.owner,
        ).execute())

    def test_a_worker_from_a_template_is_written_locked_and_prepared(self):
        steps = []

        def fake_run(command, log):
            steps.append(command[1])
            log('Resolved 12 packages')
            folder = Path(command[-1])
            (folder / 'uv.lock').write_text('version = 1\n')

        def fake_prepare(worker, log=print):
            steps.append(f'prepare {worker.alias} {worker.label}')

        with patch.object(environments, '_run', side_effect=fake_run), \
                patch.object(environments, 'prepare', side_effect=fake_prepare):
            started = self.call(dict(name='Feast', template='feast', label='Online features'))
            for _ in range(100):
                job = environments.prepare_job(self.repo_path, 'feast')
                if job['state'] != 'running':
                    break
                time.sleep(0.05)

        self.assertEqual(started['python_worker']['name'], 'feast')
        self.assertEqual(job['state'], 'completed', job['problems'])
        self.assertEqual(steps, ['lock', 'prepare feast Online features'])
        pyproject = Path(self.repo_path, environments.WORKERS_DIR, 'feast', 'pyproject.toml')
        text = pyproject.read_text()
        self.assertIn('"pandas>=2.2,<3"', text)
        self.assertIn('label = "Online features"', text)
        self.assertIn('description = "Feature retrieval with Feast, which needs pandas 2"', text)

    def test_the_api_combines_workers_for_a_pipeline_and_prepares_the_new_one(self):
        from mage_ai.data_preparation.models.block import Block

        for name, dependency in (('feast', 'six==1.16.0'), ('mlflow', 'packaging==24.1')):
            folder = Path(self.repo_path, environments.WORKERS_DIR, name)
            folder.mkdir(parents=True)
            (folder / 'pyproject.toml').write_text(
                f'[project]\nname = "{name}"\nversion = "0"\ndependencies = ["{dependency}"]\n')
        pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        uuids = []
        for worker in ('feast', 'mlflow'):
            block = Block.create(f'{pipeline.uuid}_{worker}', 'data_loader', self.repo_path,
                                 pipeline=pipeline, configuration=dict(python_worker=worker))
            uuids.append(block.uuid)

        steps = []
        with patch.object(environments, '_run',
                          side_effect=lambda command, log: steps.append(command[1])), \
                patch.object(environments, 'prepare',
                             side_effect=lambda worker, log=print: steps.append('prepare')):
            self.call(dict(name='both', combine=['feast', 'mlflow'],
                           pipeline_uuid=pipeline.uuid))
            for _ in range(100):
                job = environments.prepare_job(self.repo_path, 'both')
                if job and job['state'] != 'running':
                    break
                time.sleep(0.05)
        self.assertEqual(steps, ['lock', 'prepare'])
        pipeline = Pipeline.get(pipeline.uuid, repo_path=self.repo_path)
        self.assertEqual(
            [pipeline.get_block(u).configuration['python_worker'] for u in uuids],
            ['both', 'both'],
        )
        self.assertEqual(environments.named_workers(self.repo_path)['both'].label,
                         'feast + mlflow')
        refused = self.call(dict(name='other', combine=['feast'], pipeline_uuid=pipeline.uuid))
        self.assertEqual(refused['error']['code'], 400)

    def test_names_and_templates_are_checked_before_anything_is_written(self):
        for payload, message in (
            (dict(name='my worker!', template='empty'), 'letters, digits and "-"'),
            (dict(name='tools', template='spark'), 'no worker template spark'),
            (dict(name='mage', template='empty'), 'exists already'),
        ):
            with self.subTest(payload=payload):
                result = self.call(payload)
                self.assertEqual(result['error']['code'], 400)
                self.assertIn(message, result['error']['message'])
        self.assertFalse(Path(self.repo_path, environments.WORKERS_DIR, 'tools').exists())
