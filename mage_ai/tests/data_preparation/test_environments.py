"""
Pipeline environments: a pipeline's Python blocks run in a virtual environment with the
packages its requirements file lists. Builds with uv, from its cache when it has the
packages; skipped where uv is not installed.
"""
import asyncio
import shutil
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

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
