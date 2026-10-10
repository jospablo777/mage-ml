"""
Resource-aware scheduling: block runs start only when the shared limits, memory and GPUs
their blocks declare are free; waiting block runs say why, impossible requests fail, and
a block run's thread pools and GPUs follow what it holds.
"""
import os
from pathlib import Path
from unittest.mock import patch

import yaml

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import fusion, pipeline_scheduler_original, resources
from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun
from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import DBTestCase, TestCase

GiB = 1024 ** 3


class DecisionTest(TestCase):
    def test_sizes_parse_and_bad_requests_are_explained(self):
        self.assertEqual(resources.parse_bytes('8GiB', 'm'), 8 * GiB)
        self.assertEqual(resources.parse_bytes('1.5G', 'm'), 1_500_000_000)
        self.assertEqual(resources.parse_bytes(1024, 'm'), 1024)
        for value in ('8 GiBs', '-1G', 0, True):
            with self.assertRaises(resources.ResourceError):
                resources.parse_bytes(value, 'm')

        block = Block('b', 'b', 'transformer', configuration=dict(resources=dict(memroy='1G')))
        with self.assertRaises(resources.ResourceError) as caught:
            resources.request_of(block)
        self.assertIn('unknown settings: memroy', str(caught.exception))

    def test_limits_memory_and_gpus_are_counted_from_what_runs_hold(self):
        policy = resources.Policy(limits=dict(warehouse=2), memory=16 * GiB, gpus=['0', '1'])
        held = [
            dict(project='p', launched_by='me', uses=['warehouse'], memory=10 * GiB, gpus=['0']),
            dict(project='p', launched_by='other', uses=['warehouse'], memory=10 * GiB),
            dict(project='q', launched_by='me', uses=['warehouse'], memory=10 * GiB),
        ]
        waiting = resources.decide(resources.Request(uses=('warehouse',)), policy, held, 'p', 'me')
        self.assertEqual(waiting.reason, 'waiting for warehouse: 2 of 2 in use')

        # Memory counts this scheduler's block runs only.
        memory = resources.decide(resources.Request(memory=8 * GiB), policy, held, 'p', 'me')
        self.assertFalse(memory.admitted)
        self.assertIn('waiting for memory', memory.reason)
        fits = resources.decide(resources.Request(memory=6 * GiB), policy, held, 'p', 'me')
        self.assertTrue(fits.admitted)

        gpu = resources.decide(resources.Request(gpu=1), policy, held, 'p', 'me')
        self.assertEqual(gpu.held['gpus'], ['1'])
        self.assertFalse(resources.decide(resources.Request(gpu=2), policy, held, 'p', 'me').admitted)

        for request, reason in [
            (resources.Request(uses=('lake',)), 'not declared'),
            (resources.Request(memory=32 * GiB), 'needs 32.0 GiB'),
            (resources.Request(gpu=3), 'declares 2'),
        ]:
            decision = resources.decide(request, policy, [], 'p', 'me')
            self.assertTrue(decision.impossible)
            self.assertIn(reason, decision.reason)

    def test_a_block_run_gets_its_threads_and_devices(self):
        with patch.dict(os.environ, {}, clear=True):
            env = resources.environment_for(dict(cpu=2, gpus=['1']))
        self.assertEqual(env['OMP_NUM_THREADS'], '2')
        self.assertEqual(env['POLARS_MAX_THREADS'], '2')
        self.assertEqual(env['CUDA_VISIBLE_DEVICES'], '1')
        with patch.dict(os.environ, {'OMP_NUM_THREADS': '8'}):
            self.assertNotIn('OMP_NUM_THREADS', resources.environment_for(dict(cpu=2)))


class AdmissionTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.metadata = Path(self.repo_path) / 'metadata.yaml'
        if self.metadata.exists():
            original = self.metadata.read_text()
            self.addCleanup(self.metadata.write_text, original)
        else:
            original = ''
            self.addCleanup(self.metadata.unlink, missing_ok=True)
        config = yaml.safe_load(original) or {}
        config['resources'] = dict(limits=dict(warehouse=1), memory='4GiB', gpus=1)
        self.metadata.write_text(yaml.safe_dump(config))

    def pipeline(self, block_resources):
        pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        Block.create(
            f'{pipeline.uuid}_load', 'data_loader', self.repo_path, pipeline=pipeline,
            configuration=dict(resources=block_resources),
        )
        return Pipeline.get(pipeline.uuid, repo_path=self.repo_path)

    def schedule(self, pipeline):
        run = trigger_pipeline(pipeline.uuid)
        job_manager = pipeline_scheduler_original.get_job_manager()
        with patch.object(job_manager, 'add_job'), \
                patch.object(job_manager, 'has_block_run_job', return_value=True), \
                patch.object(pipeline_scheduler_original, '_launcher', return_value='me'):
            scheduler = PipelineScheduler(run)
            scheduler.start(should_schedule=False)
            scheduler.schedule()
        run.refresh()
        return run.block_runs[0]

    def reschedule(self, block_run):
        block_run.pipeline_run.refresh()
        job_manager = pipeline_scheduler_original.get_job_manager()
        with patch.object(job_manager, 'add_job'), \
                patch.object(job_manager, 'has_block_run_job', return_value=True), \
                patch.object(pipeline_scheduler_original, '_launcher', return_value='me'):
            PipelineScheduler(block_run.pipeline_run).schedule()
        block_run.refresh()
        return block_run

    def test_a_shared_limit_holds_block_runs_of_other_pipelines_until_it_frees(self):
        first = self.schedule(self.pipeline(dict(uses=['warehouse'])))
        self.assertEqual(first.status, BlockRun.BlockRunStatus.QUEUED)
        self.assertEqual(first.metrics['resources']['uses'], ['warehouse'])

        second = self.schedule(self.pipeline(dict(uses=['warehouse'])))
        self.assertEqual(second.status, BlockRun.BlockRunStatus.INITIAL)
        self.assertEqual(second.metrics['waiting'], 'waiting for warehouse: 1 of 1 in use')

        first.update(status=BlockRun.BlockRunStatus.COMPLETED)
        second = self.reschedule(second)
        self.assertEqual(second.status, BlockRun.BlockRunStatus.QUEUED)
        self.assertNotIn('waiting', second.metrics)

    def test_gpus_go_to_one_block_run_at_a_time_and_impossible_requests_fail(self):
        first = self.schedule(self.pipeline(dict(gpu=1, cpu=2)))
        self.assertEqual(first.metrics['resources']['gpus'], ['0'])
        second = self.schedule(self.pipeline(dict(gpu=1)))
        self.assertIn('waiting for GPUs', second.metrics['waiting'])

        too_big = self.schedule(self.pipeline(dict(memory='8GiB')))
        self.assertEqual(too_big.status, BlockRun.BlockRunStatus.FAILED)
        self.assertIn('needs 8.0 GiB', too_big.metrics['error']['message'])
        unknown = self.schedule(self.pipeline(dict(uses=['lake'])))
        self.assertEqual(unknown.status, BlockRun.BlockRunStatus.FAILED)
        self.assertIn('lake is not declared', unknown.metrics['error']['message'])

    def test_blocks_that_declare_resources_run_alone(self):
        pipeline = self.pipeline(dict(memory='1GiB'))
        block = list(pipeline.blocks_by_uuid.values())[0]
        self.assertFalse(fusion.block_can_fuse(pipeline, block))
        self.assertEqual(PipelineRun.PipelineRunStatus.RUNNING.value, 'running')
