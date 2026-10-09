"""
Pipeline runs with block fusion, driven through the scheduler: each job it enqueues runs
in this process, as a worker would run it, until the run ends.
"""
import os
import textwrap
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import pipeline_scheduler_original
from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun
from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import DBTestCase

LOAD = '''
    import pandas as pd
    @data_loader
    def load(**kwargs):
        frame = pd.DataFrame({
            'id': pd.Series([1, 2, 3], dtype='Int64'),
            'amount': [10.0, 20.0, 30.0],
            'name': pd.Series(['a', None, 'c'], dtype='str'),
        })
        open(kwargs['output_path'] + '.load', 'w').write(str(id(frame)))
        return frame
'''
CLEAN = '''
    @transformer
    def clean(frame, **kwargs):
        open(kwargs['output_path'] + '.clean', 'w').write(str(id(frame)))
        return frame[frame['amount'] > 10]
'''
ENRICH = '''
    @transformer
    def enrich(frame, **kwargs):
        return frame.assign(total=frame['amount'] * 2)
'''
EXPORT = '''
    @data_exporter
    def export(frame, **kwargs):
        frame.to_parquet(kwargs['output_path'], index=False)
'''


class FusionFlowTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)
        self.output = Path(self.repo_path) / f'{self.pipeline.uuid}.parquet'
        self.jobs = []

    def block(self, name, kind, code, upstream=(), **kwargs):
        block = Block.create(
            f'{self.pipeline.uuid}_{name}', kind, self.repo_path, language='python', **kwargs,
        )
        Path(block.file_path).write_text(textwrap.dedent(code))
        self.pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    def chain(self, clean=CLEAN, enrich=ENRICH):
        load = self.block('load', 'data_loader', LOAD)
        cleaned = self.block('clean', 'transformer', clean, upstream=[load])
        enriched = self.block('enrich', 'transformer', enrich, upstream=[cleaned])
        exported = self.block('export', 'data_exporter', EXPORT, upstream=[enriched])
        return [load, cleaned, enriched, exported]

    def fuse(self):
        self.pipeline.block_fusion = 'chains'
        self.pipeline.save()

    def run_pipeline(self, settings=None):
        run = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})
        if settings:
            run.pipeline_schedule.update(settings=settings)
        job_manager = pipeline_scheduler_original.get_job_manager()

        def add_job(job_type, uid, target, *args, **kwargs):
            self.jobs.append((target.__name__, args))
            pending.append((target, args, kwargs))

        pending = []
        with patch.object(job_manager, 'add_job', side_effect=add_job), \
                patch.object(job_manager, 'has_block_run_job', return_value=True):
            idle = 0
            for _ in range(30):
                run.refresh()
                scheduler = PipelineScheduler(run)
                if run.status == PipelineRun.PipelineRunStatus.INITIAL:
                    scheduler.start(should_schedule=False)
                scheduler.schedule()
                run.refresh()
                if run.status not in (
                    PipelineRun.PipelineRunStatus.INITIAL,
                    PipelineRun.PipelineRunStatus.RUNNING,
                ):
                    break
                idle = 0 if pending else idle + 1
                if idle >= 2:
                    break
                while pending:
                    target, args, kwargs = pending.pop(0)
                    try:
                        target(*args, **kwargs)
                    except Exception:
                        pass
        run.refresh()
        return run

    def statuses(self, run):
        return {
            b.block_uuid.rsplit('_', 1)[1]: b.status.value for b in run.block_runs
        }

    def stored(self, block, run):
        return self.pipeline.variable_manager.get_variable(
            self.pipeline.uuid, block.uuid, 'output_0', partition=run.execution_partition,
        )

    def test_a_chain_runs_as_one_stage_with_outputs_passed_in_memory(self):
        blocks = self.chain()
        self.fuse()

        run = self.run_pipeline()

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        self.assertEqual([name for name, _ in self.jobs], ['run_stage'])
        self.assertEqual(len(self.jobs[0][1][1]), 4)
        self.assertEqual(set(self.statuses(run).values()), {'completed'})
        # clean received the object load returned, not a copy read from storage.
        self.assertEqual(
            Path(str(self.output) + '.load').read_text(),
            Path(str(self.output) + '.clean').read_text(),
        )
        frame = pd.read_parquet(self.output)
        self.assertEqual(frame['total'].tolist(), [40.0, 60.0])
        # Every block's output is stored, as without fusion.
        for block in blocks[:3]:
            self.assertIsInstance(self.stored(block, run), pd.DataFrame)

    def test_fused_and_unfused_runs_store_the_same_outputs(self):
        blocks = self.chain()
        unfused = self.run_pipeline()
        self.fuse()
        fused = self.run_pipeline()

        for block in blocks[:3]:
            pd.testing.assert_frame_equal(self.stored(block, fused), self.stored(block, unfused))

    def test_without_the_setting_each_block_runs_alone(self):
        self.chain()

        run = self.run_pipeline()

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        self.assertEqual([name for name, _ in self.jobs], ['run_block'] * 4)

    def test_branches_are_separate_jobs(self):
        load = self.block('load', 'data_loader', LOAD)
        left = self.block('left', 'transformer', CLEAN, upstream=[load])
        right = self.block('right', 'transformer', ENRICH, upstream=[load])
        self.block('both', 'data_exporter', '''
            @data_exporter
            def export(left, right, **kwargs):
                right.loc[right['id'].isin(left['id'])].to_parquet(kwargs['output_path'])
        ''', upstream=[left, right])
        self.fuse()

        run = self.run_pipeline()

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        # load has two downstream blocks and both has two upstream blocks: no chains.
        self.assertEqual([name for name, _ in self.jobs], ['run_block'] * 4)
        # The two branches were enqueued in the same scheduler pass, to run in parallel.
        self.assertEqual(len(pd.read_parquet(self.output)), 2)

    def test_a_failed_block_stops_the_stage(self):
        self.chain(enrich='''
            @transformer
            def enrich(frame, **kwargs):
                raise ValueError('enrich failed')
        ''')
        self.fuse()

        run = self.run_pipeline()

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.FAILED)
        statuses = self.statuses(run)
        self.assertEqual(
            (statuses['load'], statuses['clean'], statuses['enrich']),
            ('completed', 'completed', 'failed'),
        )
        self.assertIn(statuses['export'], ('cancelled', 'upstream_failed'))
        self.assertFalse(self.output.exists())
        enrich = BlockRun.get(pipeline_run_id=run.id, block_uuid=f'{self.pipeline.uuid}_enrich')
        self.assertIn('enrich failed', enrich.metrics['error']['error'])

    def test_a_failed_block_with_failures_allowed_marks_the_rest_upstream_failed(self):
        self.chain(enrich='''
            @transformer
            def enrich(frame, **kwargs):
                raise ValueError('enrich failed')
        ''')
        self.fuse()

        run = self.run_pipeline(settings={'allow_blocks_to_fail': True})

        self.assertEqual(self.statuses(run)['export'], 'upstream_failed')
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.FAILED)

    def test_cancelling_the_run_stops_the_stage_at_the_next_block(self):
        self.chain(clean='''
            from mage_ai.orchestration.db.models.schedules import PipelineRun
            @transformer
            def clean(frame, **kwargs):
                run = PipelineRun.get_by_id(kwargs['pipeline_run_id'])
                run.update(status=PipelineRun.PipelineRunStatus.CANCELLED)
                return frame
        ''')
        self.fuse()

        run = self.run_pipeline()

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.CANCELLED)
        statuses = self.statuses(run)
        self.assertNotIn(statuses['enrich'], ('running', 'completed'))
        self.assertFalse(self.output.exists())

    def test_a_retry_reads_the_input_a_failed_attempt_changed_from_storage(self):
        blocks = self.chain(clean='''
            @transformer
            def clean(frame, **kwargs):
                frame['amount'] = frame['amount'] * 100
                if kwargs['retry']['attempts'] == 1:
                    raise ValueError('first attempt')
                return frame
        ''')
        blocks[1].retry_config = {'retries': 1, 'delay': 0}
        self.pipeline.save()
        self.fuse()

        run = self.run_pipeline()

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        # Multiplied once: the second attempt did not see the first attempt's change.
        self.assertEqual(pd.read_parquet(self.output)['amount'].tolist(), [1000.0, 2000.0,
                                                                           3000.0])

    def test_a_block_test_cannot_change_the_next_blocks_input(self):
        self.chain(clean='''
            @transformer
            def clean(frame, **kwargs):
                return frame

            @test
            def test_changes(frame, *args):
                frame['amount'] = -1
        ''')
        self.fuse()

        run = self.run_pipeline()

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        self.assertEqual(pd.read_parquet(self.output)['amount'].tolist(), [10.0, 20.0, 30.0])

    def test_a_block_with_fusion_off_runs_alone(self):
        blocks = self.chain()
        blocks[2].configuration = {'fusion': False}
        self.pipeline.save()
        self.fuse()

        run = self.run_pipeline()

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        stages = [len(args[1]) if name == 'run_stage' else 1 for name, args in self.jobs]
        self.assertEqual(stages, [2, 1, 1])

    def test_environment_changes_do_not_reach_the_next_block(self):
        self.chain(clean='''
            import os
            @transformer
            def clean(frame, **kwargs):
                os.environ['MAGE_FUSION_TEST'] = 'set'
                kwargs['context']['leak'] = 1
                return frame
        ''', enrich='''
            import os
            @transformer
            def enrich(frame, **kwargs):
                assert 'MAGE_FUSION_TEST' not in os.environ
                assert 'leak' not in kwargs.get('context', {})
                return frame
        ''')
        self.fuse()

        run = self.run_pipeline()

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED, self.statuses(run))
        self.assertNotIn('MAGE_FUSION_TEST', os.environ)

    def test_a_block_whose_process_died_runs_alone(self):
        blocks = self.chain()
        self.fuse()
        run = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})
        PipelineScheduler(run).start(should_schedule=False)
        clean = BlockRun.get(pipeline_run_id=run.id, block_uuid=blocks[1].uuid)
        clean.update(metrics={'crashes': 1})

        from mage_ai.orchestration.fusion import stage_block_runs

        load = BlockRun.get(pipeline_run_id=run.id, block_uuid=blocks[0].uuid)
        self.assertEqual(len(stage_block_runs(self.pipeline, load, run.block_runs)), 1)
        self.assertEqual(len(stage_block_runs(self.pipeline, clean, run.block_runs)), 1)


class ChainDetectionTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)
        self.pipeline.block_fusion = 'chains'

    def blocks(self, *names):
        blocks = []
        for index, name in enumerate(names):
            kind = 'data_loader' if index == 0 else 'transformer'
            block = Block.create(f'{self.pipeline.uuid}_{name}', kind, self.repo_path,
                                 language='python')
            self.pipeline.add_block(block, upstream_block_uuids=[blocks[-1].uuid] if blocks
                                    else [])
            blocks.append(block)
        self.pipeline.save()
        return blocks

    def stage(self, blocks, head=0, metrics=None, statuses=None):
        from mage_ai.orchestration.fusion import stage_block_runs

        runs = [
            BlockRun(id=index + 1, block_uuid=block.uuid, metrics=(metrics or {}).get(index),
                     status=(statuses or {}).get(index, BlockRun.BlockRunStatus.INITIAL))
            for index, block in enumerate(blocks)
        ]
        pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        return [r.id - 1 for r in stage_block_runs(pipeline, runs[head], runs)]

    def test_a_chain(self):
        self.assertEqual(self.stage(self.blocks('a', 'b', 'c')), [0, 1, 2])

    def test_a_stage_starts_at_its_head(self):
        self.assertEqual(self.stage(self.blocks('a', 'b', 'c'), head=1), [1, 2])

    def test_a_block_with_two_downstream_blocks_ends_the_chain(self):
        a, b = self.blocks('a', 'b')
        c = Block.create(f'{self.pipeline.uuid}_c', 'transformer', self.repo_path)
        self.pipeline.add_block(c, upstream_block_uuids=[a.uuid])
        self.pipeline.save()

        self.assertEqual(self.stage([a, b, c]), [0])

    def test_a_block_with_two_upstream_blocks_starts_its_own_stage(self):
        a, b = self.blocks('a', 'b')
        other = Block.create(f'{self.pipeline.uuid}_other', 'data_loader', self.repo_path)
        self.pipeline.add_block(other)
        c = Block.create(f'{self.pipeline.uuid}_c', 'transformer', self.repo_path)
        self.pipeline.add_block(c, upstream_block_uuids=[b.uuid, other.uuid])
        self.pipeline.save()

        self.assertEqual(self.stage([a, b, c]), [0, 1])

    def test_excluded_blocks_run_alone(self):
        cases = {
            'fusion off': dict(configuration={'fusion': False}),
            'read settings': dict(configuration={'variables': {'upstream': {}}}),
            'kubernetes': dict(executor_type='k8s'),
            'dynamic': dict(configuration={'dynamic': True}),
        }
        for name, attributes in cases.items():
            with self.subTest(name):
                self.pipeline = Pipeline.create(
                    f'{self._testMethodName}_{name.replace(" ", "_")}', repo_path=self.repo_path,
                )
                self.pipeline.block_fusion = 'chains'
                blocks = self.blocks('a', 'b', 'c')
                for key, value in attributes.items():
                    setattr(blocks[1], key, value)
                self.pipeline.save()

                self.assertEqual(self.stage(blocks), [0])
                self.assertEqual(self.stage(blocks, head=1), [1])

    def test_special_or_started_block_runs_end_the_chain(self):
        blocks = self.blocks('a', 'b', 'c')
        self.assertEqual(self.stage(blocks, metrics={1: {'crashes': 1}}), [0])
        self.assertEqual(self.stage(blocks, metrics={1: {'dynamic_block_index': 0}}), [0])
        self.assertEqual(
            self.stage(blocks, statuses={2: BlockRun.BlockRunStatus.COMPLETED}), [0, 1],
        )

    def test_the_plan_lists_the_stages_of_the_saved_pipeline(self):
        from mage_ai.orchestration.fusion import fusion_plan

        a, b = self.blocks('a', 'b')
        for name in ['c', 'd']:
            block = Block.create(f'{self.pipeline.uuid}_{name}', 'transformer', self.repo_path)
            self.pipeline.add_block(block, upstream_block_uuids=[b.uuid])
        e = Block.create(f'{self.pipeline.uuid}_e', 'data_exporter', self.repo_path)
        self.pipeline.add_block(e, upstream_block_uuids=[f'{self.pipeline.uuid}_d'])
        self.pipeline.save()
        pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        prefix = len(self.pipeline.uuid) + 1

        plan = [[uuid[prefix:] for uuid in stage] for stage in fusion_plan(pipeline)]

        # b has two downstream blocks, so the chain a -> b ends there; d -> e is a chain.
        self.assertEqual(sorted(plan), [['a', 'b'], ['c'], ['d', 'e']])
