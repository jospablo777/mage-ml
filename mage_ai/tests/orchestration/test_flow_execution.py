import asyncio
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from freezegun import freeze_time

from mage_ai.data_preparation.executors.pipeline_executor import PipelineExecutor
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.data_preparation.models.triggers import ScheduleStatus, ScheduleType
from mage_ai.orchestration.db.models.schedules import (
    EventMatcher,
    PipelineRun,
    PipelineSchedule,
)
from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler, schedule_all
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.orchestration.triggers.event_trigger import EventTrigger
from mage_ai.tests.base_test import DBTestCase


class FlowExecutionTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)
        self.output = Path(self.repo_path) / f'{self.pipeline.uuid}.parquet'

    def block(self, name, kind, code, upstream=(), **kwargs):
        block = Block.create(
            f'{self.pipeline.uuid}_{name}', kind, self.repo_path, language='python', **kwargs,
        )
        Path(block.file_path).write_text(textwrap.dedent(code))
        self.pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    def branched_flow(self):
        load = self.block('load', 'data_loader', '''
            import pandas as pd
            @data_loader
            def load(**kwargs):
                return pd.DataFrame({
                    'id': pd.Series([1, 2, 3], dtype='Int64'),
                    'amount': pd.Series([10, None, 30], dtype='Int64'),
                })
        ''')
        filtered = self.block('filter', 'transformer', '''
            @transformer
            def filter_rows(frame, **kwargs):
                result = frame[frame['amount'].notna()].copy()
                result['amount'] *= kwargs['multiplier']
                return result
        ''', upstream=[load])
        aggregate = self.block('aggregate', 'transformer', '''
            import pandas as pd
            @transformer
            def aggregate(frame, **kwargs):
                return pd.DataFrame({'total': [int(frame['amount'].sum())]})
        ''', upstream=[load])
        self.block('export', 'data_exporter', '''
            @data_exporter
            def export(filtered, aggregate, **kwargs):
                result = filtered.assign(total=int(aggregate['total'].iloc[0]))
                result.to_parquet(kwargs['output_path'], index=False)
                return result
        ''', upstream=[filtered, aggregate])

    def execute_run(self, run, asynchronous=False):
        scheduler = PipelineScheduler(run)
        scheduler.start(should_schedule=False)
        executor = PipelineExecutor(self.pipeline, execution_partition=run.execution_partition)
        options = dict(
            pipeline_run_id=run.id,
            global_vars=run.get_variables(),
            allow_blocks_to_fail=run.pipeline_schedule.get_settings().allow_blocks_to_fail,
        )
        if asynchronous:
            asyncio.run(executor.execute_async(**options))
        else:
            executor.execute(**options)
        scheduler.schedule()
        run.refresh()
        return run

    def assert_export(self, run):
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        self.assertTrue(all(b.status.value == 'completed' for b in run.block_runs))
        frame = pd.read_parquet(self.output)
        self.assertEqual(
            frame.to_dict('list'), {'id': [1, 3], 'amount': [20, 60], 'total': [40, 40]},
        )
        self.assertEqual(str(frame['id'].dtype), 'Int64')

    def test_api_trigger_executes_branches_with_and_without_memory_cache(self):
        self.branched_flow()
        for cache in (False, True):
            with self.subTest(cache=cache):
                self.pipeline.cache_block_output_in_memory = cache
                self.pipeline.save()
                run = trigger_pipeline(self.pipeline.uuid, variables={
                    'multiplier': 2, 'output_path': str(self.output),
                })
                self.assert_export(self.execute_run(run))

    def test_async_execution_awaits_all_blocks(self):
        self.branched_flow()
        run = trigger_pipeline(self.pipeline.uuid, variables={
            'multiplier': 2, 'output_path': str(self.output),
        })
        self.assert_export(self.execute_run(run, asynchronous=True))

    def test_sync_call_in_running_loop_does_not_start_background_work(self):
        self.branched_flow()
        executor = PipelineExecutor(self.pipeline)

        async def run():
            with self.assertRaisesRegex(RuntimeError, 'execute_async'):
                executor.execute(global_vars={'multiplier': 2, 'output_path': str(self.output)})
            await asyncio.sleep(0)
            self.assertFalse(self.output.exists())

        asyncio.run(run())

    def test_failure_stops_downstream_blocks_and_flushes_logs(self):
        failed = self.block('fail', 'data_loader', '''
            @data_loader
            def fail(**kwargs):
                raise RuntimeError('source failed')
        ''')
        self.block('export', 'data_exporter', '''
            from pathlib import Path
            @data_exporter
            def export(*args, **kwargs):
                Path(kwargs['output_path']).write_text('unexpected execution')
        ''', upstream=[failed])
        run = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})
        scheduler = PipelineScheduler(run)
        scheduler.start(should_schedule=False)
        executor = PipelineExecutor(self.pipeline, execution_partition=run.execution_partition)
        with patch.object(executor.logger_manager, 'output_logs_to_destination') as flush:
            with self.assertRaisesRegex(RuntimeError, 'source failed'):
                executor.execute(pipeline_run_id=run.id, global_vars=run.get_variables())
            flush.assert_called_once()
        scheduler.schedule()
        run.refresh()
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.FAILED)
        self.assertFalse(self.output.exists())

    def test_cancelled_run_does_not_execute_blocks(self):
        self.branched_flow()
        run = trigger_pipeline(self.pipeline.uuid, variables={
            'multiplier': 2, 'output_path': str(self.output),
        })
        scheduler = PipelineScheduler(run)
        scheduler.start(should_schedule=False)
        scheduler.stop()
        executor = PipelineExecutor(self.pipeline, execution_partition=run.execution_partition)
        executor.execute(pipeline_run_id=run.id, global_vars=run.get_variables())
        run.refresh()
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.CANCELLED)
        self.assertFalse(self.output.exists())

    def test_allowed_failure_does_not_stop_independent_branch(self):
        self.block('fail', 'data_loader', '''
            @data_loader
            def fail(**kwargs):
                raise RuntimeError('optional source failed')
        ''')
        self.branched_flow()
        for cache in (False, True):
            with self.subTest(cache=cache):
                self.pipeline.cache_block_output_in_memory = cache
                self.pipeline.save()
                run = trigger_pipeline(self.pipeline.uuid, variables={
                    'multiplier': 2, 'output_path': str(self.output),
                })
                run.pipeline_schedule.update(settings={'allow_blocks_to_fail': True})
                self.execute_run(run)
                self.assertEqual(run.status, PipelineRun.PipelineRunStatus.FAILED)
                self.assertEqual(pd.read_parquet(self.output)['amount'].tolist(), [20, 60])
                self.assertEqual(len(run.completed_block_runs), 4)

    def test_cancellation_between_blocks_stops_downstream_execution(self):
        load = self.block('cancel', 'data_loader', '''
            from mage_ai.orchestration.db.models.schedules import PipelineRun
            @data_loader
            def load(**kwargs):
                run = PipelineRun.get_by_id(kwargs['pipeline_run_id'])
                run.update(status=PipelineRun.PipelineRunStatus.CANCELLED)
                return 42
        ''')
        self.block('export', 'data_exporter', '''
            from pathlib import Path
            @data_exporter
            def export(*args, **kwargs):
                Path(kwargs['output_path']).write_text('unexpected execution')
        ''', upstream=[load])
        run = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})
        self.execute_run(run)
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.CANCELLED)
        self.assertFalse(self.output.exists())

    def test_transient_block_failure_retries_before_export(self):
        load = self.block('retry', 'data_loader', '''
            @data_loader
            def load(**kwargs):
                attempts = kwargs['retry']['attempts']
                with open(kwargs['output_path'] + '.attempts', 'a') as stream:
                    stream.write(str(attempts))
                if attempts == 1:
                    raise ValueError('retry source')
                return 42
        ''')
        load.retry_config = {'retries': 1, 'delay': 0}
        self.pipeline.save()
        self.block('export', 'data_exporter', '''
            from pathlib import Path
            @data_exporter
            def export(value, **kwargs):
                Path(kwargs['output_path']).write_text(str(value))
        ''', upstream=[load])
        run = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})
        self.execute_run(run)
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        self.assertEqual(self.output.read_text(), '42')
        self.assertEqual(Path(str(self.output) + '.attempts').read_text(), '12')

    def test_false_condition_skips_its_downstream_branch(self):
        load = self.block('load', 'data_loader', '''
            @data_loader
            def load(**kwargs):
                return 42
        ''')
        guard = self.block('guard', 'conditional', '''
            @condition
            def should_run(*args, **kwargs):
                return False
        ''')
        self.pipeline.update_block(load, conditional_block_uuids=[guard.uuid])
        self.block('export', 'data_exporter', '''
            from pathlib import Path
            @data_exporter
            def export(*args, **kwargs):
                Path(kwargs['output_path']).write_text('unexpected execution')
        ''', upstream=[load])
        run = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})
        self.execute_run(run)
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        self.assertTrue(all(b.status.value == 'condition_failed' for b in run.block_runs))
        self.assertFalse(self.output.exists())

    def test_dynamic_children_reduce_to_one_export(self):
        load = self.block('dynamic', 'data_loader', '''
            @data_loader
            def load(**kwargs):
                return [[1, 2, 3], [{'block_uuid': str(i)} for i in range(3)]]
        ''', configuration={'dynamic': True})
        mapped = self.block('map', 'transformer', '''
            @transformer
            def transform(value, **kwargs):
                return value * 10
        ''', upstream=[load], configuration={'reduce_output': True})
        self.block('reduce', 'data_exporter', '''
            from pathlib import Path
            @data_exporter
            def export(values, **kwargs):
                Path(kwargs['output_path']).write_text(str(sum(values)))
        ''', upstream=[mapped])
        run = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})
        self.execute_run(run)
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        self.assertEqual(self.output.read_text(), '60')

    def test_command_preserves_spaces_in_project_path(self):
        self.pipeline.repo_config.repo_path = '/tmp/project with spaces'
        command = PipelineExecutor(self.pipeline)._run_commands()
        self.assertEqual(command[3], '/tmp/project with spaces')

    def test_event_matching_enqueues_each_trigger_once(self):
        self.branched_flow()
        schedule = PipelineSchedule.create(
            name='event', pipeline_uuid=self.pipeline.uuid, schedule_type=ScheduleType.EVENT,
            status=ScheduleStatus.ACTIVE,
            variables={'multiplier': 2, 'output_path': str(self.output)},
        )
        for pattern in ({'source': ['orders']}, {'detail': {'status': ['ready']}}):
            matcher = EventMatcher.create(pattern=pattern)
            matcher.update(pipeline_schedules=[schedule])
        event = {'source': 'orders', 'detail': {'status': 'ready'}}
        EventTrigger().run(event)
        runs = PipelineRun.query.filter_by(pipeline_schedule_id=schedule.id).all()
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0].variables['event'], event)
        self.assert_export(self.execute_run(runs[0]))
        schedule.update(status=ScheduleStatus.INACTIVE)
        EventTrigger().run(event)
        self.assertEqual(PipelineRun.query.filter_by(pipeline_schedule_id=schedule.id).count(), 1)

    @freeze_time('2026-09-10 12:00:10')
    def test_time_trigger_runs_once_per_cron_interval(self):
        self.branched_flow()
        schedule = PipelineSchedule.create(
            name='minute', pipeline_uuid=self.pipeline.uuid, schedule_type=ScheduleType.TIME,
            status=ScheduleStatus.ACTIVE, schedule_interval='* * * * *',
            start_time=datetime(2026, 9, 10, 11, 0, tzinfo=timezone.utc),
            variables={'multiplier': 2, 'output_path': str(self.output)},
        )
        with patch.object(PipelineScheduler, 'schedule'):
            schedule_all()
        run = PipelineRun.query.filter_by(pipeline_schedule_id=schedule.id).one()
        self.assert_export(self.execute_run(run))
        with patch.object(PipelineScheduler, 'schedule'):
            schedule_all()
        self.assertEqual(PipelineRun.query.filter_by(pipeline_schedule_id=schedule.id).count(), 1)
        with freeze_time('2026-09-10 12:01:10'), patch.object(PipelineScheduler, 'schedule'):
            schedule_all()
        self.assertEqual(PipelineRun.query.filter_by(pipeline_schedule_id=schedule.id).count(), 2)
        schedule.update(status=ScheduleStatus.INACTIVE)
