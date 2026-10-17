import asyncio
import textwrap
import time
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

    def test_outputs_missing_from_the_memory_cache_are_read_from_storage(self):
        """A resumed run caches only the blocks it runs; the others ran before."""
        self.branched_flow()
        variables = {'multiplier': 2, 'output_path': str(self.output)}
        run = self.execute_run(trigger_pipeline(self.pipeline.uuid, variables=variables))
        self.assert_export(run)
        self.output.unlink()
        uuid = self.pipeline.uuid
        cached = pd.DataFrame({
            'id': pd.Series([7], dtype='Int64'), 'amount': pd.Series([5], dtype='Int64'),
        })

        self.pipeline.get_block(f'{uuid}_export').execute_sync(
            block_run_outputs_cache={f'{uuid}_filter': [cached]}, global_vars=variables,
            execution_partition=run.execution_partition, run_all_blocks=True,
        )

        # filter from the cache; aggregate from its stored output. It came as no input.
        self.assertEqual(pd.read_parquet(self.output).to_dict('list'), {
            'id': [7], 'amount': [5], 'total': [40],
        })

    def test_block_tests_receive_the_output_with_and_without_memory_cache(self):
        load = self.block('load', 'data_loader', '''
            import pandas as pd
            @data_loader
            def load(**kwargs):
                return pd.DataFrame({'id': [1, 2]})

            @test
            def test_frame(frame, *args):
                assert isinstance(frame, pd.DataFrame), type(frame)
                assert frame['id'].tolist() == [1, 2]
        ''')
        self.block('export', 'data_exporter', '''
            @data_exporter
            def export(frame, **kwargs):
                frame.to_parquet(kwargs['output_path'])
        ''', upstream=[load])
        for cache in (False, True):
            with self.subTest(cache=cache):
                self.pipeline.cache_block_output_in_memory = cache
                self.pipeline.save()
                run = trigger_pipeline(self.pipeline.uuid, variables={
                    'output_path': str(self.output),
                })
                self.execute_run(run)
                self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)

    def test_the_memory_cache_drops_outputs_once_read(self):
        self.branched_flow()
        self.pipeline.cache_block_output_in_memory = True
        self.pipeline.save()
        uuid = self.pipeline.uuid
        cached_after_wave = []
        release = PipelineExecutor._PipelineExecutor__release_read_outputs

        def record(executor, pipeline_run, cache):
            release(executor, pipeline_run, cache)
            cached_after_wave.append(sorted(key[len(uuid) + 1:] for key in cache))

        with patch.object(PipelineExecutor, '_PipelineExecutor__release_read_outputs', record):
            run = self.execute_run(trigger_pipeline(uuid, variables={
                'multiplier': 2, 'output_path': str(self.output),
            }))

        self.assert_export(run)
        # load is kept until filter and aggregate have read it, then dropped; the export
        # has no downstream block, and nothing is kept at the end.
        self.assertEqual(cached_after_wave[0], ['load'])
        self.assertNotIn('load', cached_after_wave[-2])
        self.assertEqual(cached_after_wave[-1], [])

    def test_cli_runs_keep_their_outputs_under_their_partition(self):
        # mage run dropped --execution-partition: concurrent CLI runs shared one output
        # path (I-111). Each run reads its own block outputs back from its partition.
        from mage_ai.data_preparation.variable_manager import VariableManager
        from mage_ai.orchestration import fusion

        load = self.block('load', 'data_loader', '''
            import pandas as pd
            @data_loader
            def load(**kwargs):
                return pd.DataFrame({'n': pd.Series([kwargs['n']], dtype='Int64')})
        ''')
        double = self.block('double', 'transformer', '''
            @transformer
            def double(frame, **kwargs):
                return frame.assign(n=frame['n'] * 2)
        ''', upstream=[load])
        self.assertTrue(fusion.stages_apply_to_cli(self.pipeline))
        for partition, n in (('cli_a', 1), ('cli_b', 5)):
            PipelineExecutor(self.pipeline, execution_partition=partition).execute(
                global_vars=dict(n=n), update_status=False,
            )
        manager = VariableManager.get_manager(self.repo_path, self.pipeline.variables_dir)
        for partition, expected in (('cli_a', 2), ('cli_b', 10)):
            frame = manager.get_variable(
                self.pipeline.uuid, double.uuid, 'output_0', partition=partition,
            )
            self.assertEqual(int(frame['n'].iloc[0]), expected)

    def test_a_markdown_block_keeps_the_pipeline_in_stages(self):
        # A Markdown block made the whole pipeline ineligible for mage run's stages (I-115).
        from mage_ai.orchestration import fusion

        load = self.block('load', 'data_loader', '''
            import pandas as pd
            @data_loader
            def load(**kwargs):
                return pd.DataFrame({'n': [1]})
        ''')
        notes = Block.create(
            f'{self.pipeline.uuid}_notes', 'markdown', self.repo_path, language='markdown',
        )
        self.pipeline.add_block(notes)
        self.assertTrue(fusion.stages_apply_to_cli(self.pipeline))
        self.assertEqual(fusion.fusion_plan(self.pipeline), [[load.uuid]])
        PipelineExecutor(self.pipeline).execute(global_vars={}, update_status=False)

    def test_mage_run_ends_with_each_blocks_time_and_tests(self):
        # Tests are off by default in mage run; a pipeline of @test functions passed
        # without saying none ran.
        import contextlib
        import io

        self.block('load', 'data_loader', '''
            import pandas as pd
            @data_loader
            def load(**kwargs):
                return pd.DataFrame({'n': [1]})

            @test
            def has_rows(frame, *args):
                assert len(frame) == 1

            @test
            def has_n(frame, *args):
                assert 'n' in frame
        ''')

        def run(tests):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                PipelineExecutor(self.pipeline).execute(
                    global_vars={}, run_tests=tests, update_status=False,
                )
            return out.getvalue()

        skipped = run(False)
        self.assertIn('2 not run', skipped)
        self.assertIn('2 block tests were not run: run with --test to run them.', skipped)
        ran = run(True)
        self.assertIn('2/2 passed', ran)
        self.assertIn('2 tests in 1 blocks, all passed.', ran)

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

    def test_a_failed_block_in_one_process_keeps_its_error(self):
        """The error was left out when blocks ran in one process."""
        self.block('fail', 'data_loader', '''
            @data_loader
            def fail(**kwargs):
                raise RuntimeError('source failed')
        ''')
        run = trigger_pipeline(self.pipeline.uuid, variables={})
        run.pipeline_schedule.update(settings={'allow_blocks_to_fail': True})

        self.execute_run(run)

        block_run = run.block_runs[0]
        block_run.refresh()
        self.assertEqual(block_run.status.value, 'failed')
        self.assertIn('source failed', block_run.metrics['error']['error'])
        self.assertIn('RuntimeError', block_run.metrics['error']['message'])

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

    def test_one_process_run_gets_the_triggers_allow_blocks_to_fail(self):
        """The one-process job dropped the setting and stopped at the first failure."""
        from mage_ai.orchestration import pipeline_scheduler_original

        self.block('fail', 'data_loader', '''
            @data_loader
            def fail(**kwargs):
                raise RuntimeError('optional source failed')
        ''')
        self.branched_flow()
        self.pipeline.run_pipeline_in_one_process = True
        self.pipeline.save()
        run = trigger_pipeline(self.pipeline.uuid, variables={
            'multiplier': 2, 'output_path': str(self.output),
        })
        run.pipeline_schedule.update(settings={'allow_blocks_to_fail': True})
        jobs = []
        job_manager = pipeline_scheduler_original.get_job_manager()
        with patch.object(job_manager, 'add_job', side_effect=lambda *a, **k: jobs.append(
            (a, k),
        )), patch.object(job_manager, 'has_pipeline_run_job', return_value=False):
            scheduler = PipelineScheduler(run)
            scheduler.start(should_schedule=False)
            scheduler.schedule()

        (args, kwargs), = jobs
        self.assertIs(kwargs['allow_blocks_to_fail'], True)
        args[2](*args[3:], **kwargs)

        # One process ran every branch that could run.
        self.assertEqual(pd.read_parquet(self.output)['amount'].tolist(), [20, 60])
        run.refresh()
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

    def test_the_pipelines_retry_config_applies_when_the_block_leaves_it_null(self):
        load = self.block('retry', 'data_loader', '''
            @data_loader
            def load(**kwargs):
                if kwargs['retry']['attempts'] == 1:
                    raise ValueError('retry source')
                return 42
        ''')
        load.retry_config = {'retries': None, 'delay': 0}
        self.pipeline.retry_config = {'retries': 1, 'delay': 0}
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

    def empty_dynamic_flow(self, reduce_output: bool):
        load = self.block('dynamic', 'data_loader', '''
            @data_loader
            def load(**kwargs):
                return [[], []]
        ''', configuration={'dynamic': True})
        mapped = self.block('map', 'transformer', '''
            @transformer
            def transform(value, **kwargs):
                with open(kwargs['output_path'] + '.calls', 'a') as f:
                    f.write(repr(value))
                return value
        ''', upstream=[load], configuration={'reduce_output': reduce_output})
        self.block('reduce', 'data_exporter', '''
            from pathlib import Path
            @data_exporter
            def export(values, **kwargs):
                Path(kwargs['output_path']).write_text(repr(values))
        ''', upstream=[mapped])
        run = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})
        start = time.monotonic()
        self.execute_run(run)
        # It waited 12 times 10 seconds for items, then ran one child without input.
        self.assertLess(time.monotonic() - start, 60)
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        self.assertFalse(Path(str(self.output) + '.calls').exists())
        return run

    def test_a_dynamic_block_without_items_runs_no_children(self):
        """Upstream #6045: a child ran as :0 and failed for missing input."""
        self.empty_dynamic_flow(reduce_output=True)

        self.assertEqual(self.output.read_text(), '[]')

    def test_children_of_an_empty_dynamic_block_have_no_children(self):
        run = self.empty_dynamic_flow(reduce_output=False)

        self.assertFalse(any(':' in b.block_uuid for b in run.block_runs))

    def test_children_of_one_dynamic_block_pair_item_by_item(self):
        """Upstream #5980: a block fed by B and C, both children of A, got n * n children."""
        load = self.block('dynamic', 'data_loader', '''
            @data_loader
            def load(**kwargs):
                return [[1, 2, 3], [{'block_uuid': str(i)} for i in range(3)]]
        ''', configuration={'dynamic': True})
        tens = self.block('tens', 'transformer', '''
            @transformer
            def transform(value, **kwargs):
                return value * 10
        ''', upstream=[load])
        hundreds = self.block('hundreds', 'transformer', '''
            @transformer
            def transform(value, **kwargs):
                return value * 100
        ''', upstream=[load])
        pair = self.block('pair', 'transformer', '''
            @transformer
            def transform(tens, hundreds, **kwargs):
                return dict(tens=tens, hundreds=hundreds)
        ''', upstream=[tens, hundreds], configuration={'reduce_output': True})
        self.block('collect', 'data_exporter', '''
            from pathlib import Path
            @data_exporter
            def export(pairs, **kwargs):
                Path(kwargs['output_path']).write_text(
                    repr(sorted((p['tens'], p['hundreds']) for p in pairs)),
                )
        ''', upstream=[pair])
        run = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})

        self.execute_run(run)

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        children = [b for b in run.block_runs if b.block_uuid.startswith(f'{pair.uuid}:')]
        self.assertEqual(len(children), 3)
        self.assertEqual(self.output.read_text(), '[(10, 100), (20, 200), (30, 300)]')

    def test_no_database_transaction_stays_open_while_a_block_runs(self):
        """
        Reads before the block opened a transaction that stayed open for the whole block:
        on PostgreSQL an idle-in-transaction connection holding locks (upstream #6030).
        """
        self.block('load', 'data_loader', '''
            from pathlib import Path
            from mage_ai.orchestration.db import db_connection
            @data_loader
            def load(**kwargs):
                Path(kwargs['output_path']).write_text(str(db_connection.session().in_transaction()))
                return 1
        ''')
        run = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})

        self.execute_run(run)

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        self.assertEqual(self.output.read_text(), 'False')

    def test_async_block_functions_run(self):
        """An async def block returned its coroutine unrun (upstream #5164)."""
        load = self.block('load', 'data_loader', '''
            import asyncio
            @data_loader
            async def load(**kwargs):
                await asyncio.sleep(0)
                return [1, 2, 3]
        ''')
        self.block('export', 'data_exporter', '''
            from pathlib import Path
            @data_exporter
            async def export(values, **kwargs):
                Path(kwargs['output_path']).write_text(str(sum(values)))
        ''', upstream=[load])
        run = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})

        self.execute_run(run)

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        self.assertEqual(self.output.read_text(), '6')

    def test_cancelling_a_run_cancels_the_runs_its_blocks_started(self):
        """A child run kept running after its parent was cancelled (upstream #5641)."""
        from mage_ai.orchestration.pipeline_scheduler_original import stop_pipeline_run

        child = Pipeline.create(f'{self.pipeline.uuid}_child', repo_path=self.repo_path)
        self.addCleanup(child.delete)
        self.block('start_child', 'data_loader', f'''
            from mage_ai.orchestration.triggers.api import trigger_pipeline
            @data_loader
            def load(**kwargs):
                return trigger_pipeline('{child.uuid}').id
        ''')
        parent = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})

        self.execute_run(parent)

        child_run = PipelineRun.query.filter_by(pipeline_uuid=child.uuid).one()
        self.assertEqual(child_run.metrics['parent_pipeline_run_id'], parent.id)

        # The parent is still running when it is cancelled.
        parent.update(status=PipelineRun.PipelineRunStatus.RUNNING)
        child_run.update(status=PipelineRun.PipelineRunStatus.RUNNING)
        with patch(
            'mage_ai.orchestration.pipeline_scheduler_original.cancel_block_runs_and_jobs',
        ):
            stop_pipeline_run(parent)

        child_run.refresh()
        self.assertEqual(child_run.status, PipelineRun.PipelineRunStatus.CANCELLED)

    def test_listed_library_loggers_reach_the_block_log(self):
        """Libraries' log records, such as urllib3's, never reached block logs (#5766)."""
        import logging

        self.block('load', 'data_loader', '''
            import logging
            @data_loader
            def load(**kwargs):
                logging.getLogger('example.library').debug('library says hi')
                logging.getLogger('example.unlisted').warning('not listed')
                return 1
        ''')
        self.pipeline.repo_config.logging_config = {'loggers': {'example.library': 'DEBUG'}}
        run = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})

        self.execute_run(run)

        content = run.block_runs[0].logs['content']
        self.assertIn('[example.library] library says hi', content)
        self.assertNotIn('not listed', content)
        # The block's run put the logger back as it was.
        self.assertEqual(logging.getLogger('example.library').handlers, [])

    def test_a_disabled_block_and_its_downstream_blocks_are_skipped(self):
        """Blocks could not be disabled (upstream #5470)."""
        load = self.block('load', 'data_loader', '''
            @data_loader
            def load(**kwargs):
                return 1
        ''')
        disabled = self.block('disabled', 'transformer', '''
            from pathlib import Path
            @transformer
            def transform(value, **kwargs):
                Path(kwargs['output_path']).write_text('ran')
                return value
        ''', upstream=[load], configuration={'disabled': True})
        self.block('export', 'data_exporter', '''
            from pathlib import Path
            @data_exporter
            def export(value, **kwargs):
                Path(kwargs['output_path']).write_text('exported')
        ''', upstream=[disabled])
        run = trigger_pipeline(self.pipeline.uuid, variables={'output_path': str(self.output)})

        self.execute_run(run)

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        statuses = {b.block_uuid.replace(f'{self.pipeline.uuid}_', ''): b.status.value
                    for b in run.block_runs}
        self.assertEqual(statuses, {
            'load': 'completed', 'disabled': 'condition_failed', 'export': 'condition_failed',
        })
        self.assertFalse(self.output.exists())

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
    def test_an_empty_cron_leaves_other_triggers_running(self):
        """An active TIME trigger saved with "" broke each scheduler pass (upstream #5769)."""
        self.branched_flow()
        start = datetime(2026, 9, 10, 11, 0, tzinfo=timezone.utc)
        empty = PipelineSchedule.create(
            name='empty', pipeline_uuid=self.pipeline.uuid, schedule_type=ScheduleType.TIME,
            status=ScheduleStatus.ACTIVE, schedule_interval='', start_time=start,
        )
        valid = PipelineSchedule.create(
            name='minute', pipeline_uuid=self.pipeline.uuid, schedule_type=ScheduleType.TIME,
            status=ScheduleStatus.ACTIVE, schedule_interval='* * * * *', start_time=start,
        )

        with patch.object(PipelineScheduler, 'schedule'):
            schedule_all()

        self.assertIsNone(empty.schedule_interval)
        self.assertEqual(PipelineRun.query.filter_by(pipeline_schedule_id=empty.id).count(), 0)
        self.assertEqual(PipelineRun.query.filter_by(pipeline_schedule_id=valid.id).count(), 1)

    @freeze_time('2026-09-10 12:00:10')
    def test_a_stored_empty_cron_and_a_failing_trigger_leave_the_others_running(self):
        self.branched_flow()
        start = datetime(2026, 9, 10, 11, 0, tzinfo=timezone.utc)
        schedules = [
            PipelineSchedule.create(
                name=name, pipeline_uuid=self.pipeline.uuid, schedule_type=ScheduleType.TIME,
                status=ScheduleStatus.ACTIVE, schedule_interval='* * * * *', start_time=start,
            )
            for name in ('stored_empty', 'failing', 'valid')
        ]
        # Saved before empty crons were refused: the validator does not run on this update.
        PipelineSchedule.query.filter_by(id=schedules[0].id).update({'schedule_interval': ''})
        PipelineSchedule.query.session.commit()
        should_schedule = PipelineSchedule.should_schedule

        def failing_for_one(schedule, *args, **kwargs):
            if schedule.name == 'failing':
                raise RuntimeError('broken trigger')
            return should_schedule(schedule, *args, **kwargs)

        with patch.object(PipelineScheduler, 'schedule'), \
                patch.object(PipelineSchedule, 'should_schedule', failing_for_one):
            schedule_all()

        counts = [
            PipelineRun.query.filter_by(pipeline_schedule_id=s.id).count() for s in schedules
        ]
        self.assertEqual(counts, [0, 0, 1])

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
