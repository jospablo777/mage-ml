"""
Partition runs (orchestration/partition_runs.py, ADR-0039): ordinary pipeline runs with the
partition's variables, execution_date and output folder, at most one active per partition.
"""
import asyncio
import textwrap
from datetime import datetime, timezone
from pathlib import Path

from mage_ai.data_preparation.executors.pipeline_executor import PipelineExecutor
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.orchestration.partition_runs import run_partition
from mage_ai.orchestration.partitions import PartitionError
from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
from mage_ai.tests.base_test import DBTestCase

DECLARATION = dict(key='day', daily=dict(start='2026-11-01', timezone='America/New_York'))


class PartitionRunsTest(DBTestCase):
    def pipeline(self, declaration=DECLARATION):
        pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(pipeline.delete)
        block = Block.create(f'{self._testMethodName}_load', 'data_loader', self.repo_path,
                             language='python')
        Path(block.file_path).write_text(textwrap.dedent('''
            import pandas as pd

            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'day': [kwargs['day']]})
        '''))
        pipeline.add_block(block)
        asyncio.run(pipeline.update(dict(partitions=declaration)))
        return Pipeline.get(pipeline.uuid, repo_path=self.repo_path)

    def test_the_declaration_is_kept_in_the_pipelines_metadata(self):
        pipeline = self.pipeline()
        self.assertEqual(pipeline.partitions, DECLARATION)
        self.assertEqual(pipeline.partition_declaration.key, 'day')
        self.assertIn('partitions:', Path(pipeline.config_path).read_text())

    def test_a_partition_has_one_active_run_with_its_variables(self):
        pipeline = self.pipeline()

        run, created = run_partition(pipeline, '2026-11-02')
        again, created_again = run_partition(pipeline, '2026-11-02')

        self.assertEqual((created, created_again), (True, False))
        self.assertEqual(again.id, run.id)
        self.assertEqual(run.partition, '2026-11-02')
        self.assertEqual(run.variables['day'], '2026-11-02')
        self.assertEqual(run.execution_partition, 'partitions/2026-11-02')
        # The partition's start in its time zone, in UTC.
        self.assertEqual(run.execution_date.replace(tzinfo=timezone.utc),
                         datetime(2026, 11, 2, 5, 0, tzinfo=timezone.utc))
        # Once it ends, the partition gets a new run.
        run.update(status=PipelineRun.PipelineRunStatus.COMPLETED)
        later, created_later = run_partition(pipeline, '2026-11-02')
        self.assertTrue(created_later)
        self.assertNotEqual(later.id, run.id)

    def test_a_partition_that_is_not_declared_is_refused(self):
        with self.assertRaisesRegex(PartitionError, "'2026-11-31' is not a daily partition"):
            run_partition(self.pipeline(), '2026-11-31')
        plain = Pipeline.create(f'{self._testMethodName}_plain', repo_path=self.repo_path)
        self.addCleanup(plain.delete)
        with self.assertRaisesRegex(PartitionError, 'declares no partitions'):
            run_partition(plain, '2026-11-02')

    def test_a_partitions_outputs_go_to_its_own_folder(self):
        pipeline = self.pipeline()
        run, _ = run_partition(pipeline, '2026-11-02')
        scheduler = PipelineScheduler(run)
        scheduler.start(should_schedule=False)
        PipelineExecutor(pipeline, execution_partition=run.execution_partition).execute(
            pipeline_run_id=run.id, global_vars=run.get_variables(),
        )
        scheduler.schedule()
        run.refresh()

        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        output = Path(pipeline.variable_manager.pipeline_path(pipeline.uuid), '.variables',
                      'partitions', '2026-11-02', f'{self._testMethodName}_load', 'output_0')
        self.assertTrue(output.is_dir(), output)
