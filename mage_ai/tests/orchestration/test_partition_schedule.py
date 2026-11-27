"""
Partitions that run when they close (orchestration/partition_schedule.py): the scheduler
queues each closed partition that never ran, newest first, at most catch_up at once.
"""
import asyncio
from datetime import datetime, timezone

from freezegun import freeze_time

from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import partition_schedule, partitions
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.orchestration.partition_runs import run_partition
from mage_ai.orchestration.partitions import PartitionError
from mage_ai.tests.base_test import DBTestCase, TestCase

NOW = datetime(2026, 11, 7, 12, tzinfo=timezone.utc)


class DeclarationTest(TestCase):
    def test_schedule_and_catch_up(self):
        declaration = partitions.parse(dict(
            key='day', daily=dict(start='2026-11-01'), schedule='on_close', catch_up=3))
        self.assertEqual((declaration.schedule, declaration.catch_up), ('on_close', 3))
        for document, message in [
            (dict(key='day', daily=dict(start='2026-11-01'), schedule='hourly'),
             'schedule is on_close'),
            (dict(key='day', daily=dict(start='2026-11-01'), catch_up=0), 'catch_up is how many'),
            (dict(key='region', values=['eu'], schedule='on_close'), 'values do not close'),
        ]:
            with self.assertRaisesRegex(PartitionError, message):
                partitions.parse(document)


class PartitionScheduleTest(DBTestCase):
    def pipeline(self, **settings):
        pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(pipeline.delete)
        asyncio.run(pipeline.update(dict(partitions=dict(
            key='day', daily=dict(start='2026-11-01'), **settings))))
        return Pipeline.get(pipeline.uuid, repo_path=self.repo_path)

    def test_the_newest_closed_partitions_that_never_ran_are_due(self):
        pipeline = self.pipeline(schedule='on_close', catch_up=3)
        self.assertEqual(partition_schedule.due(pipeline, NOW),
                         ['2026-11-06', '2026-11-05', '2026-11-04'])
        failed, _ = run_partition(pipeline, '2026-11-05')
        failed.update(status=PipelineRun.PipelineRunStatus.FAILED)

        queued = partition_schedule.tick([pipeline], NOW)

        # A partition that ran and failed waits for Run failed.
        self.assertEqual(queued, [f'{pipeline.uuid}/2026-11-06', f'{pipeline.uuid}/2026-11-04'])
        self.assertEqual(partition_schedule.due(pipeline, NOW), [])

    def test_without_on_close_nothing_is_queued(self):
        self.assertEqual(partition_schedule.due(self.pipeline(), NOW), [])

    def test_the_scheduler_queues_them_in_its_pass(self):
        from mage_ai.orchestration.pipeline_scheduler import schedule_all

        pipeline = self.pipeline(schedule='on_close', catch_up=2)
        with freeze_time(NOW):
            schedule_all()

        runs = PipelineRun.query.filter(PipelineRun.pipeline_uuid == pipeline.uuid).all()
        self.assertEqual(sorted(r.partition for r in runs), ['2026-11-05', '2026-11-06'])
