from mage_ai.data_preparation.models.triggers import ScheduleType
from mage_ai.orchestration.db.models.schedules import (
    BlockRun,
    PipelineRun,
    PipelineSchedule,
)
from mage_ai.orchestration.monitor.monitor_stats import MonitorStats, MonitorStatsType
from mage_ai.tests.base_test import DBTestCase


class MonitorStatsScopeTest(DBTestCase):
    def runs(self, repo_path, count):
        schedule = PipelineSchedule.create(
            name=f'trigger_{repo_path}', pipeline_uuid=self._testMethodName,
            schedule_type=ScheduleType.API, repo_path=repo_path,
        )
        for _ in range(count):
            run = PipelineRun.create(
                pipeline_uuid=self._testMethodName, pipeline_schedule_id=schedule.id,
                create_block_runs=False,
            )
            BlockRun.create(pipeline_run_id=run.id, block_uuid='load')
        return schedule

    def total(self, stats):
        return sum(
            sum(counts.values())
            for entry in stats.values()
            for counts in entry['data'].values()
        )

    def test_stats_count_only_this_projects_runs(self):
        """Projects sharing a database saw each other's runs (upstream #6036)."""
        self.runs(self.repo_path, 2)
        self.runs('/somewhere/else', 3)

        stats = MonitorStats()
        pipeline = self._testMethodName

        for stats_type in (MonitorStatsType.PIPELINE_RUN_COUNT, MonitorStatsType.BLOCK_RUN_COUNT):
            self.assertEqual(self.total(stats.get_stats(stats_type, pipeline_uuid=pipeline)), 2)

    def test_block_runs_of_one_trigger_are_counted_once(self):
        # Filtered by trigger without the join, each block run counted once per pipeline run.
        schedule = self.runs(self.repo_path, 3)

        stats = MonitorStats().get_stats(
            MonitorStatsType.BLOCK_RUN_COUNT, pipeline_schedule_id=schedule.id,
        )

        self.assertEqual(self.total(stats), 3)
