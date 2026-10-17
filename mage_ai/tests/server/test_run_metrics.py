from datetime import datetime, timedelta, timezone

from prometheus_client import CollectorRegistry, generate_latest

from mage_ai.data_preparation.models.triggers import ScheduleType
from mage_ai.orchestration.db.models.schedules import (
    BlockRun,
    PipelineRun,
    PipelineSchedule,
)
from mage_ai.server import run_metrics
from mage_ai.tests.base_test import DBTestCase


class RunMetricsTest(DBTestCase):
    def test_runs_of_the_project_are_exposed_from_the_database(self):
        """/metrics had server metrics only, no pipeline or block runs (upstream #5196)."""
        pipeline = self._testMethodName
        here = PipelineSchedule.create(
            name='here', pipeline_uuid=pipeline, schedule_type=ScheduleType.API,
            repo_path=self.repo_path,
        )
        elsewhere = PipelineSchedule.create(
            name='elsewhere', pipeline_uuid=pipeline, schedule_type=ScheduleType.API,
            repo_path='/another/project',
        )
        end = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
        for status, schedule in (
            (PipelineRun.PipelineRunStatus.COMPLETED, here),
            (PipelineRun.PipelineRunStatus.FAILED, here),
            (PipelineRun.PipelineRunStatus.COMPLETED, elsewhere),
        ):
            run = PipelineRun.create(
                pipeline_uuid=pipeline, pipeline_schedule_id=schedule.id,
                create_block_runs=False, status=status,
                started_at=end - timedelta(seconds=90), completed_at=end,
            )
            BlockRun.create(pipeline_run_id=run.id, block_uuid='load', status='completed')

        registry = CollectorRegistry()
        run_metrics.register(registry)
        text = generate_latest(registry).decode()

        self.assertIn(f'mage_pipeline_runs{{pipeline="{pipeline}",status="completed"}} 1.0', text)
        self.assertIn(f'mage_pipeline_runs{{pipeline="{pipeline}",status="failed"}} 1.0', text)
        self.assertIn(
            f'mage_block_runs{{block="load",pipeline="{pipeline}",status="completed"}} 2.0', text,
        )
        self.assertIn(
            f'mage_pipeline_run_last_duration_seconds{{pipeline="{pipeline}"}} 90.0', text,
        )
        self.assertEqual(
            registry.get_sample_value(
                'mage_pipeline_run_last_success_timestamp_seconds', {'pipeline': pipeline},
            ),
            end.timestamp(),
        )
