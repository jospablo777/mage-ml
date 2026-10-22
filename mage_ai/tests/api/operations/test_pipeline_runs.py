"""
A trigger's runs list groups each run with its retries (runs of the same execution date).
A page past the last run failed (results[0] of no results).
"""
from datetime import datetime

from mage_ai.api.operations import constants
from mage_ai.data_preparation.models.triggers import ScheduleType
from mage_ai.orchestration.db.models.schedules import PipelineRun, PipelineSchedule
from mage_ai.tests.api.operations.test_base import BaseApiTestCase
from mage_ai.tests.factory import create_pipeline, create_user


class PipelineRunGroupingTests(BaseApiTestCase):
    async def runs(self, schedule, user, **meta):
        response = await self.build_operation(
            action=constants.LIST,
            resource='pipeline_runs',
            resource_parent='pipeline_schedules',
            resource_parent_id=schedule.id,
            meta=meta,
            user=user,
        ).execute()
        self.assertNotIn('error', response, response)
        return [run['id'] for run in response['pipeline_runs']]

    async def test_a_page_past_the_last_run_is_empty(self):
        user = create_user(email=self.faker.email(), roles=1)
        pipeline = create_pipeline(self.faker.unique.name(), self.repo_path)
        schedule = PipelineSchedule.create(
            name='grouping', pipeline_uuid=pipeline.uuid, schedule_type=ScheduleType.API,
        )
        dated = [
            PipelineRun.create(
                pipeline_schedule_id=schedule.id,
                pipeline_uuid=pipeline.uuid,
                execution_date=datetime(2026, 1, 1, hour),
            )
            for hour in (1, 2)
        ]
        undated = PipelineRun.create(
            pipeline_schedule_id=schedule.id, pipeline_uuid=pipeline.uuid,
        )
        undated.update(execution_date=None)

        listed = await self.runs(schedule, user)
        self.assertEqual(sorted(listed), sorted([r.id for r in dated] + [undated.id]))
        self.assertEqual(await self.runs(schedule, user, _limit=2, _offset=10), [])
