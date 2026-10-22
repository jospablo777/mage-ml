from datetime import datetime

from mage_ai.data_preparation.models.triggers import (
    ScheduleInterval,
    ScheduleStatus,
    ScheduleType,
    Trigger,
    add_or_update_trigger_for_pipeline_and_persist,
    get_trigger_configs_by_name,
)
from mage_ai.orchestration.db.models.schedules import PipelineSchedule
from mage_ai.settings.repo import get_repo_path
from mage_ai.tests.api.operations.test_base import BaseApiTestCase
from mage_ai.tests.factory import create_pipeline, create_user


class PipelineScheduleOperationTests(BaseApiTestCase):
    model_class = PipelineSchedule

    @property
    def model_class_name(self) -> str:
        return 'pipeline_schedule'

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        self.pipeline = create_pipeline('test pipeline', self.repo_path)

    async def test_execute_create_schedule(self):
        email = self.faker.email()
        user = create_user(email=email, roles=1)
        operation = self.build_create_operation(
            dict(
                name='test_schedule',
                schedule_interval='@daily',
                schedule_type=ScheduleType.TIME,
                start_time='2023-01-01T00:00:00',
                status='inactive',
                variables=[],
            ),
            resource_parent='pipeline',
            resource_parent_id=self.pipeline.uuid,
            user=user,
        )
        response = await operation.execute()

        self.assertEqual(response['pipeline_schedule']['name'], 'test_schedule')
        self.assertEqual(response['pipeline_schedule']['repo_path'], get_repo_path())

    async def test_execute_create_schedule_with_tags(self):
        user = create_user(email=self.faker.email(), roles=1)
        operation = self.build_create_operation(
            dict(
                name='tagged_schedule',
                schedule_interval='@daily',
                schedule_type=ScheduleType.TIME,
                start_time='2023-01-01T00:00:00',
                status='inactive',
                tags=['finance', 'daily'],
                variables=[],
            ),
            resource_parent='pipeline',
            resource_parent_id=self.pipeline.uuid,
            user=user,
        )
        response = await operation.execute()

        self.assertIsNone(response.get('error'), response.get('error'))
        self.assertEqual(response['pipeline_schedule']['name'], 'tagged_schedule')
        schedule = PipelineSchedule.query.filter(
            PipelineSchedule.id == response['pipeline_schedule']['id'],
        ).first()
        self.assertEqual(sorted(t.name for t in schedule.tag_associations), ['daily', 'finance'])

    async def test_execute_update_schedule_saved_in_code(self):
        email = self.faker.email()
        user = create_user(email=email, roles=1)
        pipeline_schedule = PipelineSchedule.create(
            name='test_schedule_2',
            pipeline_uuid='test_pipeline',
            schedule_interval=ScheduleInterval.DAILY,
            schedule_type=ScheduleType.TIME,
            start_time=datetime(2023, 8, 30, 12, 30, 45),
            status=ScheduleStatus.INACTIVE,
        )
        trigger = Trigger(
            name=pipeline_schedule.name,
            pipeline_uuid=pipeline_schedule.pipeline_uuid,
            schedule_interval=pipeline_schedule.schedule_interval,
            schedule_type=pipeline_schedule.schedule_type,
            start_time=pipeline_schedule.start_time,
        )
        trigger_configs_by_name = add_or_update_trigger_for_pipeline_and_persist(
            trigger,
            pipeline_schedule.pipeline_uuid,
        )

        self.assertEqual(
            trigger_configs_by_name['test_schedule_2'].get('schedule_interval'),
            ScheduleInterval.DAILY,
        )
        self.assertEqual(
            trigger_configs_by_name['test_schedule_2'].get('status'),
            ScheduleStatus.INACTIVE,
        )

        update_payload = dict(
            schedule_interval='@hourly',
            status='active',
        )
        operation = self.build_update_operation(
            pipeline_schedule.id,
            {
                'pipeline_schedule': update_payload,
            },
            user=user,
        )
        response = await operation.execute()

        self.assertEqual(response['pipeline_schedule']['status'], ScheduleStatus.ACTIVE)
        self.assertEqual(
            response['pipeline_schedule']['schedule_interval'],
            ScheduleInterval.HOURLY,
        )

        # Check if trigger in triggers.yaml config file was also updated
        updated_trigger_configs_by_name = get_trigger_configs_by_name('test_pipeline')
        self.assertEqual(
            updated_trigger_configs_by_name['test_schedule_2']['schedule_interval'],
            ScheduleInterval.HOURLY,
        )
        self.assertEqual(
            updated_trigger_configs_by_name['test_schedule_2']['status'],
            ScheduleStatus.ACTIVE,
        )

    async def test_execute_list(self):
        email = self.faker.email()
        user = create_user(email=email, roles=1)

        await self.base_test_execute_list(
            [
                dict(
                    name='test_schedule',
                    schedule_interval='@daily',
                    schedule_type=ScheduleType.TIME,
                    start_time='2023-01-01T00:00:00',
                    status='inactive',
                    variables=[],
                    repo_path=None,
                ),
                dict(
                    name='test_schedule_2',
                    schedule_interval='@daily',
                    schedule_type=ScheduleType.TIME,
                    start_time='2023-01-01T00:00:00',
                    status='inactive',
                    variables=[],
                ),
            ],
            [
                'id',
            ],
            resource_parent='pipeline',
            resource_parent_id=self.pipeline.uuid,
            user=user,
        )

    async def test_list_tags_sorted_and_ties_in_id_order(self):
        """
        Pages are slices of the list, so its order holds from one request to the next: ties
        on the order asked for, and no order at all, go by id. Tags come sorted, as in the
        detail. And a user who is not an owner may search, which the policy refused.
        """
        from mage_ai.api.resources.PipelineScheduleResource import (
            PipelineScheduleResource,
        )

        user = create_user(email=self.faker.email(), roles=1)
        name = f'tie {self.faker.uuid4()}'
        schedules = [
            PipelineSchedule.create(
                name=name,
                pipeline_uuid=self.pipeline.uuid,
                repo_path=get_repo_path(),
                schedule_type=ScheduleType.API,
            )
            for _ in range(3)
        ]
        PipelineScheduleResource(schedules[0], user).update(dict(tags=['zeta', 'alpha', 'mid']))

        for order_by in [None, 'name', 'status']:
            with self.subTest(order_by=order_by):
                query = dict(search=[name])
                if order_by:
                    query['order_by'] = [order_by]
                response = await self.build_list_operation(query=query, user=user).execute()
                self.assertNotIn('error', response, response)
                found = response['pipeline_schedules']
                self.assertEqual([s['id'] for s in found], [s.id for s in schedules])
                self.assertEqual(found[0]['tags'], ['alpha', 'mid', 'zeta'])
