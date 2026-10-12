"""
After-run triggers in MageML: a pipeline starts when a run of another ends. The rules for
which endings start it and which triggers are refused, the scheduler creating one run per
upstream run when a run completes or fails, and the API refusing loops.
"""
import asyncio
import unittest
from unittest.mock import patch

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.triggers import ScheduleStatus, ScheduleType
from mage_ai.orchestration import after_runs
from mage_ai.orchestration.db.models.schedules import (
    BlockRun,
    PipelineRun,
    PipelineSchedule,
)
from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
from mage_ai.tests.base_test import DBTestCase
from mage_ai.tests.factory import create_pipeline_with_blocks, create_user


class AfterRunRulesTest(unittest.TestCase):
    def test_a_trigger_follows_the_endings_it_names(self):
        completed = dict(after_pipeline='load')
        failed = dict(after_pipeline='load', after_status='failed')
        either = dict(after_pipeline='load', after_status='any')

        self.assertTrue(after_runs.follows(completed, 'load', 'completed'))
        self.assertFalse(after_runs.follows(completed, 'load', 'failed'))
        self.assertFalse(after_runs.follows(completed, 'other', 'completed'))
        self.assertTrue(after_runs.follows(failed, 'load', 'failed'))
        self.assertTrue(after_runs.follows(either, 'load', 'failed'))
        self.assertTrue(after_runs.follows(either, 'load', 'completed'))
        self.assertFalse(after_runs.follows(either, 'load', 'cancelled'))
        self.assertEqual(after_runs.describe(completed), 'after load completes')
        self.assertEqual(after_runs.describe(either), 'after load ends')

    def test_problems_name_what_to_fix(self):
        exists = {'load', 'score', 'report'}.__contains__

        self.assertIn('Choose the pipeline', after_runs.trigger_problems('score', {})[0])
        self.assertIn('its own runs', after_runs.trigger_problems(
            'score', dict(after_pipeline='score'), pipeline_exists=exists)[0])
        self.assertIn('does not exist', after_runs.trigger_problems(
            'score', dict(after_pipeline='nope'), pipeline_exists=exists)[0])
        self.assertIn('is not completed, failed or any', after_runs.trigger_problems(
            'score', dict(after_pipeline='load', after_status='later'), pipeline_exists=exists)[0])

        loop = after_runs.trigger_problems(
            'load',
            dict(after_pipeline='report'),
            [('score', dict(after_pipeline='load')), ('report', dict(after_pipeline='score'))],
            pipeline_exists=exists,
        )
        self.assertEqual(len(loop), 1)
        self.assertIn('report -> load -> score -> report', loop[0])
        self.assertEqual(after_runs.trigger_problems(
            'report', dict(after_pipeline='score'),
            [('score', dict(after_pipeline='load'))], pipeline_exists=exists,
        ), [])


class AfterRunSchedulerTest(DBTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.upstream = create_pipeline_with_blocks('after run upstream', cls.repo_path)
        cls.follower = create_pipeline_with_blocks('after run follower', cls.repo_path)

    def setUp(self):
        super().setUp()
        for schedule in PipelineSchedule.query.filter(
            PipelineSchedule.schedule_type == ScheduleType.AFTER_RUN,
        ).all():
            schedule.update(status=ScheduleStatus.INACTIVE)

    def after_run(self, pipeline_uuid=None, **settings):
        schedule = PipelineSchedule.create(
            name=f'after_{self.faker.unique.word()}',
            pipeline_uuid=pipeline_uuid or self.follower.uuid,
            repo_path=self.repo_path,
            schedule_type=ScheduleType.AFTER_RUN,
            settings=dict(after_pipeline=self.upstream.uuid, **settings),
            status=ScheduleStatus.ACTIVE,
        )
        self.addCleanup(lambda: schedule.update(status=ScheduleStatus.INACTIVE))
        return schedule

    def finished_run(self, status=PipelineRun.PipelineRunStatus.COMPLETED):
        run = PipelineRun.create(pipeline_uuid=self.upstream.uuid)
        run.update(status=status)
        return run

    def runs_of(self, schedule):
        return PipelineRun.query.filter(PipelineRun.pipeline_schedule_id == schedule.id).all()

    def test_a_completed_run_starts_one_run_of_the_follower(self):
        schedule = self.after_run()
        upstream = self.finished_run()

        started = after_runs.start_followers(upstream, 'completed')
        again = after_runs.start_followers(upstream, 'completed')

        self.assertEqual(len(started), 1)
        self.assertEqual(again, [])
        [run] = self.runs_of(schedule)
        self.assertEqual(run.pipeline_uuid, self.follower.uuid)
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.INITIAL)
        self.assertEqual(run.variables['upstream_pipeline_run_id'], upstream.id)
        self.assertEqual(run.variables['upstream_status'], 'completed')

    def test_only_the_endings_and_active_triggers_named_start_runs(self):
        on_failure = self.after_run(after_status='failed')
        inactive = self.after_run()
        inactive.update(status=ScheduleStatus.INACTIVE)

        after_runs.start_followers(self.finished_run(), 'completed')
        self.assertEqual(self.runs_of(on_failure), [])
        after_runs.start_followers(
            self.finished_run(PipelineRun.PipelineRunStatus.FAILED), 'failed',
        )
        self.assertEqual(len(self.runs_of(on_failure)), 1)
        self.assertEqual(self.runs_of(inactive), [])

    def test_a_trigger_of_another_environment_starts_no_run(self):
        """
        envs in triggers.yaml kept a trigger out of the sync, but a trigger synced before
        its envs changed stayed active in the database and started runs everywhere.
        """
        import os

        import yaml

        schedule = self.after_run()
        path = os.path.join(self.follower.dir_path, 'triggers.yaml')
        with open(path, 'w') as file:
            yaml.safe_dump(dict(triggers=[dict(
                name=schedule.name,
                schedule_type='after_run',
                settings=dict(after_pipeline=self.upstream.uuid),
                status='active',
                envs=['prod'],
            )]), file)
        self.addCleanup(os.remove, path)

        after_runs.start_followers(self.finished_run(), 'completed')

        self.assertEqual(self.runs_of(schedule), [])

    def test_a_running_follower_is_skipped_when_asked(self):
        schedule = self.after_run(skip_if_previous_running=True)
        after_runs.start_followers(self.finished_run(), 'completed')
        after_runs.start_followers(self.finished_run(), 'completed')

        self.assertEqual(len(self.runs_of(schedule)), 1)

    def test_triggers_that_loop_start_nothing(self):
        # Saved straight in triggers.yaml, a loop skips the API's checks.
        first = self.after_run()
        PipelineSchedule.create(
            name='back_to_upstream',
            pipeline_uuid=self.upstream.uuid,
            repo_path=self.repo_path,
            schedule_type=ScheduleType.AFTER_RUN,
            settings=dict(after_pipeline=self.follower.uuid),
            status=ScheduleStatus.ACTIVE,
        ).update(status=ScheduleStatus.ACTIVE)

        with self.assertLogs('mage_ai.orchestration.after_runs', level='ERROR'):
            after_runs.start_followers(self.finished_run(), 'completed')

        self.assertEqual(self.runs_of(first), [])
        for schedule in PipelineSchedule.query.filter(
            PipelineSchedule.name == 'back_to_upstream',
        ):
            schedule.update(status=ScheduleStatus.INACTIVE)

    def test_the_scheduler_starts_followers_when_a_run_completes_or_fails(self):
        on_success = self.after_run()
        on_failure = self.after_run(after_status='failed')

        completed = PipelineRun.create(pipeline_uuid=self.upstream.uuid)
        completed.update(status=PipelineRun.PipelineRunStatus.RUNNING)
        for block_run in completed.block_runs:
            block_run.update(status=BlockRun.BlockRunStatus.COMPLETED)
        scheduler = PipelineScheduler(pipeline_run=completed)
        with patch.object(scheduler.notification_sender, 'send_pipeline_run_success_message'):
            scheduler.schedule()
        self.assertEqual(completed.status, PipelineRun.PipelineRunStatus.COMPLETED)
        self.assertEqual(len(self.runs_of(on_success)), 1)
        self.assertEqual(self.runs_of(on_failure), [])

        failed = PipelineRun.create(pipeline_uuid=self.upstream.uuid)
        failed.update(status=PipelineRun.PipelineRunStatus.RUNNING)
        failed.block_runs[0].update(status=BlockRun.BlockRunStatus.FAILED)
        scheduler = PipelineScheduler(pipeline_run=failed)
        with patch.object(scheduler.notification_sender, 'send_pipeline_run_failure_message'):
            scheduler.schedule()
        self.assertEqual(failed.status, PipelineRun.PipelineRunStatus.FAILED)
        self.assertEqual(len(self.runs_of(on_failure)), 1)
        self.assertEqual(len(self.runs_of(on_success)), 1)

    def test_after_run_triggers_never_run_on_a_clock(self):
        self.assertFalse(self.after_run().should_schedule())

    def test_the_api_refuses_loops_and_triggers_that_follow_nothing(self):
        owner = create_user(_owner=True)
        self.after_run()

        def create(pipeline, settings):
            return asyncio.run(BaseOperation(
                action=OperationType.CREATE,
                payload=dict(pipeline_schedule=dict(
                    name=f'api_{self.faker.unique.word()}',
                    schedule_type='after_run',
                    settings=settings,
                    status='active',
                )),
                resource='pipeline_schedules',
                resource_parent='pipelines',
                resource_parent_id=pipeline.uuid,
                user=owner,
            ).execute())

        loop = create(self.upstream, dict(after_pipeline=self.follower.uuid))
        self.assertEqual(loop['error']['code'], 400)
        self.assertIn('in a loop', loop['error']['message'])
        nothing = create(self.follower, {})
        self.assertIn('Choose the pipeline', nothing['error']['message'])

        created = create(self.follower, dict(after_pipeline=self.upstream.uuid,
                                             after_status='any'))
        schedule = created['pipeline_schedule']
        self.assertEqual(schedule['schedule_type'], 'after_run')
        PipelineSchedule.get(schedule['id']).update(status=ScheduleStatus.INACTIVE)
