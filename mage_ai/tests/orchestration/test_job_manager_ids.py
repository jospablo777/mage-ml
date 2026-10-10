from unittest.mock import MagicMock

from mage_ai.orchestration.job_manager import JobManager, JobType
from mage_ai.tests.base_test import TestCase


class JobManagerIdsTest(TestCase):
    def test_each_job_is_found_and_killed_under_the_id_it_was_enqueued_with(self):
        manager = JobManager.__new__(JobManager)
        manager.queue = MagicMock()
        cases = [
            (JobType.BLOCK_RUN, 7, manager.has_block_run_job, (7,), manager.kill_block_run_job),
            (
                JobType.INTEGRATION_STREAM, '3_users', manager.has_integration_stream_job,
                (3, 'users'), lambda *a: manager.kill_integration_stream_job(3, 'users'),
            ),
            (
                JobType.PIPELINE_RUN, 3, manager.has_pipeline_run_job, (3,),
                manager.kill_pipeline_run_job,
            ),
        ]
        for job_type, uid, has, has_args, kill in cases:
            with self.subTest(job_type=job_type):
                manager.queue.reset_mock()
                manager.add_job(job_type, uid, print)
                enqueued = manager.queue.enqueue.call_args[0][0]
                has(*has_args)
                self.assertEqual(manager.queue.has_job.call_args[0][0], enqueued)
                kill(*has_args)
                self.assertEqual(manager.queue.kill_job.call_args[0][0], enqueued)
