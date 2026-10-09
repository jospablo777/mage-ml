from unittest.mock import patch

from mage_ai.orchestration.queue.config import QueueConfig
from mage_ai.orchestration.queue.process_queue import JobStatus, ProcessQueue
from mage_ai.tests.base_test import TestCase


def run_block():
    print('test run block')


class ProcessQueueTests(TestCase):
    def setUp(self):
        queue_config = QueueConfig.load(config=dict(concurrency=100))
        self.queue = ProcessQueue(queue_config=queue_config)
        self.queue.start()

    def test_init(self):
        self.assertEqual(self.queue.size, 100)

    @patch('mage_ai.orchestration.queue.process_queue.psutil.pid_exists')
    def test_clean_up_jobs(self, mock_pid_exists):
        mock_pid_exists.return_value = True

        self.queue.job_dict['block_run_1'] = JobStatus.QUEUED
        self.queue.job_dict['block_run_2'] = 100
        self.queue.job_dict['block_run_3'] = JobStatus.COMPLETED
        self.queue.job_dict['block_run_4'] = JobStatus.CANCELLED
        self.queue.clean_up_jobs()
        # queue is empty, thus 'block_run_1' is not in queue
        self.assertFalse('block_run_1' in self.queue.job_dict)
        self.assertEqual(self.queue.job_dict['block_run_2'], 100)
        self.assertFalse('block_run_3' in self.queue.job_dict)
        self.assertFalse('block_run_4' in self.queue.job_dict)

    @patch.object(ProcessQueue, 'start_worker_pool')
    @patch('mage_ai.orchestration.queue.process_queue.psutil.pid_exists')
    def test_has_job(self, mock_pid_exists, mock_start_worker_pool):
        mock_start_worker_pool.return_value = None
        mock_pid_exists.return_value = True

        self.queue.job_dict['block_run_1'] = JobStatus.QUEUED
        self.queue.job_dict['block_run_2'] = 100
        self.queue.job_dict['block_run_3'] = JobStatus.COMPLETED
        self.queue.job_dict['block_run_4'] = JobStatus.CANCELLED

        # Queue is empty, thus return False
        self.assertFalse(self.queue.has_job('block_run_1'))
        # After enqueueing the job, the has_job method returns True
        self.queue.enqueue('block_run_1', run_block)
        self.assertTrue(self.queue.has_job('block_run_1'))

        self.assertTrue(self.queue.has_job('block_run_2'))
        self.assertFalse(self.queue.has_job('block_run_3'))
        self.assertFalse(self.queue.has_job('block_run_4'))
        self.assertFalse(self.queue.has_job('block_run_5'))

        # Process not exists
        mock_pid_exists.return_value = False
        self.assertTrue(self.queue.has_job('block_run_1'))
        self.assertFalse(self.queue.has_job('block_run_2'))


class ProcessQueueRaceTests(TestCase):
    """Races the scheduler soak test found in the process queue."""

    def setUp(self):
        self.queue = ProcessQueue(queue_config=QueueConfig.load(config=dict(concurrency=2)))
        self.queue.start()

    @patch.object(ProcessQueue, 'start_worker_pool')
    def test_a_job_enqueued_just_before_clean_up_is_kept(self, _):
        """
        Queue.empty() reported an empty queue right after put, and clean_up_jobs, which
        runs right after the scheduler enqueues, deleted the job.
        """
        with patch.object(self.queue.queue, 'empty', return_value=True):
            self.queue.enqueue('block_run_1', run_block)
            self.queue.clean_up_jobs()

            self.assertEqual(self.queue.job_dict['block_run_1'], JobStatus.QUEUED)
            self.assertTrue(self.queue.has_job('block_run_1'))

    @patch.object(ProcessQueue, 'start_worker_pool')
    def test_a_lost_queued_job_is_released_after_the_grace_period(self, _):
        with patch.object(self.queue.queue, 'empty', return_value=True):
            self.queue.enqueue('block_run_1', run_block)
            self.queue.queued_at['block_run_1'] -= 120

            self.assertFalse(self.queue.has_job('block_run_1'))

    @patch.object(ProcessQueue, 'start_worker_pool')
    def test_the_status_is_set_before_the_job_is_queued(self, _):
        """A worker that took the job before its status was set raised KeyError."""
        statuses = []
        with patch.object(
            self.queue.queue, 'put',
            side_effect=lambda item: statuses.append(self.queue.job_dict.get(item[0])),
        ):
            self.queue.enqueue('block_run_1', run_block)

        self.assertEqual(statuses, [JobStatus.QUEUED])

    def test_the_worker_pool_gets_picklable_arguments(self):
        """
        The worker pool received the Redis client, which holds locks. With spawn or
        forkserver its arguments are pickled, so the pool failed to start.
        """
        import pickle

        self.queue.redis_url = 'redis://localhost:6379/0'
        with patch('mage_ai.orchestration.queue.process_queue.mp.Process') as process:
            self.queue.start_worker_pool()

        args = process.call_args.kwargs['args']
        self.assertIn('redis://localhost:6379/0', args)
        pickle.dumps(args[2:])

    def test_clean_up_starts_a_worker_pool_for_waiting_jobs(self):
        """A job put while the pool exited waited for the next enqueue."""
        with patch.object(self.queue.queue, 'empty', return_value=False), \
                patch.object(ProcessQueue, 'is_worker_pool_alive', return_value=False), \
                patch.object(ProcessQueue, 'start_worker_pool') as start:
            self.queue.clean_up_jobs()

        start.assert_called_once()


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.expiries = {}

    def set(self, key, value, ex=None):
        self.values[key] = value
        self.expiries[key] = ex
        return True

    def get(self, key):
        return self.values.get(key)

    def delete(self, key):
        self.values.pop(key, None)
        self.expiries.pop(key, None)


class ProcessQueueLivenessTests(TestCase):
    """How scheduler processes that share Redis see each other's jobs."""

    def setUp(self):
        self.queue = ProcessQueue(queue_config=QueueConfig.load(config=dict(concurrency=2)))
        self.queue.redis_client = FakeRedis()
        self.queue.start()

    @patch.object(ProcessQueue, 'start_worker_pool')
    def test_liveness_expires_in_30_seconds(self, _):
        """It expired in 300, so a crashed scheduler's runs waited 5 minutes to run again."""
        from mage_ai.orchestration.queue import process_queue

        self.queue.enqueue('block_run_1', run_block)

        self.assertEqual(process_queue.LIVENESS_TIMEOUT_SECONDS, 30)
        self.assertEqual(self.queue.redis_client.expiries[self.queue.client_id], 30)

    def test_jobs_of_a_scheduler_that_died_are_not_running(self):
        redis = self.queue.redis_client
        job_key = f'{self.queue.redis_namespace}:block_run_1'
        redis.set(job_key, 'other_client')
        redis.set('other_client', '1', ex=30)
        self.assertTrue(self.queue.has_job('block_run_1'))

        # The other process died; its liveness key expired.
        redis.delete('other_client')

        self.assertFalse(self.queue.has_job('block_run_1'))

    def test_kill_requests_outlive_the_liveness_key(self):
        """The process that runs the job checks for a kill request on its next tick."""
        self.queue.kill_job('block_run_1')

        kill_key = f'{self.queue.redis_namespace}:kill_job_block_run_1'
        self.assertEqual(self.queue.redis_client.expiries[kill_key], 300)

    @patch.object(ProcessQueue, 'start_worker_pool')
    def test_stop_releases_the_liveness_key(self, _):
        self.queue.enqueue('block_run_1', run_block)

        self.queue.stop()

        self.assertIsNone(self.queue.redis_client.get(self.queue.client_id))
