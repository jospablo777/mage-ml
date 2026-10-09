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



class WorkerPoolTests(TestCase):
    def run_pool(self, jobs, size):
        """
        Runs the pool with fake workers that stay alive for two pool loops; returns the
        jobs started and the pool size at each loop.
        """
        import queue as queue_module

        from mage_ai.orchestration.queue import process_queue

        started = []

        class FakeWorker:
            def __init__(self, job, job_dict):
                self.job = job
                self.checks = 0

            def start(self):
                started.append(self.job[0])

            def is_alive(self):
                self.checks += 1
                return self.checks <= 2

        queue = queue_module.Queue()
        for job_id in jobs:
            queue.put([job_id, run_block, (), {}])
        with patch.object(process_queue, 'Worker', FakeWorker), \
                patch.object(process_queue.time, 'sleep'), \
                patch('builtins.print') as printed:
            process_queue.poll_job_and_execute(queue, size, {}, None, None)
        sizes = [
            int(call.args[0].rsplit(': ', 1)[1]) for call in printed.call_args_list
            if 'Worker pool size' in str(call.args[0])
        ]
        return started, sizes

    def test_each_job_starts_one_worker(self):
        """Workers took jobs only after they started, so a job started `size` of them."""
        started, _ = self.run_pool(['a'], size=20)

        self.assertEqual(started, ['a'])

    def test_the_pool_runs_every_job_with_at_most_size_workers(self):
        jobs = [f'job_{i}' for i in range(5)]

        started, sizes = self.run_pool(jobs, size=2)

        self.assertEqual(started, jobs)
        self.assertEqual(max(sizes), 2)


class JobsFinishedTests(TestCase):
    def setUp(self):
        self.queue = ProcessQueue(queue_config=QueueConfig.load(config=dict(concurrency=2)))
        self.queue.start()

    def test_a_completed_job_counts_once_it_is_cleaned_up(self):
        self.assertFalse(self.queue.jobs_finished())
        self.queue.job_dict['block_run_1'] = JobStatus.COMPLETED
        self.assertTrue(self.queue.jobs_finished())

        # Removed by clean_up_jobs, as at the end of a scheduler run: the next wait
        # still starts early, since that run may have missed it.
        self.queue.clean_up_jobs()
        self.assertNotIn('block_run_1', self.queue.job_dict)
        self.assertTrue(self.queue.jobs_finished())
        self.assertFalse(self.queue.jobs_finished())
