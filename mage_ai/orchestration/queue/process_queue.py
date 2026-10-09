import multiprocessing as mp
import os
import queue as queue_module
import signal
import time
from multiprocessing import Manager
from typing import Callable, Dict

import newrelic.agent
import psutil
import sentry_sdk
from sentry_sdk import capture_exception

from mage_ai.orchestration.db.process import start_session_and_run
from mage_ai.orchestration.queue.config import QueueConfig
from mage_ai.orchestration.queue.queue import Queue
from mage_ai.services.newrelic import initialize_new_relic
from mage_ai.services.redis.redis import init_redis_client, redis_namespace
from mage_ai.settings import (
    HOSTNAME,
    REDIS_URL,
    SENTRY_DSN,
    SENTRY_SERVER_NAME,
    SENTRY_TRACES_SAMPLE_RATE,
    SERVER_LOGGING_FORMAT,
    SERVER_VERBOSITY,
)
from mage_ai.shared.enum import StrEnum
from mage_ai.shared.logger import set_logging_format

LIVENESS_TIMEOUT_SECONDS = 300
# Seconds the worker pool waits with no workers and an empty queue before it exits.
POOL_IDLE_CHECKS = 5
# Seconds a queued job counts as present while the queue looks empty.
QUEUED_GRACE_SECONDS = 60


class JobStatus(StrEnum):
    QUEUED = 'queued'
    RUNNING = 'running'  # Not used. The value for RUNNING job is process id.
    COMPLETED = 'completed'
    CANCELLED = 'cancelled'


class QueueStatus(StrEnum):
    ACTIVE = 'active'
    INACTIVE = 'inactive'


class ProcessQueue(Queue):
    def __init__(self, queue_config: QueueConfig):
        """
        A process-based implementation of a queue that allows enqueueing and executing jobs using
        multiprocessing.

        Args:
            queue_config (QueueConfig): The configuration for the process queue.

        Attributes:
            queue_config (QueueConfig): The configuration for the process queue.
            queue (mp.Queue): A multiprocessing queue for storing the jobs.
            size (int): The size of the worker pool (defaults to the number of CPUs).
            mp_manager (Manager): A multiprocessing manager for maintaining a shared dictionary for
                jobs.

        """
        self.status = QueueStatus.INACTIVE
        self.queue_config = queue_config
        self.process_queue_config = self.queue_config.process_queue_config
        self.queue = mp.Queue()
        self.size = queue_config.concurrency or os.cpu_count()
        self.mp_manager = Manager()
        self.job_dict = self.mp_manager.dict()

        # Initialize redis client to track jobs across multiple replicas
        if self.process_queue_config and self.process_queue_config.redis_url:
            redis_url = self.process_queue_config.redis_url
        elif REDIS_URL:
            redis_url = REDIS_URL
        else:
            redis_url = None

        self.redis_url = redis_url
        self.redis_client = init_redis_client(redis_url)

        self.client_id = f'HOST_{HOSTNAME}_PID_{os.getpid()}'
        self.redis_namespace = redis_namespace()

        self.worker_pool_proc = None
        # When each job was enqueued, in this process.
        self.queued_at = {}

    def clean_up_jobs(self):
        """
        1. Cleans up completed jobs from the job dictionary.
        2. Check whether there're jobs need to be killed.
        """
        job_ids = self.job_dict.keys()
        for job_id in job_ids:
            if job_id in self.job_dict:
                if not self.has_job(job_id):
                    del self.job_dict[job_id]
                    self.queued_at.pop(job_id, None)
                elif self.__should_kill_job(job_id):
                    self.kill_job(job_id)
        # The worker pool exits when it finds the queue empty. A job put while it was
        # exiting waited until the next enqueue started a pool.
        if not self.queue.empty() and not self.is_worker_pool_alive():
            self.start_worker_pool()

    def enqueue(self, job_id: str, target: Callable, *args, **kwargs):
        """
        Enqueues a job to be executed in the worker pool.

        Args:
            job_id (str): The ID of the job.
            target (Callable): The target function to execute.
            *args: Variable length argument list for the target function.
            **kwargs: Keyword arguments for the target function.

        """
        if self.status != QueueStatus.ACTIVE:
            self._print('Cannot enqueue a job to an inactive queue.')
            return
        if self.has_job(job_id):
            self._print(f'Job {job_id} exists. Skip enqueue.')
            return
        self._print(f'Enqueue job {job_id}')
        if self.redis_client:
            self.redis_client.set(self.__redis_key_job(job_id), self.client_id)
        if self.redis_client:
            self.redis_client.set(self.client_id, '1', ex=LIVENESS_TIMEOUT_SECONDS)
        # The status is set first: a worker that took the job before it was set raised
        # KeyError and the job was lost.
        self.job_dict[job_id] = JobStatus.QUEUED
        self.queued_at[job_id] = time.monotonic()
        self.queue.put([job_id, target, args, kwargs])
        if not self.is_worker_pool_alive():
            self.start_worker_pool()

    def has_job(self, job_id: str, logger=None, logging_tags: Dict = None) -> bool:
        """
        Checks if a job with the given ID exists in the queue or is currently being executed.

        Args:
            job_id (str): The ID of the job.

        Returns:
            bool: True if the job exists, False otherwise.

        """
        if self.redis_client:
            job_client_id = self.redis_client.get(self.__redis_key_job(job_id))
            if not job_client_id:
                return False
            if job_client_id != self.client_id and self.redis_client.get(job_client_id):
                return True
        job = self.job_dict.get(job_id)
        if job is None:
            return False
        if job == JobStatus.QUEUED:
            if not self.queue.empty():
                # Job is in queue
                return True
            # Queue.empty() reports an empty queue right after put, before the feeder
            # thread writes the job, and while a worker that took the job has not marked
            # it yet. clean_up_jobs runs right after the scheduler enqueues, so it deleted
            # every new job, the worker skipped it, and the block run never ran. A queued
            # job counts as present for a while; one lost after that is enqueued again.
            queued_at = self.queued_at.get(job_id)
            return queued_at is not None and (
                time.monotonic() - queued_at < QUEUED_GRACE_SECONDS
            )
        if isinstance(job, int):
            # Job is being processed
            if self.__is_process_alive(job):
                return True
            else:
                # Process is dead
                if logger is not None:
                    logger.info(
                        f'Process {job} is dead for job {job_id}',
                        **(logging_tags or dict()))
                return False
        # Return False if job is in other statuses
        return False

    def kill_job(self, job_id: str):
        """
        Cancels and kills a job with the given ID if it is running.

        Args:
            job_id (str): The ID of the job.

        """
        print(f'Kill job {job_id}, job_dict {self.job_dict}')
        job = self.job_dict.get(job_id)
        if not job:
            self.__set_kill_job(job_id)
            return
        if isinstance(job, int):
            if job == os.getpid():
                # Update the job status before the process is killed
                self.job_dict[job_id] = JobStatus.CANCELLED
            try:
                os.kill(job, signal.SIGKILL)
            except Exception as err:
                print(err)
        self.job_dict[job_id] = JobStatus.CANCELLED
        self.__unset_kill_job(job_id)

    def start_worker_pool(self):
        """
        Starts the worker pool by creating a new process for executing jobs.
        """
        self.worker_pool_proc = mp.Process(
            target=poll_job_and_execute,
            # The Redis URL, not the client: the worker pool process receives its
            # arguments pickled where processes start with spawn or forkserver (macOS,
            # Windows, Linux from Python 3.14), and a Redis client holds locks that cannot
            # be pickled. The pool failed to start and block runs stayed queued.
            args=[
                self.queue,
                self.size,
                self.job_dict,
                self.redis_url,
                self.client_id,
            ],
        )
        self.worker_pool_proc.start()

    def start(self):
        self.status = QueueStatus.ACTIVE

    def stop(self):
        """
        1. Stop enqueueing new jobs
        2. Clear the queue
        3. Kill all the running jobs
        """
        self.status = QueueStatus.INACTIVE
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
            except self.queue.Empty:
                break
        job_ids = self.job_dict.keys()
        for job_id in job_ids:
            if job_id in self.job_dict:
                if isinstance(self.job_dict.get(job_id), int):
                    self.kill_job(job_id)
                del self.job_dict[job_id]

    def is_worker_pool_alive(self) -> bool:
        """
        Checks if the worker pool process is alive.

        Returns:
            bool: True if the worker pool process is alive, False otherwise.

        """
        if self.worker_pool_proc is None:
            return False
        return self.worker_pool_proc.is_alive()

    def __is_process_alive(self, pid: int) -> bool:
        return psutil.pid_exists(pid)

    def __redis_key_job(self, job_id):
        return f'{self.redis_namespace}:{job_id}'

    def __redis_key_kill_job(self, job_id):
        return f'{self.redis_namespace}:kill_job_{job_id}'

    def __set_kill_job(self, job_id):
        if not self.redis_client:
            return
        return self.redis_client.set(
            self.__redis_key_kill_job(job_id),
            '1',
            ex=LIVENESS_TIMEOUT_SECONDS,
        )

    def __unset_kill_job(self, job_id):
        if not self.redis_client:
            return
        key = self.__redis_key_kill_job(job_id)
        if self.redis_client.get(key):
            self.redis_client.delete(key)

    def __should_kill_job(self, job_id):
        if not self.redis_client:
            return False
        value = self.redis_client.get(self.__redis_key_kill_job(job_id))
        return value is not None


class Worker(mp.Process):
    def __init__(
        self,
        queue: mp.Queue,
        job_dict,
    ):
        """
        A worker process for executing jobs from the process queue.

        Args:
            queue (mp.Queue): The multiprocessing queue from which jobs are fetched.
            job_dict: The shared job dictionary.

        Attributes:
            queue (mp.Queue): The multiprocessing queue from which jobs are fetched.
            job_dict: The shared job dictionary.
            dsn (str): The Sentry DSN for error reporting.

        """
        super().__init__()
        self.queue = queue
        self.job_dict = job_dict
        self.dsn = SENTRY_DSN
        if self.dsn:
            sentry_sdk.init(
                self.dsn,
                traces_sample_rate=SENTRY_TRACES_SAMPLE_RATE,
                server_name=SENTRY_SERVER_NAME,
            )
            import atexit
            atexit.register(lambda: sentry_sdk.flush(timeout=5))
        initialize_new_relic()

        set_logging_format(
            logging_format=SERVER_LOGGING_FORMAT,
            level=SERVER_VERBOSITY,
        )

    @newrelic.agent.background_task(name='worker-run', group='Task')
    def run(self):
        """
        The entry point for the worker process.

        Fetches a job from the queue, executes it, and updates the job status in the job dictionary.

        """
        if not self.queue.empty():
            try:
                args = self.queue.get(timeout=1)
            except queue_module.Empty:
                return
            job_id = args[0]
            print(f'Run worker for job {job_id}')
            # clean_up_jobs removes a queued job when the queue looks empty, which it is
            # once a worker has taken the job. The scheduler then enqueues the job again,
            # so this worker skips it.
            status = self.job_dict.get(job_id)
            if status != JobStatus.QUEUED:
                print(f'Skip job {job_id} with status {status}')
                return
            self.job_dict[job_id] = self.pid

            try:
                start_session_and_run(args[1], *args[2], **args[3])
            except Exception as e:
                if self.dsn:
                    capture_exception(e)
                raise
            finally:
                self.job_dict[job_id] = JobStatus.COMPLETED


def poll_job_and_execute(
    queue: mp.Queue,
    size: int,
    job_dict,
    redis_url,
    client_id: str,
):
    """
    Continuously polls the job queue and executes jobs in a worker pool.

    Args:
        queue: The multiprocessing queue from which jobs are fetched.
        size: The size of the worker pool.
        job_dict: The shared job dictionary.

    """
    pid = os.getpid()
    redis_client = init_redis_client(redis_url) if redis_url else None
    workers = []
    idle_checks = 0
    while True:
        workers = [w for w in workers if w.is_alive()]
        print(f'[Process {pid}] Worker pool size: {len(workers)}')
        if not workers and queue.empty():
            # A job just put may not be visible yet: Queue.put hands it to a feeder thread.
            idle_checks += 1
            if idle_checks >= POOL_IDLE_CHECKS:
                break
        else:
            idle_checks = 0
        while not queue.empty():
            if len(workers) >= size:
                break
            worker = Worker(queue, job_dict)
            worker.start()
            workers.append(worker)
        time.sleep(1)
        if redis_client and client_id:
            redis_client.set(client_id, '1', ex=LIVENESS_TIMEOUT_SECONDS)
