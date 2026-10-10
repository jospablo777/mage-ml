import multiprocessing as mp
import os
import queue as queue_module
import time
from dataclasses import dataclass
from multiprocessing.managers import SyncManager
from typing import Any, Callable, Dict, List, Optional

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
from mage_ai.shared.processes import exit_with_parent

# Seconds a scheduler process's liveness key lives; its worker pool renews it every
# second. Another process treats this one's jobs as running while the key exists, so a
# crashed scheduler's block runs are found and run again this long after it died. It was
# 300 seconds.
LIVENESS_TIMEOUT_SECONDS = int(os.getenv('MAGE_QUEUE_LIVENESS_SECONDS') or 30)
# Seconds a request to kill a job waits for the process that runs it, which checks on
# every scheduler tick.
KILL_REQUEST_SECONDS = 300
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
        # The manager is a process of its own; it ends with the process that owns the
        # queue, so a killed scheduler leaves no manager behind.
        self.mp_manager = SyncManager()
        self.mp_manager.start(exit_with_parent)
        self.job_dict = self.mp_manager.dict()
        # Whether clean_up_jobs removed a finished job since jobs_finished last ran.
        self.removed_finished_job = False

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
                    if self.job_dict.get(job_id) == JobStatus.COMPLETED:
                        self.removed_finished_job = True
                    del self.job_dict[job_id]
                    self.queued_at.pop(job_id, None)
                elif self.__should_kill_job(job_id):
                    self.kill_job(job_id)
        # The worker pool exits when it finds the queue empty. A job put while it was
        # exiting waited until the next enqueue started a pool.
        if not self.queue.empty() and not self.is_worker_pool_alive():
            self.start_worker_pool()

    def jobs_finished(self) -> bool:
        """
        Whether a job finished since the last call, so that the scheduler can start the
        block runs that waited for it.
        """
        finished = self.removed_finished_job
        self.removed_finished_job = False
        try:
            statuses = list(self.job_dict.values())
        except Exception:
            return finished
        return finished or JobStatus.COMPLETED in statuses

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
            _kill_process_tree(job)
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
        if self.redis_client:
            # Other processes see at once that this one's jobs stopped.
            self.redis_client.delete(self.client_id)
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
            ex=KILL_REQUEST_SECONDS,
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


def _kill_process_tree(pid: int) -> None:
    """
    Kills a job's process and the processes it started. Killing only the job's process
    left the processes a block started, such as R, running.
    """
    try:
        process = psutil.Process(pid)
        children = process.children(recursive=True)
    except psutil.Error as error:
        print(error)
        return
    for victim in children + [process]:
        try:
            # SIGKILL on POSIX, TerminateProcess on Windows.
            victim.kill()
        except psutil.Error as error:
            print(error)


@dataclass
class _CurrentJob:
    aliases: List[str]
    client_id: Optional[str]
    job_dict: Any
    job_id: str
    redis_client: Any


# The job this worker process runs; None outside workers.
_current_job: Optional[_CurrentJob] = None


def _redis_job_key(job_id: str) -> str:
    return f'{redis_namespace()}:{job_id}'


def _release_redis_job_key(redis_client, client_id: Optional[str], job_id: str) -> None:
    """
    Deletes the key that marks a job as this process's. Keys were never deleted, so a
    job that ran on a replica that is still alive counted as running there, and running
    the same block run again from another replica was skipped and left it queued.
    """
    if not redis_client or not client_id:
        return
    try:
        key = _redis_job_key(job_id)
        if redis_client.get(key) == client_id:
            redis_client.delete(key)
    except Exception as error:
        print(f'[WARNING] Could not release the Redis key of job {job_id}: {error}')


def register_job_alias(job_id: str) -> None:
    """
    Makes job_id point at the job this worker runs: checks for it see the job alive, and
    killing it kills this process. A stage that runs several block runs registers each
    one when it starts it, so crash detection, timeouts and cancellation work on them as
    on any block run job. Outside a worker it does nothing.
    """
    current = _current_job
    if current is None or job_id == current.job_id:
        return
    current.job_dict[job_id] = os.getpid()
    if current.redis_client and current.client_id:
        current.redis_client.set(_redis_job_key(job_id), current.client_id)
    current.aliases.append(job_id)


class Worker(mp.Process):
    def __init__(
        self,
        job: List,
        job_dict,
        redis_url: str = None,
        client_id: str = None,
    ):
        """
        A worker process that runs one job of the process queue.

        Args:
            job (List): The job, as queued: [job_id, target, args, kwargs].
            job_dict: The shared job dictionary.

        Attributes:
            job (List): The job.
            job_dict: The shared job dictionary.
            dsn (str): The Sentry DSN for error reporting.

        """
        super().__init__()
        self.job = job
        self.job_dict = job_dict
        self.redis_url = redis_url
        self.client_id = client_id
        self.dsn = SENTRY_DSN
        if self.dsn:
            sentry_sdk.init(
                self.dsn,
                traces_sample_rate=SENTRY_TRACES_SAMPLE_RATE,
                server_name=SENTRY_SERVER_NAME,
                # Frames of block code hold DataFrames; their values would be sent.
                include_local_variables=False,
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

        Executes the job and updates the job status in the job dictionary.

        """
        global _current_job
        job_id, target, args, kwargs = self.job
        print(f'Run worker for job {job_id}')
        # clean_up_jobs removes a queued job when the queue looks empty, which it is
        # once the pool has taken the job. The scheduler then enqueues the job again,
        # so this worker skips it. A stopped queue or a killed job also leave another
        # status.
        status = self.job_dict.get(job_id)
        if status != JobStatus.QUEUED:
            print(f'Skip job {job_id} with status {status}')
            return
        self.job_dict[job_id] = self.pid
        exit_with_parent()
        redis_client = init_redis_client(self.redis_url) if self.redis_url else None
        _current_job = _CurrentJob(
            aliases=[],
            client_id=self.client_id,
            job_dict=self.job_dict,
            job_id=job_id,
            redis_client=redis_client,
        )

        try:
            start_session_and_run(target, *args, **kwargs)
        except Exception as e:
            if self.dsn:
                capture_exception(e)
            raise
        finally:
            for finished in [job_id] + _current_job.aliases:
                self.job_dict[finished] = JobStatus.COMPLETED
                _release_redis_job_key(redis_client, self.client_id, finished)
            _current_job = None


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
    # The pool and its workers end with the scheduler that started them.
    exit_with_parent()
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
        # The pool takes each job and starts one worker for it. Workers used to take
        # jobs themselves: the pool started a worker while the queue was not empty, and
        # a worker took a job only after it started, seconds later where processes
        # start with spawn (macOS, Linux from Python 3.14). Each job started up to
        # `size` processes that loaded Mage and exited, 20 by default.
        while len(workers) < size:
            try:
                job = queue.get_nowait()
            except queue_module.Empty:
                break
            worker = Worker(job, job_dict, redis_url=redis_url, client_id=client_id)
            worker.start()
            workers.append(worker)
        time.sleep(1)
        if redis_client and client_id:
            redis_client.set(client_id, '1', ex=LIVENESS_TIMEOUT_SECONDS)
