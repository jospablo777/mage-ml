"""
The scheduler's job queue on the Rust run engine (docs/development/run-engine.md).

Jobs run in warm Python workers held by `mage-service jobs`, a child of this process,
instead of a new process each: a job starts as soon as it is enqueued and a worker is
free, without starting Python or importing MageML. The scheduler sees the same queue as
the process queue: has_job, kill_job and jobs_finished behave the same, and with Redis the
job keys and this process's liveness key are the process queue's, so replicas and crash
detection work as before.

It runs the jobs when an engine is installed (queue_factory.py); MAGE_JOB_ENGINE=off runs
them in the process queue, and without an engine that has the `jobs` command the process
queue runs them too.
"""
import base64
import json
import os
import pickle
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union

from mage_ai.orchestration.queue.config import QueueConfig
from mage_ai.orchestration.queue.process_queue import (
    KILL_REQUEST_SECONDS,
    LIVENESS_TIMEOUT_SECONDS,
    JobStatus,
    QueueStatus,
    sweep_dead_job_keys,
)
from mage_ai.orchestration.queue.queue import Queue
from mage_ai.services.redis.redis import init_redis_client, redis_namespace
from mage_ai.settings import HOSTNAME, REDIS_URL

# Seconds the engine may take to start and say it is ready.
READY_SECONDS = 15
# Workers kept started and idle.
WARM_WORKERS = int(os.getenv('MAGE_JOBS_WARM') or 2)
SWEEP_SECONDS = 300


class EngineUnavailable(RuntimeError):
    pass


def engine_supports_jobs(path: Path) -> bool:
    """Whether this engine has the `jobs` command (mageml-engine 0.1.0 does not)."""
    try:
        result = subprocess.run(
            [str(path), 'jobs', '--help'], capture_output=True, text=True, timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return result.returncode == 0


class EngineQueue(Queue):
    def __init__(self, queue_config: QueueConfig, engine: Path):
        self.status = QueueStatus.INACTIVE
        self.queue_config = queue_config
        self.engine = engine
        self.size = max(1, queue_config.concurrency or os.cpu_count() or 1)
        self.lock = threading.RLock()
        self.process: Optional[subprocess.Popen] = None
        # Job id to JobStatus.QUEUED, the pid of the worker running it, or a final status.
        self.jobs: Dict[str, Union[str, int]] = {}
        # Block run job ids a stage runs, to the stage's job id.
        self.aliases: Dict[str, str] = {}
        self.queued_at: Dict[str, float] = {}
        self.finished = False
        self.broken: Optional[str] = None

        process_queue_config = queue_config.process_queue_config
        redis_url = (process_queue_config.redis_url if process_queue_config else None) or REDIS_URL
        self.redis_url = redis_url or None
        self.redis_client = init_redis_client(self.redis_url)
        self.client_id = f'HOST_{HOSTNAME}_PID_{os.getpid()}'
        self.redis_namespace = redis_namespace()
        self.last_sweep = None
        self.liveness: Optional[threading.Thread] = None

    # ---- the engine process -------------------------------------------------------

    def _engine_alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def _start_engine(self) -> None:
        ready = threading.Event()
        command = [
            str(self.engine), 'jobs',
            '--python', sys.executable,
            '--workers', str(self.size),
            '--warm', str(min(WARM_WORKERS, self.size)),
        ]
        # The workers' output and the engine's log go where this process's stderr goes.
        process = subprocess.Popen(
            command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1,
        )
        self.process = process
        self.broken = None
        reader = threading.Thread(
            target=self._read_events, args=(process, ready), name='engine-queue-events',
            daemon=True,
        )
        reader.start()
        if not ready.wait(READY_SECONDS):
            process.kill()
            raise EngineUnavailable(f'{self.engine} jobs did not start in {READY_SECONDS} s')
        self._print(f'Jobs run on the Rust run engine ({self.engine}, pid {process.pid}).')
        if self.redis_client and (self.liveness is None or not self.liveness.is_alive()):
            self.liveness = threading.Thread(
                target=self._renew_liveness, name='engine-queue-liveness', daemon=True,
            )
            self.liveness.start()

    def _ensure_engine(self) -> None:
        with self.lock:
            if not self._engine_alive():
                self._start_engine()

    def _send(self, request: Dict) -> None:
        with self.lock:
            self._ensure_engine()
            try:
                self.process.stdin.write(json.dumps(request) + '\n')
                self.process.stdin.flush()
            except (BrokenPipeError, OSError, ValueError):
                # The engine ended since it was checked: start another and send again.
                self._forget_running('the run engine stopped')
                self._start_engine()
                self.process.stdin.write(json.dumps(request) + '\n')
                self.process.stdin.flush()

    def _read_events(self, process: subprocess.Popen, ready: threading.Event) -> None:
        for line in process.stdout:
            try:
                event = json.loads(line)
            except ValueError:
                continue
            kind = event.get('event')
            job_id = event.get('job_id')
            if kind == 'ready':
                # Not under the lock: the thread that starts the engine holds it while it
                # waits for this.
                ready.set()
                continue
            with self.lock:
                if kind == 'started' and job_id in self.jobs:
                    self.jobs[job_id] = int(event['pid'])
                elif kind == 'alias' and job_id:
                    alias = event.get('alias')
                    owner = self.jobs.get(job_id)
                    if alias and isinstance(owner, int):
                        self.aliases[alias] = job_id
                        self.jobs[alias] = owner
                elif kind == 'finished' and job_id:
                    if self.jobs.get(job_id) != JobStatus.CANCELLED:
                        self.jobs[job_id] = JobStatus.COMPLETED
                    for alias, owner in list(self.aliases.items()):
                        if owner == job_id:
                            self.jobs[alias] = JobStatus.COMPLETED
                            del self.aliases[alias]
                    self.queued_at.pop(job_id, None)
                    self.finished = True
                    if not event.get('ok') and event.get('error'):
                        self._print(f'Job {job_id} ended: {event["error"]}')
                elif kind == 'broken':
                    self.broken = event.get('error') or 'the job workers cannot start'
                    self._print(f'The run engine cannot start job workers: {self.broken}')
        # The engine ended: its jobs ended with it.
        with self.lock:
            if self.process is process:
                self._forget_running('the run engine stopped')

    def _forget_running(self, reason: str) -> None:
        """Running and queued jobs of an engine that ended: no longer present."""
        lost = [j for j, s in self.jobs.items() if s == JobStatus.QUEUED or isinstance(s, int)]
        for job_id in lost:
            self.jobs[job_id] = JobStatus.COMPLETED
        self.aliases.clear()
        if lost:
            self.finished = True
            self._print(f'{reason}; {len(lost)} job(s) ended with it.')

    def _renew_liveness(self) -> None:
        while self.status == QueueStatus.ACTIVE and self._engine_alive():
            try:
                self.redis_client.set(self.client_id, '1', ex=LIVENESS_TIMEOUT_SECONDS)
            except Exception as error:
                print(f'[WARNING] Could not renew the liveness key: {error}')
            time.sleep(min(5, LIVENESS_TIMEOUT_SECONDS / 3))

    # ---- the queue ----------------------------------------------------------------

    def enqueue(self, job_id: str, target: Callable, *args, **kwargs):
        if self.status != QueueStatus.ACTIVE:
            self._print('Cannot enqueue a job to an inactive queue.')
            return
        if self.has_job(job_id):
            self._print(f'Job {job_id} exists. Skip enqueue.')
            return
        self._print(f'Enqueue job {job_id}')
        if self.redis_client:
            self.redis_client.set(self._redis_key_job(job_id), self.client_id)
            self.redis_client.set(self.client_id, '1', ex=LIVENESS_TIMEOUT_SECONDS)
        payload = dict(
            pickle=base64.b64encode(pickle.dumps((target, args, kwargs))).decode(),
            client_id=self.client_id,
            redis_url=self.redis_url,
        )
        with self.lock:
            self.jobs[job_id] = JobStatus.QUEUED
            self.queued_at[job_id] = time.monotonic()
            self._send(dict(op='run', job_id=job_id, payload=payload))

    def has_job(self, job_id: str, logger=None, logging_tags: Dict = None) -> bool:
        if self.redis_client:
            owner = self.redis_client.get(self._redis_key_job(job_id))
            if not owner:
                return False
            if owner != self.client_id and self.redis_client.get(owner):
                return True
        with self.lock:
            state = self.jobs.get(job_id)
            if state is None:
                return False
            if state == JobStatus.QUEUED or isinstance(state, int):
                if self._engine_alive():
                    return True
                if logger is not None:
                    logger.info(f'The run engine stopped with job {job_id}',
                                **(logging_tags or dict()))
                return False
            return False

    def kill_job(self, job_id: str):
        print(f'Kill job {job_id}')
        with self.lock:
            owner = self.aliases.get(job_id, job_id)
            state = self.jobs.get(owner)
            if state is None:
                self._set_kill_job(job_id)
                return
            if (state == JobStatus.QUEUED or isinstance(state, int)) and self._engine_alive():
                self._send(dict(op='kill', job_id=owner))
            self.jobs[owner] = JobStatus.CANCELLED
            if job_id != owner:
                self.jobs[job_id] = JobStatus.CANCELLED
        self._unset_kill_job(job_id)

    def clean_up_jobs(self):
        with self.lock:
            for job_id in list(self.jobs):
                state = self.jobs.get(job_id)
                if state in (JobStatus.COMPLETED, JobStatus.CANCELLED):
                    del self.jobs[job_id]
                    self.queued_at.pop(job_id, None)
                elif self._should_kill_job(job_id):
                    self.kill_job(job_id)
        if self.redis_client and (
            self.last_sweep is None or time.monotonic() - self.last_sweep >= SWEEP_SECONDS
        ):
            self.last_sweep = time.monotonic()
            sweep_dead_job_keys(self.redis_client, self.client_id)

    def jobs_finished(self) -> bool:
        with self.lock:
            finished, self.finished = self.finished, False
        return finished

    def start(self):
        self.status = QueueStatus.ACTIVE
        self._ensure_engine()

    def stop(self):
        self.status = QueueStatus.INACTIVE
        if self.redis_client:
            self.redis_client.delete(self.client_id)
        with self.lock:
            process, self.process = self.process, None
            self.jobs.clear()
            self.aliases.clear()
        if process is not None and process.poll() is None:
            try:
                process.stdin.write(json.dumps(dict(op='shutdown')) + '\n')
                process.stdin.close()
            except (BrokenPipeError, OSError, ValueError):
                pass
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()

    def running_jobs(self) -> List[str]:
        with self.lock:
            return [j for j, s in self.jobs.items() if isinstance(s, int)]

    # ---- Redis --------------------------------------------------------------------

    def _redis_key_job(self, job_id: str) -> str:
        return f'{self.redis_namespace}:{job_id}'

    def _redis_key_kill_job(self, job_id: str) -> str:
        return f'{self.redis_namespace}:kill_job_{job_id}'

    def _set_kill_job(self, job_id: str) -> None:
        if self.redis_client:
            self.redis_client.set(self._redis_key_kill_job(job_id), '1', ex=KILL_REQUEST_SECONDS)

    def _unset_kill_job(self, job_id: str) -> None:
        if self.redis_client:
            self.redis_client.delete(self._redis_key_kill_job(job_id))

    def _should_kill_job(self, job_id: str) -> bool:
        if not self.redis_client:
            return False
        return self.redis_client.get(self._redis_key_kill_job(job_id)) is not None
