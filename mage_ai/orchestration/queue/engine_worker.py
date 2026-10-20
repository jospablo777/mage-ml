"""
A warm job worker of the Rust run engine (`mage-service jobs`, docs/development/run-engine.md).

It imports MageML once, says it is ready on file descriptor 3, and then runs the scheduler's
jobs one at a time as they arrive on file descriptor 4: each is a pickled call, the target
and arguments the process queue would run in a new process. What a job prints goes to the
engine's stderr, as a process queue worker's output goes to the scheduler's.

Between jobs it puts back what block code can change in a process (environment variables,
working directory, sys.path) and drops the project's own modules, so the next job reads
edited code. It asks to be replaced when a job leaves threads running or its memory grew
past MAGE_JOBS_MAX_RSS_MB (2048 by default): the engine starts a fresh worker instead.
"""
import base64
import json
import os
import pickle
import sys
import threading
import traceback
from typing import Dict, List, Optional

REPLY_FD = 3
REQUEST_FD = int(os.getenv('MAGE_WORKER_REQUEST_FD') or 4)
MAX_RSS_BYTES = int(os.getenv('MAGE_JOBS_MAX_RSS_MB') or 2048) * 1024 * 1024


class _Replies:
    """Writes one JSON object per line to the engine."""

    def __init__(self, fd: int):
        self.file = os.fdopen(fd, 'w', buffering=1)
        self.lock = threading.Lock()

    def send(self, message: Dict) -> None:
        line = json.dumps(message, default=str)
        with self.lock:
            self.file.write(line + '\n')
            self.file.flush()


class _AliasSink(dict):
    """
    The job dictionary a job sees in this worker: register_job_alias writes the pid of the
    process that runs a block run into it; here that becomes an event, so the scheduler
    finds the block run alive in this job.
    """

    def __init__(self, replies: _Replies):
        super().__init__()
        self.replies = replies

    def __setitem__(self, key, value) -> None:
        super().__setitem__(key, value)
        if isinstance(value, int) and not isinstance(value, bool):
            self.replies.send(dict(event='alias', alias=key))


class _ProcessState:
    """What block code can change in a process, taken before a job and put back after."""

    def __init__(self):
        self.environ = dict(os.environ)
        self.cwd = os.getcwd()
        self.path = list(sys.path)
        self.threads = set(threading.enumerate())

    def restore(self) -> None:
        if dict(os.environ) != self.environ:
            os.environ.clear()
            os.environ.update(self.environ)
        try:
            os.chdir(self.cwd)
        except OSError:
            pass
        sys.path[:] = self.path

    def threads_left(self) -> List[threading.Thread]:
        return [
            t for t in threading.enumerate()
            if t not in self.threads and t.is_alive() and not t.daemon
        ]


def _project_roots() -> List[str]:
    from mage_ai.settings.repo import get_repo_path

    roots = set()
    for root in (os.getenv('MAGE_REPO_PATH'), get_repo_path()):
        if root:
            roots.add(os.path.realpath(root) + os.sep)
    return sorted(roots)


def drop_project_modules(roots: Optional[List[str]] = None) -> List[str]:
    """Removes the modules loaded from the project's files; returns their names."""
    roots = roots if roots is not None else _project_roots()
    dropped = []
    for name, module in list(sys.modules.items()):
        path = getattr(module, '__file__', None)
        if not path:
            continue
        real = os.path.realpath(path)
        if any(real.startswith(root) for root in roots):
            sys.modules.pop(name, None)
            dropped.append(name)
    return dropped


def _rss_bytes() -> int:
    try:
        import psutil

        return psutil.Process().memory_info().rss
    except Exception:
        return 0


def _set_up() -> None:
    """As the process queue sets up each worker: error reporting, tracing, log format."""
    from mage_ai.orchestration.queue.process_queue import Worker
    from mage_ai.settings import SENTRY_DSN

    worker = Worker.__new__(Worker)
    worker.dsn = SENTRY_DSN
    Worker._set_up_process(worker)


def _preload() -> None:
    """The modules every job imports, loaded once here instead of in each job."""
    import mage_ai.data_preparation.executors.executor_factory  # noqa: F401
    import mage_ai.orchestration.pipeline_scheduler  # noqa: F401


def run_job(request: Dict, replies: _Replies) -> Dict:
    """Runs one job; returns the reply for the engine."""
    from mage_ai.orchestration.db.process import start_session_and_run
    from mage_ai.orchestration.queue import process_queue
    from mage_ai.services.redis.redis import init_redis_client

    payload = request.get('payload') or {}
    job_id = request.get('id')
    target, args, kwargs = pickle.loads(base64.b64decode(payload['pickle']))
    client_id = payload.get('client_id')
    redis_url = payload.get('redis_url')
    redis_client = init_redis_client(redis_url) if redis_url else None
    state = _ProcessState()
    current = process_queue._CurrentJob(
        aliases=[],
        client_id=client_id,
        job_dict=_AliasSink(replies),
        job_id=job_id,
        redis_client=redis_client,
    )
    process_queue._current_job = current
    reply = dict(id=job_id, ok=True)
    try:
        start_session_and_run(target, *args, **kwargs)
    except SystemExit as error:
        reply.update(ok=error.code in (None, 0), retire=True)
        if not reply['ok']:
            reply['error'] = f'the job exited with code {error.code}'
    except BaseException as error:
        traceback.print_exc()
        reply.update(ok=False, error=f'{type(error).__name__}: {error}')
        if isinstance(error, KeyboardInterrupt):
            reply['retire'] = True
    finally:
        for finished in [job_id] + current.aliases:
            process_queue._release_redis_job_key(redis_client, client_id, finished)
        process_queue._current_job = None
        state.restore()
        drop_project_modules()
    threads = state.threads_left()
    if threads:
        print(
            f'[engine worker {os.getpid()}] job {job_id} left threads running '
            f'({", ".join(t.name for t in threads)}); this worker is replaced.',
            file=sys.stderr,
        )
        reply['retire'] = True
    rss = _rss_bytes()
    if rss > MAX_RSS_BYTES:
        print(
            f'[engine worker {os.getpid()}] holds {rss // 1048576} MB after job {job_id}, '
            f'above {MAX_RSS_BYTES // 1048576} MB; this worker is replaced.',
            file=sys.stderr,
        )
        reply['retire'] = True
    reply['rss'] = rss
    return reply


def main() -> None:
    # The job's prints are this worker's stdout and stderr; requests and replies use
    # their own descriptors, which processes a job starts do not inherit.
    os.set_inheritable(REPLY_FD, False)
    os.set_inheritable(REQUEST_FD, False)
    replies = _Replies(REPLY_FD)
    # A worker whose engine was killed (the OOM killer, a forced stop) ends with it, and
    # its job's processes too: the scheduler runs that block run again elsewhere.
    from mage_ai.shared.processes import exit_with_parent

    exit_with_parent()
    _set_up()
    _preload()
    replies.send(dict(ready=True, pid=os.getpid()))
    requests = os.fdopen(REQUEST_FD, 'r')
    for line in requests:
        request = json.loads(line)
        if request.get('op') == 'shutdown':
            break
        if request.get('op') != 'run':
            continue
        try:
            reply = run_job(request, replies)
        except BaseException as error:
            # The request itself could not be read: the job never started.
            traceback.print_exc()
            reply = dict(id=request.get('id'), ok=False, retire=True,
                         error=f'{type(error).__name__}: {error}')
        replies.send(reply)
        if reply.get('retire'):
            break


if __name__ == '__main__':
    main()
