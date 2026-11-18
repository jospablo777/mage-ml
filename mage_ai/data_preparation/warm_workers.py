"""
Warm Python worker processes for MageML runs: a block in a Python worker runs in a process
of that environment that stays up for the next block, instead of a new one per block. A
new process costs about 200 ms (Python's start and importing pandas and pyarrow); a warm
one answers in milliseconds.

- A process serves one request at a time; blocks that run at once get processes of their
  own, and an idle one is reused. At most MAX_IDLE_PER_ENVIRONMENT stay idle per
  environment: after twenty blocks at once, twenty processes do not wait five minutes.
- Each request starts as in a new process: the project's changed modules are imported
  again, and what a block did to os.environ or sys.path is undone (worker.serve).
- What a block prints reaches its own log: the worker prints an end marker on stdout and
  stderr after each request, and a request returns once both arrived.
- A process that ends during a request (a crash, the system out of memory) is dropped and
  the request fails saying how it ended; an interrupted request stops its process.
- A process is stopped after MAX_REQUESTS requests, above MAX_RSS_BYTES of memory, or
  IDLE_SECONDS without a request; a worker prepared again has a new environment, so its
  old processes are not reused.
- A process forked from this one does not use this one's processes.

MAGE_WARM_WORKERS=0 runs each block in a process of its own.
"""
import atexit
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from mage_ai.shared.processes import (
    HAS_PROCESS_GROUPS,
    stop_process_group,
    supervised_command,
)

IDLE_SECONDS = int(os.getenv('MAGE_WARM_WORKER_IDLE_SECONDS') or 300)
MAX_REQUESTS = int(os.getenv('MAGE_WARM_WORKER_MAX_REQUESTS') or 500)
MAX_RSS_BYTES = int(os.getenv('MAGE_WARM_WORKER_MAX_RSS_BYTES') or 2 * 1024 ** 3)
MAX_IDLE_PER_ENVIRONMENT = int(os.getenv('MAGE_WARM_WORKER_MAX_IDLE') or 4)
# How long a request waits, after its result, for the lines printed before it.
OUTPUT_SECONDS = 5


def enabled() -> bool:
    return (os.getenv('MAGE_WARM_WORKERS') or '1').strip().lower() not in ('0', 'false', 'off')


class WorkerExited(Exception):
    """The worker's process ended before it answered: returncode says how."""

    def __init__(self, returncode: Optional[int]):
        super().__init__(f'The worker exited with code {returncode}.')
        self.returncode = returncode


# MageML's own credentials, which a block has no use for: its metadata database, the keys
# that sign sessions and downloads, the first owner's password and the sign-in providers'
# secrets. The secrets blocks use (MAGE_SECRET_*, a project's own variables) stay.
PLATFORM_SECRETS = (
    'ACTIVE_DIRECTORY_CLIENT_SECRET', 'DEFAULT_OWNER_PASSWORD', 'GHE_CLIENT_SECRET',
    'GITLAB_CLIENT_SECRET', 'GOOGLE_CLIENT_SECRET', 'JWT_DOWNLOAD_SECRET', 'JWT_SECRET',
    'LDAP_BIND_PASSWORD', 'MAGE_DATABASE_CONNECTION_URL', 'OIDC_CLIENT_SECRET',
    'OKTA_CLIENT_SECRET',
)


class MemoryLimitExceeded(Exception):
    """The worker's processes used more memory than its limit; they were stopped."""

    def __init__(self, used: int, limit: int):
        super().__init__(f'The worker used {used} bytes, over its limit of {limit}.')
        self.used = used
        self.limit = limit


def _tree_rss(pid: int) -> int:
    """The resident memory of a process and its children (the supervisor, the worker and
    whatever a block started)."""
    import psutil

    try:
        root = psutil.Process(pid)
        processes = [root, *root.children(recursive=True)]
    except psutil.Error:
        return 0
    total = 0
    for process in processes:
        try:
            total += process.memory_info().rss
        except psutil.Error:
            continue
    return total


def watch_memory(process: subprocess.Popen, limit: Optional[int], stop: threading.Event,
                 exceeded: List[int], interval: float = 0.25) -> Optional[threading.Thread]:
    """Stops the process's group when its tree's resident memory passes limit; the
    memory it used goes in exceeded."""
    if not limit:
        return None

    def run():
        while not stop.wait(interval):
            used = _tree_rss(process.pid)
            if used > limit:
                exceeded.append(used)
                stop_process_group(process)
                return

    thread = threading.Thread(target=run, daemon=True, name='worker-memory')
    thread.start()
    return thread


def worker_env(repo_path: str, reply_fd: int) -> Dict[str, str]:
    """The environment a worker process runs with (environments.run_block's)."""
    env = dict(
        os.environ,
        MAGE_REPO_PATH=str(repo_path),
        MAGE_WORKER_REPLY_FD=str(reply_fd),
        MAGE_SUPERVISE_PASS_FDS=str(reply_fd),
        PYTHONUNBUFFERED='1',
    )
    # The environment's packages first; MageML's own code stays importable for blocks that
    # use its helpers, as long as the environment has their dependencies.
    mage_root = str(Path(__file__).resolve().parents[2])
    env['PYTHONPATH'] = os.pathsep.join(p for p in (env.get('PYTHONPATH'), mage_root) if p)
    env.pop('VIRTUAL_ENV', None)
    # A run on the engine's job worker has its protocol descriptor named here; this worker
    # reads its requests on stdin.
    env.pop('MAGE_WORKER_REQUEST_FD', None)
    for name in PLATFORM_SECRETS:
        env.pop(name, None)
    return env


class WarmWorker:
    def __init__(self, python: str, repo_path: str, script: str):
        from mage_ai.pipeline_services.runtime.worker import END_MARKER

        self.marker = END_MARKER
        read_fd, write_fd = os.pipe()
        self.process = subprocess.Popen(
            supervised_command([python, '-u', script]),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            env=worker_env(repo_path, write_fd),
            cwd=str(repo_path),
            pass_fds=(write_fd,),
            start_new_session=HAS_PROCESS_GROUPS,
        )
        os.close(write_fd)
        self.replies = os.fdopen(read_fd, encoding='utf-8')
        self.requests = 0
        self.rss_bytes = 0
        self.last_used = time.monotonic()
        # Request id: the events its end markers set, on stdout and on stderr.
        self._ends: Dict[str, Tuple[threading.Event, threading.Event]] = {}
        self._ends_lock = threading.Lock()
        self._pumps = [self._pump(self.process.stdout, 0), self._pump(self.process.stderr, 1)]

    def _pump(self, stream, which: int) -> threading.Thread:
        # sys.stdout and sys.stderr are read on each line: a block's log redirects them.
        def run():
            for line in stream:
                if line.startswith(self.marker):
                    with self._ends_lock:
                        ends = self._ends.get(line[len(self.marker):].strip())
                    if ends:
                        ends[which].set()
                    continue
                target = sys.stdout if which == 0 else sys.stderr
                print(line, end='', file=target, flush=True)

        thread = threading.Thread(target=run, daemon=True, name='warm-worker-output')
        thread.start()
        return thread

    def alive(self) -> bool:
        return self.process.poll() is None

    def call(self, request: Dict, memory_limit: Optional[int] = None) -> Dict:
        """Sends the request and returns its result message."""
        request_id = uuid.uuid4().hex
        request = dict(request, id=request_id, end_marker=True)
        ends = (threading.Event(), threading.Event())
        with self._ends_lock:
            self._ends[request_id] = ends
        done, exceeded = threading.Event(), []
        watch_memory(self.process, memory_limit, done, exceeded)
        try:
            self.process.stdin.write(json.dumps(request) + '\n')
            self.process.stdin.flush()
            result = None
            for line in self.replies:
                if not line.strip():
                    continue
                message = json.loads(line)
                if message.get('type') == 'result' and message.get('id') == request_id:
                    result = message
                    break
            if result is None:
                code = self.process.wait()
                if exceeded:
                    raise MemoryLimitExceeded(exceeded[0], memory_limit)
                raise WorkerExited(code)
            for event in ends:
                event.wait(OUTPUT_SECONDS)
            self.requests += 1
            self.rss_bytes = result.get('rss_bytes') or 0
            self.last_used = time.monotonic()
            return result
        except OSError:
            # The process went away while the request was written or read.
            raise WorkerExited(self.process.wait())
        except BaseException:
            # An interrupted request: the process may be mid-block, so it goes.
            self.stop()
            raise
        finally:
            done.set()
            with self._ends_lock:
                self._ends.pop(request_id, None)

    def worn(self) -> bool:
        return (not self.alive() or self.requests >= MAX_REQUESTS
                or self.rss_bytes >= MAX_RSS_BYTES)

    def stop(self) -> None:
        if self.alive():
            try:
                self.process.stdin.write(json.dumps(dict(op='shutdown', id='bye')) + '\n')
                self.process.stdin.flush()
                self.process.wait(timeout=2)
            except (OSError, ValueError, subprocess.TimeoutExpired):
                stop_process_group(self.process)
        try:
            self.replies.close()
        except OSError:
            pass


_lock = threading.Lock()
_idle: Dict[Tuple[str, str], List[WarmWorker]] = {}
_owner = os.getpid()
_reaper: Optional[threading.Thread] = None


def _reset_after_fork() -> None:
    global _owner, _reaper
    if os.getpid() != _owner:
        # The parent's processes and pipes are the parent's.
        _idle.clear()
        _owner = os.getpid()
        _reaper = None


def _reap() -> None:
    while True:
        time.sleep(30)
        now = time.monotonic()
        stale = []
        with _lock:
            for key, workers in list(_idle.items()):
                keep = []
                for warm in workers:
                    if warm.worn() or now - warm.last_used > IDLE_SECONDS:
                        stale.append(warm)
                    else:
                        keep.append(warm)
                _idle[key] = keep
        for warm in stale:
            warm.stop()


def call(python: str, repo_path: str, script: str, request: Dict,
         memory_limit: Optional[int] = None) -> Dict:
    """Runs the request in a warm process of the environment whose interpreter is python."""
    global _reaper
    key = (str(python), str(repo_path))
    with _lock:
        _reset_after_fork()
        warm = None
        workers = _idle.get(key) or []
        while workers and warm is None:
            candidate = workers.pop()
            if candidate.alive():
                warm = candidate
            else:
                candidate.stop()
        if _reaper is None:
            _reaper = threading.Thread(target=_reap, daemon=True, name='warm-worker-reaper')
            _reaper.start()
    if warm is None:
        warm = WarmWorker(str(python), str(repo_path), script)
    try:
        result = warm.call(request, memory_limit=memory_limit)
    except BaseException:
        warm.stop()
        raise
    kept = False
    if not warm.worn():
        with _lock:
            idle = _idle.setdefault(key, [])
            if os.getpid() == _owner and len(idle) < MAX_IDLE_PER_ENVIRONMENT:
                idle.append(warm)
                kept = True
    if not kept:
        warm.stop()
    return result


def stop_all() -> None:
    with _lock:
        workers = [w for ws in _idle.values() for w in ws]
        _idle.clear()
    for warm in workers:
        warm.stop()


def idle_count() -> int:
    with _lock:
        return sum(len(ws) for ws in _idle.values())


atexit.register(stop_all)
