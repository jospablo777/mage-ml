"""
The server's health: the machine's (or container's) load, memory and disk, MageML's
processes (the server, the scheduler, kernels, block runs, the service engine) with their
CPU and memory, the size of what it stores, and six hours of history sampled every
SAMPLE_SECONDS in this process. Sizes of folders are measured in the background every
SIZE_SECONDS: walking a large variables folder takes a while.
"""
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional

import psutil

SAMPLE_SECONDS = 30
HISTORY = 6 * 3600 // SAMPLE_SECONDS
SIZE_SECONDS = 600
MB = 1024 * 1024

_history: deque = deque(maxlen=HISTORY)
_sizes: Dict = {}
_lock = threading.Lock()
_started = False


def _kind(process: psutil.Process, scheduler_pid: Optional[int]) -> str:
    if process.pid == scheduler_pid:
        return 'scheduler'
    try:
        command = ' '.join(process.cmdline())
    except (psutil.Error, OSError):
        return 'other'
    if 'ipykernel' in command or ('kernel' in command and 'json' in command):
        return 'kernel'
    if 'mage-service' in command:
        return 'engine'
    if ' run ' in f' {command} ' or 'block_run' in command:
        return 'block run'
    return 'other'


def _process(process: psutil.Process, kind: str) -> Optional[Dict]:
    try:
        with process.oneshot():
            return dict(
                pid=process.pid,
                kind=kind,
                name=process.name(),
                cpu_percent=process.cpu_percent(interval=None),
                memory_mb=round(process.memory_info().rss / MB, 1),
                started_at=datetime.fromtimestamp(
                    process.create_time(), tz=timezone.utc,
                ).isoformat(),
            )
    except (psutil.Error, OSError):
        return None


def processes() -> List[Dict]:
    """The server process and its descendants."""
    from mage_ai.server.scheduler_manager import scheduler_manager

    try:
        scheduler_pid = scheduler_manager.get_scheduler_pid()
    except Exception:
        scheduler_pid = None
    server = psutil.Process()
    found = [_process(server, 'server')]
    try:
        children = server.children(recursive=True)
    except (psutil.Error, OSError):
        children = []
    found += [_process(child, _kind(child, scheduler_pid)) for child in children]
    return [p for p in found if p]


def machine() -> Dict:
    from mage_ai.orchestration.utils.resources import get_compute, get_memory

    load1, load5, load15, cpus = get_compute()
    free, used, total = get_memory()
    return dict(
        cpu_count=cpus,
        load_1=round(load1, 2),
        load_5=round(load5, 2),
        load_15=round(load15, 2),
        memory_used_mb=round(used, 1) if used is not None else None,
        memory_total_mb=round(total, 1) if total is not None else None,
    )


def sample() -> Dict:
    current = machine()
    found = processes()
    return dict(
        time=datetime.now(timezone.utc).isoformat(),
        load_1=current['load_1'],
        memory_used_mb=current['memory_used_mb'],
        mageml_memory_mb=round(sum(p['memory_mb'] for p in found), 1),
        processes=len(found),
    )


def _folder_size(path: str) -> Optional[int]:
    if not path or not os.path.isdir(path):
        return None
    total = 0
    stack = [path]
    while stack:
        try:
            with os.scandir(stack.pop()) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(entry.path)
                        elif entry.is_file(follow_symlinks=False):
                            total += entry.stat(follow_symlinks=False).st_size
                    except OSError:
                        continue
        except OSError:
            continue
    return total


def storage_paths() -> Dict[str, Optional[str]]:
    from mage_ai.settings.repo import get_variables_dir

    try:
        variables = get_variables_dir()
    except Exception:
        variables = None
    database = None
    url = os.getenv('MAGE_DATABASE_CONNECTION_URL') or ''
    try:
        from mage_ai.orchestration.db import db_connection

        url = url or str(db_connection.engine.url)
    except Exception:
        pass
    if url.startswith('sqlite:///'):
        database = url[len('sqlite:///'):]
    # The project folder is not walked: it can be on network storage, and the variables
    # folder (outputs and logs) is what grows.
    return dict(variables=variables, database=database)


def measure_sizes() -> Dict:
    paths = storage_paths()
    sizes = {}
    for name, path in paths.items():
        if name == 'database':
            size = os.path.getsize(path) if path and os.path.isfile(path) else None
        else:
            size = _folder_size(path)
        sizes[name] = dict(path=path, bytes=size)
    variables = paths.get('variables')
    if variables and os.path.isdir(variables):
        usage = psutil.disk_usage(variables)
        sizes['disk'] = dict(path=variables, bytes=usage.used, total_bytes=usage.total,
                             free_bytes=usage.free)
    sizes['measured_at'] = datetime.now(timezone.utc).isoformat()
    return sizes


def _loop() -> None:
    last_sizes = 0.0
    while True:
        try:
            point = sample()
            with _lock:
                _history.append(point)
            if time.monotonic() - last_sizes >= SIZE_SECONDS:
                last_sizes = time.monotonic()
                measured = measure_sizes()
                with _lock:
                    _sizes.clear()
                    _sizes.update(measured)
        except Exception:
            pass
        time.sleep(SAMPLE_SECONDS)


def start() -> None:
    """Starts sampling in a daemon thread, once per process."""
    global _started

    with _lock:
        if _started:
            return
        _started = True
    threading.Thread(target=_loop, name='mageml-health', daemon=True).start()


def report() -> Dict:
    start()
    with _lock:
        history = list(_history)
        sizes = dict(_sizes)
    return dict(
        machine=machine(),
        processes=processes(),
        history=history,
        storage=sizes or None,
        sample_seconds=SAMPLE_SECONDS,
    )
