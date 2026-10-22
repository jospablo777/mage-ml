"""
Lifetime of the processes Mage starts: the scheduler, the worker pool and block workers.

Stopping the web server with SIGTERM, or killing it, left these processes running. A
scheduler that outlived its server kept running pipelines, and the next server started
another one, so each block run ran twice and one of the two failed writing its output.
"""
import multiprocessing
import os
import signal
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from typing import Callable, Iterator, List, Optional, Sequence

import psutil

PARENT_CHECK_SECONDS = 1.0
TERMINATE_TIMEOUT_SECONDS = 5.0
SUPERVISOR = os.path.join(os.path.dirname(__file__), 'supervise.py')
HAS_PROCESS_GROUPS = hasattr(os, 'killpg')
# How Mage starts its processes: the scheduler, the worker pool, block workers and runs from
# the editor. Linux forked them, and a process forked after its parent ran Polars' thread
# pool has the pool's state without its threads: its first Polars call waited forever.
# macOS and Windows spawn by default.
PROCESS_CONTEXT = multiprocessing.get_context('spawn')


def terminate_descendants(timeout: float = TERMINATE_TIMEOUT_SECONDS) -> None:
    """
    Terminates every process this one started, at any depth, and kills those that are
    still alive after the timeout.
    """
    try:
        children = psutil.Process().children(recursive=True)
    except psutil.Error:
        return
    for child in children:
        try:
            child.terminate()
        except psutil.Error:
            pass
    _, alive = psutil.wait_procs(children, timeout=timeout)
    for child in alive:
        try:
            child.kill()
        except psutil.Error:
            pass


# How long a cleanup before exit may take: one that blocks (a job manager waiting on its
# workers under load) kept the processes it should have stopped running for good.
CLEANUP_SECONDS = 10.0


def _run_cleanup(cleanup: Optional[Callable[[], None]], timeout: float = CLEANUP_SECONDS) -> None:
    if cleanup is None:
        return

    def run():
        try:
            cleanup()
        except Exception as error:
            print(f'[WARNING] Cleanup before exit failed: {error}')

    thread = threading.Thread(target=run, daemon=True, name='cleanup-before-exit')
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        print(f'[WARNING] Cleanup before exit took over {timeout:g} seconds; exiting without it.')


def _parent_identity():
    parent = os.getppid()
    try:
        return parent, psutil.Process(parent).create_time()
    except (psutil.Error, OSError, RuntimeError):
        # psutil raises RuntimeError when /proc/stat has no boot time (some containers).
        return parent, None


def _parent_alive(parent: int, created) -> bool:
    # Linux and macOS give an orphan a new parent; Windows keeps the old id, and a new
    # process can reuse it, so the creation time tells them apart.
    if os.getppid() != parent:
        return False
    try:
        process = psutil.Process(parent)
    except psutil.NoSuchProcess:
        return False
    except (psutil.Error, OSError, RuntimeError):
        return True
    if created is None:
        return True
    try:
        return process.create_time() == created
    except psutil.NoSuchProcess:
        return False
    except (psutil.Error, OSError, RuntimeError):
        # Unreadable is not gone: the parent id still matches.
        return True


def exit_with_parent(
    cleanup: Optional[Callable[[], None]] = None,
    interval: float = PARENT_CHECK_SECONDS,
) -> threading.Thread:
    """
    Ends this process, and the processes it started, once its parent is gone. cleanup
    runs first. Call it at the start of a child process.
    """
    parent, created = _parent_identity()

    def watch():
        while _parent_alive(parent, created):
            time.sleep(interval)
        print(f'[Process {os.getpid()}] Parent process {parent} is gone; exiting.')
        _run_cleanup(cleanup)
        terminate_descendants()
        os._exit(1)

    thread = threading.Thread(target=watch, daemon=True, name='exit-with-parent')
    thread.start()
    return thread


def stop_on_terminate(cleanup: Optional[Callable[[], None]] = None) -> None:
    """
    On SIGTERM, runs cleanup, ends the processes this one started and exits. Without it,
    the children of a process stopped with SIGTERM keep running. Call it from the main
    thread of the process.
    """

    def handle(signum, _frame):
        _run_cleanup(cleanup)
        terminate_descendants()
        os._exit(128 + signum)

    signal.signal(signal.SIGTERM, handle)


@contextmanager
def sigterm_raises() -> Iterator[None]:
    """
    SIGTERM raises SystemExit here. The scheduler stops a block run's job with SIGTERM,
    whose default action ends the process at once: no except or finally ran, and the ECS
    task or Kubernetes Job the block started kept running. The previous handler is back
    afterwards.
    """
    if threading.current_thread() is not threading.main_thread():
        # Only the main thread can set a handler.
        yield
        return

    def raise_exit(signum, _frame):
        raise SystemExit(128 + signum)

    previous = signal.signal(signal.SIGTERM, raise_exit)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def supervised_command(args: Sequence[str]) -> List[str]:
    """
    The command that runs args and stops it, with the processes it starts, once this
    process is gone. Start it with `start_new_session=HAS_PROCESS_GROUPS` and stop it with
    stop_process_group. Without process groups, as on Windows, it is args.
    """
    if not HAS_PROCESS_GROUPS:
        return list(args)
    return [sys.executable, '-I', '-S', SUPERVISOR, str(os.getpid()), *args]


def stop_process_group(process: subprocess.Popen) -> None:
    """Kills a process started in a new session, with every process in its group."""
    try:
        if HAS_PROCESS_GROUPS:
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
    except (ProcessLookupError, PermissionError):
        pass
    process.wait()


def freeze_imports() -> None:
    """
    Moves what this process has loaded so far (MageML and its libraries) out of the
    garbage collector's reach. A stage collects garbage after each block to free the
    block's inputs (fusion.release_block); a full collection also scanned every object
    of MageML, about 33 ms per block, more than half of a short run. Call it once the
    imports are done: frozen objects are never collected, which is right for modules
    that live as long as the process.
    """
    import gc

    gc.collect()
    gc.freeze()
