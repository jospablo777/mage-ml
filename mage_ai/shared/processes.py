"""
Lifetime of the processes Mage starts: the scheduler, the worker pool and block workers.

Stopping the web server with SIGTERM, or killing it, left these processes running. A
scheduler that outlived its server kept running pipelines, and the next server started
another one, so each block run ran twice and one of the two failed writing its output.
"""
import os
import signal
import subprocess
import sys
import threading
import time
from typing import Callable, List, Optional, Sequence

import psutil

PARENT_CHECK_SECONDS = 1.0
TERMINATE_TIMEOUT_SECONDS = 5.0
SUPERVISOR = os.path.join(os.path.dirname(__file__), 'supervise.py')
HAS_PROCESS_GROUPS = hasattr(os, 'killpg')


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


def _run_cleanup(cleanup: Optional[Callable[[], None]]) -> None:
    if cleanup is None:
        return
    try:
        cleanup()
    except Exception as error:
        print(f'[WARNING] Cleanup before exit failed: {error}')


def _parent_identity():
    parent = os.getppid()
    try:
        return parent, psutil.Process(parent).create_time()
    except psutil.Error:
        return parent, None


def _parent_alive(parent: int, created) -> bool:
    # Linux and macOS give an orphan a new parent; Windows keeps the old id, and a new
    # process can reuse it, so the creation time tells them apart.
    if os.getppid() != parent:
        return False
    try:
        return psutil.Process(parent).create_time() == created
    except psutil.Error:
        return False


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
