"""
Runs a command and stops it, with the processes it started, once the process that started
this one is gone.

    python supervise.py <parent pid> <command> [arguments...]

Start it as the leader of a new session (`start_new_session=True`); the command runs in
its process group, and the parent stops both with `os.killpg`. A block's R process used to
keep running after the kernel or worker running the block was killed. This file imports
only the standard library, so it starts fast; run it with `python -I -S`.
"""
import os
import signal
import subprocess
import sys
import threading
import time

PARENT_CHECK_SECONDS = 0.5
TERMINATE_TIMEOUT_SECONDS = 3.0


def _stop_group(child: subprocess.Popen) -> None:
    group = os.getpgrp()
    try:
        os.killpg(group, signal.SIGTERM)
        child.wait(timeout=TERMINATE_TIMEOUT_SECONDS)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        pass
    # Ends this process too, and what the command started and left running.
    os.killpg(group, signal.SIGKILL)


def main(argv) -> int:
    parent = int(argv[1])
    # Handlers, unlike ignored signals, do not reach the command: it gets SIGINT and
    # SIGTERM with their default behavior, and this process outlives them to report the
    # command's exit code.
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: None)
    # Descriptors the command needs besides stdin, stdout and stderr, such as a reply pipe.
    pass_fds = tuple(
        int(fd) for fd in os.environ.get('MAGE_SUPERVISE_PASS_FDS', '').split(',') if fd
    )
    child = subprocess.Popen(argv[2:], pass_fds=pass_fds)

    def watch():
        # An orphan gets a new parent, so the parent id changes when the parent is gone.
        while os.getppid() == parent:
            time.sleep(PARENT_CHECK_SECONDS)
        _stop_group(child)

    threading.Thread(target=watch, daemon=True).start()
    code = child.wait()
    return code if code >= 0 else 128 - code


if __name__ == '__main__':
    sys.exit(main(sys.argv))
