"""
Finds what makes the editor slow: code that holds the web server's event loop. While the
loop runs a slow callback, every other request (pages, API calls, block output) waits.

With MAGE_EVENT_LOOP_WATCH=1, a ticker on the loop records the time every 50 ms, and a
watcher thread samples the loop thread's stack while the ticker is late. Each stall longer
than MAGE_EVENT_LOOP_WATCH_SECONDS (0.2) is logged once it ends, with its duration and the
stack seen most often, so the log names the code to move off the loop. The stacks hold code
paths only, no values. Sampling costs a stack copy every 20 ms during a stall, nothing
otherwise.
"""
import asyncio
import collections
import logging
import os
import sys
import threading
import time
import traceback
from typing import Optional

logger = logging.getLogger(__name__)

TICK_SECONDS = 0.05
SAMPLE_SECONDS = 0.02


def enabled() -> bool:
    return os.getenv('MAGE_EVENT_LOOP_WATCH', '').lower() in ('1', 'true', 'yes')


def threshold() -> float:
    try:
        return max(float(os.getenv('MAGE_EVENT_LOOP_WATCH_SECONDS') or 0.2), TICK_SECONDS * 2)
    except ValueError:
        return 0.2


def stack_text(frame) -> str:
    """The MageML frames of a stack, which name the handler, and its deepest frames, which
    name what it waited on (an import, a file read, a query)."""
    frames = traceback.extract_stack(frame)
    keep = [f for f in frames if f'{os.sep}mage_ai{os.sep}' in f.filename][-15:]
    keep += [f for f in frames[-8:] if f not in keep]
    return ''.join(traceback.format_list(keep))


class LoopWatch:
    def __init__(self, loop: asyncio.AbstractEventLoop, limit: Optional[float] = None,
                 report=None):
        self.loop = loop
        self.limit = limit if limit is not None else threshold()
        self.report = report or self.log
        self.tick = time.monotonic()
        self.thread_id = None
        self.stopped = threading.Event()

    def start(self) -> 'LoopWatch':
        self.loop.call_soon_threadsafe(self.__ticker)
        threading.Thread(target=self.__watch, name='event-loop-watch', daemon=True).start()
        return self

    def stop(self) -> None:
        self.stopped.set()

    def __ticker(self) -> None:
        self.thread_id = threading.get_ident()
        self.tick = time.monotonic()
        if not self.stopped.is_set():
            self.loop.call_later(TICK_SECONDS, self.__ticker)

    def __watch(self) -> None:
        stall = None
        while not self.stopped.wait(SAMPLE_SECONDS):
            late = time.monotonic() - self.tick
            if late > self.limit and self.thread_id is not None:
                frame = sys._current_frames().get(self.thread_id)
                if frame is not None:
                    if stall is None:
                        stall = (time.monotonic() - late, collections.Counter())
                    stall[1][stack_text(frame)] += 1
            elif stall is not None:
                started, stacks = stall
                stall = None
                self.report(time.monotonic() - started, stacks.most_common(1)[0][0])

    @staticmethod
    def log(seconds: float, stack: str) -> None:
        logger.warning(
            'The server event loop was blocked for %.2f s; every request waited. Most of '
            'the time it was in:\n%s', seconds, stack,
        )


def start_if_enabled(loop: asyncio.AbstractEventLoop) -> Optional[LoopWatch]:
    if not enabled():
        return None
    logger.info('Watching the server event loop for stalls over %.2f s.', threshold())
    return LoopWatch(loop).start()
