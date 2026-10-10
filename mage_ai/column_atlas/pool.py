"""
Worker processes for ColumnAtlas queries. Requests for one source go to the same worker,
which keeps the source open and its computed views cached. A query past its deadline, or a
worker that dies (out of memory, a crash in native code), costs that worker: it is killed
and started again on the next request, and the request fails with an error that says why.
"""
import atexit
import multiprocessing
import threading
import time
import zlib
from typing import Any, Dict, List, Optional

from mage_ai.column_atlas.errors import (
    AtlasBusy,
    AtlasEngineError,
    AtlasInvalidRequest,
    AtlasTimeout,
)


class _Worker:
    def __init__(self, index: int, settings: Dict[str, int], target=None):
        self.index = index
        self.settings = settings
        self.target = target
        self.lock = threading.Lock()
        self.process: Optional[multiprocessing.Process] = None
        self.connection = None

    def start(self) -> None:
        context = multiprocessing.get_context('spawn')
        parent, child = context.Pipe()
        from mage_ai.column_atlas.worker import serve

        process = context.Process(
            target=self.target or serve,
            args=(
                child,
                self.settings['threads'],
                self.settings['memory_megabytes'],
                self.settings['order_cache_bytes'],
            ),
            daemon=True,
            name=f'column-atlas-{self.index}',
        )
        process.start()
        child.close()
        self.process = process
        self.connection = parent

    def alive(self) -> bool:
        return self.process is not None and self.process.is_alive()

    def stop(self) -> None:
        process, connection = self.process, self.connection
        self.process = None
        self.connection = None
        if connection is not None:
            try:
                connection.close()
            except OSError:
                pass
        if process is not None and process.is_alive():
            process.kill()
            process.join(5)


class WorkerPool:
    def __init__(
        self,
        workers: int = 2,
        timeout_seconds: float = 30,
        threads: int = 2,
        memory_megabytes: int = 2048,
        order_cache_bytes: int = 64 * 1024 * 1024,
        target=None,
    ):
        """target replaces the worker function, for tests."""
        if workers < 1:
            raise ValueError('ColumnAtlas needs at least one worker')
        settings = dict(
            threads=threads,
            memory_megabytes=memory_megabytes,
            order_cache_bytes=order_cache_bytes,
        )
        self.timeout_seconds = timeout_seconds
        self._workers: List[_Worker] = [
            _Worker(index, settings, target) for index in range(workers)
        ]
        self._closed = False
        atexit.register(self.close)

    def _worker_for(self, key: str) -> _Worker:
        return self._workers[zlib.crc32(key.encode('utf-8')) % len(self._workers)]

    def call(self, key: str, request: Dict[str, Any], timeout_seconds: float = None) -> Any:
        """Runs a request on the worker for key; blocks, so call it off the event loop."""
        if self._closed:
            raise AtlasEngineError('ColumnAtlas is shutting down')
        timeout = timeout_seconds or self.timeout_seconds
        worker = self._worker_for(key)
        started = time.monotonic()
        if not worker.lock.acquire(timeout=timeout):
            raise AtlasBusy('ColumnAtlas is busy with other queries; try again')
        try:
            remaining = max(1.0, timeout - (time.monotonic() - started))
            if not worker.alive():
                worker.stop()
                worker.start()
            try:
                worker.connection.send(request)
                if not worker.connection.poll(remaining):
                    worker.stop()
                    raise AtlasTimeout(
                        f'The query took longer than {int(timeout)} seconds and was stopped',
                    )
                status, value = worker.connection.recv()
            except (EOFError, BrokenPipeError, ConnectionResetError, OSError):
                code = worker.process.exitcode if worker.process else None
                worker.stop()
                raise AtlasEngineError(
                    'The query process stopped'
                    + (' (out of memory or killed)' if code and code < 0 else '')
                    + '; try a narrower view',
                )
        finally:
            worker.lock.release()
        if status == 'ok':
            return value
        if status == 'invalid':
            raise AtlasInvalidRequest(value)
        raise AtlasEngineError(value)

    def close(self) -> None:
        self._closed = True
        for worker in self._workers:
            with worker.lock:
                worker.stop()
