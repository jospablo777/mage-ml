"""
Reads of project files (pipelines, the project's metadata.yaml) kept for one pass of the
scheduler.

A pass read each pipeline's files about five times per running pipeline run and the
project's metadata.yaml about four times: with 50 runs in flight, about 40% of a pass.
Inside `one_pass()` a read is done once; outside it, and in threads, every read goes to
disk as before. A pass sees files as they were when it read them; the next pass reads
them again.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Callable, Hashable, Optional

_store: ContextVar[Optional[dict]] = ContextVar('mage_read_cache', default=None)


@contextmanager
def one_pass():
    token = _store.set({})
    try:
        yield
    finally:
        _store.reset(token)


def cached(key: Hashable, load: Callable[[], Any]) -> Any:
    store = _store.get()
    if store is None:
        return load()
    if key not in store:
        store[key] = load()
    return store[key]
