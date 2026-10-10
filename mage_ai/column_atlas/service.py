"""
ColumnAtlas requests: validation, a result cache bounded in bytes, and the worker pool.
Settings come from the environment:

- MAGE_COLUMN_ATLAS: 0 turns the explorer off; block outputs show the plain table.
- MAGE_COLUMN_ATLAS_WORKERS: query processes (2).
- MAGE_COLUMN_ATLAS_TIMEOUT_SECONDS: deadline of one query (30).
- MAGE_COLUMN_ATLAS_THREADS: threads per query process (2).
- MAGE_COLUMN_ATLAS_WORKER_MEMORY_MB: data memory limit (RLIMIT_DATA) of a query process on
  Linux (4096).
- MAGE_COLUMN_ATLAS_CACHE_MB: cached results in the server process (32).
"""
import asyncio
import importlib.util
import json
import os
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

from mage_ai.column_atlas.errors import AtlasInvalidRequest, AtlasUnavailable
from mage_ai.column_atlas.pool import WorkerPool
from mage_ai.column_atlas.sources import ResolvedSource, Source

ACTIONS = ('metadata', 'count', 'rows', 'summaries')
MAX_ROWS = 512
MAX_COLUMNS = 48
MAX_SUMMARY_COLUMNS = 16
MAX_VIEW_BYTES = 16 * 1024
CACHE_TTL_SECONDS = 600


def _int_setting(name: str, default: int) -> int:
    try:
        return int(os.getenv(name) or default)
    except ValueError:
        return default


def enabled() -> bool:
    if os.getenv('MAGE_COLUMN_ATLAS', '1').strip().lower() in ('0', 'false', 'no', 'off'):
        return False
    return importlib.util.find_spec('column_atlas_native') is not None


class _Cache:
    """Results by key, least recently used first out, within a byte budget."""

    def __init__(self, budget: int, ttl_seconds: int = CACHE_TTL_SECONDS):
        self.budget = budget
        self.ttl_seconds = ttl_seconds
        self.used = 0
        self.lock = threading.Lock()
        self.entries: OrderedDict = OrderedDict()

    def get(self, key: Tuple) -> Optional[Any]:
        with self.lock:
            entry = self.entries.get(key)
            if entry is None:
                return None
            created, size, value = entry
            if time.monotonic() - created > self.ttl_seconds:
                self.entries.pop(key)
                self.used -= size
                return None
            self.entries.move_to_end(key)
            return value

    def put(self, key: Tuple, value: Any, size: int) -> None:
        if size > self.budget // 4:
            return
        with self.lock:
            previous = self.entries.pop(key, None)
            if previous is not None:
                self.used -= previous[1]
            self.entries[key] = (time.monotonic(), size, value)
            self.used += size
            while self.used > self.budget and self.entries:
                _, (_, old_size, _) = self.entries.popitem(last=False)
                self.used -= old_size


def _view(payload: Dict[str, Any]) -> str:
    view = payload.get('view') or {}
    if not isinstance(view, dict):
        raise AtlasInvalidRequest('The view must be an object')
    text = json.dumps(
        dict(filters=view.get('filters') or [], sort=view.get('sort') or []),
        sort_keys=True,
        separators=(',', ':'),
    )
    if len(text) > MAX_VIEW_BYTES:
        raise AtlasInvalidRequest('The view is too large')
    return text


def _whole(value: Any, field: str, low: int, high: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise AtlasInvalidRequest(f'{field} must be a whole number from {low} to {high}')
    return value


def _columns(value: Any, limit: int) -> List[int]:
    if not isinstance(value, list) or not 1 <= len(value) <= limit:
        raise AtlasInvalidRequest(f'Request 1 to {limit} columns')
    columns = [_whole(item, 'A column', 0, 1_000_000) for item in value]
    if len(set(columns)) != len(columns):
        raise AtlasInvalidRequest('Columns are repeated')
    return columns


class ColumnAtlasService:
    def __init__(self, pool: WorkerPool, cache_bytes: int = 32 * 1024 * 1024):
        self.pool = pool
        self.cache = _Cache(cache_bytes)

    def request(
        self, action: str, resolved: ResolvedSource, payload: Dict[str, Any],
    ) -> Tuple[Tuple, Dict[str, Any]]:
        """The cache key and worker request for an action, after validation."""
        if action not in ACTIONS:
            raise AtlasInvalidRequest(f'Unknown action; use one of {", ".join(ACTIONS)}')
        request: Dict[str, Any] = dict(
            action=action, path=resolved.path, generation=resolved.generation,
        )
        key: Tuple = (action, resolved.path, resolved.generation)
        if action in ('count', 'rows', 'summaries'):
            request['view'] = _view(payload)
            key += (request['view'],)
        if action == 'rows':
            request['offset'] = _whole(payload.get('offset', 0), 'offset', 0, 2 ** 53)
            request['limit'] = _whole(payload.get('limit', 100), 'limit', 1, MAX_ROWS)
            request['columns'] = _columns(payload.get('columns'), MAX_COLUMNS)
            key += (request['offset'], request['limit'], tuple(request['columns']))
        if action == 'summaries':
            request['columns'] = _columns(payload.get('columns'), MAX_SUMMARY_COLUMNS)
            request['bins'] = _whole(payload.get('bins', 24), 'bins', 4, 64)
            key += (tuple(request['columns']), request['bins'])
        return key, request

    async def query(
        self, action: str, source: Source, resolved: ResolvedSource, payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        key, request = self.request(action, resolved, payload)
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        raw = await asyncio.to_thread(self.pool.call, source.key, request)
        # Every result names the version of the output it read, so a browser that holds
        # older metadata sees that the block ran again.
        version = resolved.generation
        if action == 'count':
            result = dict(row_count=raw, source_version=version)
            size = 64
        else:
            parsed = json.loads(raw)
            size = len(raw)
            if action == 'metadata':
                result = dict(parsed, label=resolved.label, source_version=version)
            elif action == 'summaries':
                result = dict(summaries=parsed, source_version=version)
            else:
                result = dict(parsed, source_version=version)
        self.cache.put(key, result, size)
        return result


_service: Optional[ColumnAtlasService] = None
_service_lock = threading.Lock()


def get_service() -> ColumnAtlasService:
    global _service
    if not enabled():
        raise AtlasUnavailable('ColumnAtlas is turned off on this server')
    with _service_lock:
        if _service is None:
            pool = WorkerPool(
                workers=max(1, _int_setting('MAGE_COLUMN_ATLAS_WORKERS', 2)),
                timeout_seconds=max(1, _int_setting('MAGE_COLUMN_ATLAS_TIMEOUT_SECONDS', 30)),
                threads=max(1, _int_setting('MAGE_COLUMN_ATLAS_THREADS', 2)),
                memory_megabytes=_int_setting('MAGE_COLUMN_ATLAS_WORKER_MEMORY_MB', 4096),
            )
            _service = ColumnAtlasService(
                pool, cache_bytes=max(1, _int_setting('MAGE_COLUMN_ATLAS_CACHE_MB', 32)) * 2 ** 20,
            )
        return _service
