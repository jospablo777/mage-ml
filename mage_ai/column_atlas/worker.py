"""
The process that runs ColumnAtlas queries. It imports only the native extension, so it
starts fast, and it holds the Rust engine's memory: a query that runs too long or runs out
of memory ends this process, not the Mage server.

Requests arrive as dicts on a pipe and results go back as (status, value) tuples, where
status is "ok", "invalid" (the request is wrong) or "error" (the engine failed).
"""
import os
from collections import OrderedDict

MAX_OPEN_TABLES = 8


def _limit_memory(megabytes: int) -> None:
    if megabytes <= 0:
        return
    try:
        import resource

        limit = megabytes * 1024 * 1024
        # Private memory (heap and anonymous mappings), so an allocation over the limit fails
        # in this process; memory-mapped Parquet files do not count. macOS does not enforce
        # it; the pool's deadline still applies there.
        resource.setrlimit(resource.RLIMIT_DATA, (limit, limit))
    except (ImportError, ValueError, OSError):
        pass


def serve(connection, threads: int, memory_megabytes: int, order_cache_bytes: int) -> None:
    # Set before the extension starts its thread pool.
    os.environ['POLARS_MAX_THREADS'] = str(max(1, threads))
    _limit_memory(memory_megabytes)

    from column_atlas_native import NativeTable

    tables: OrderedDict = OrderedDict()

    def table(path: str, generation: str):
        key = (path, generation)
        found = tables.get(key)
        if found is not None:
            tables.move_to_end(key)
            return found
        opened = NativeTable(path, order_cache_bytes)
        tables[key] = opened
        while len(tables) > MAX_OPEN_TABLES:
            tables.popitem(last=False)
        return opened

    while True:
        try:
            request = connection.recv()
        except (EOFError, OSError):
            return
        if request is None:
            return
        try:
            native = table(request['path'], request['generation'])
            action = request['action']
            if action == 'metadata':
                value = native.metadata()
            elif action == 'count':
                value = native.count(request['view'])
            elif action == 'rows':
                value = native.rows(
                    request['view'], request['offset'], request['limit'], request['columns'],
                )
            elif action == 'summaries':
                value = native.summaries(request['view'], request['columns'], request['bins'])
            else:
                raise ValueError(f'Unknown action {action}')
            reply = ('ok', value)
        except ValueError as error:
            reply = ('invalid', str(error))
        except MemoryError:
            reply = ('error', 'The query ran out of memory')
        except Exception as error:  # The engine's errors carry no paths.
            reply = ('error', str(error))
        try:
            connection.send(reply)
        except (BrokenPipeError, OSError):
            return
