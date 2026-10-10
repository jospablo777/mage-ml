"""
The process that runs a pipeline service's Python blocks.

mage-service starts it and keeps it running, so block runs pay no interpreter or import
start-up. Requests come as JSON lines on stdin; each reply is one JSON line on file
descriptor 3. Whatever a block prints goes to stdout and stderr, which the service reads
as that block's log.

A block runs as it does in Mage: its file runs in a new namespace that holds the
decorators, the decorated function gets the upstream outputs in the order of the block's
upstream blocks and, when it takes **kwargs, the run's variables. A returned list or tuple
is several outputs and None is no output; @test functions get the outputs.

Outputs are files in the block's output directory: tables as uncompressed Arrow IPC (the
format Rust blocks read), GeoDataFrames as GeoParquet, other values as JSON, NumPy arrays
as .npy and anything else pickled.

This module imports only the standard library at start; pandas, Polars, pyarrow and NumPy
load when a block's values need them.
"""
import io
import json
import logging
import math
import os
import pickle
import sys
import time
import traceback
from datetime import date, datetime, timezone
from decimal import Decimal
from inspect import Parameter, signature
from typing import Any, Callable, Dict, List

PROTOCOL_VERSION = 1
DATETIME_KWARGS = ('execution_date', 'interval_start_datetime', 'interval_end_datetime')
BLOCK_TYPES = ('data_loader', 'transformer', 'data_exporter', 'custom')


class BlockError(Exception):
    def __init__(self, phase: str, message: str, details: str = ''):
        super().__init__(message)
        self.phase = phase
        self.details = details


# ---- values -------------------------------------------------------------------------


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    if hasattr(value, 'item'):
        return value.item()
    raise TypeError(f'{type(value).__name__} is not JSON serializable')


def _json_safe(value: Any) -> bool:
    """Whether JSON keeps the value: NaN, tuples and non-string keys would change."""
    if value is None or isinstance(value, (bool, str)):
        return True
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, list):
        return all(_json_safe(item) for item in value)
    if isinstance(value, dict):
        return all(isinstance(k, str) and _json_safe(v) for k, v in value.items())
    return False


def _module(name: str):
    return sys.modules.get(name)


def write_output(value: Any, directory: str, index: int) -> Dict:
    """Writes one output and returns its record."""
    base = os.path.join(directory, f'output_{index}')
    pl = _module('polars')
    pd = _module('pandas')
    pa = _module('pyarrow')
    np = _module('numpy')
    gpd = _module('geopandas')

    if pl is not None and isinstance(value, pl.LazyFrame):
        path = base + '.arrow'
        try:
            # Streams the plan into the file without holding the result in memory.
            value.sink_ipc(path, compression='uncompressed')
        except Exception:
            # Plans the streaming engine cannot run are collected first.
            value.collect().write_ipc(path, compression='uncompressed')
        return dict(kind='frame', format='ipc', origin='polars', path=path,
                    rows=_ipc_rows(path))
    if pl is not None and isinstance(value, pl.DataFrame):
        path = base + '.arrow'
        value.write_ipc(path, compression='uncompressed')
        return dict(kind='frame', format='ipc', origin='polars', path=path, rows=value.height)
    if gpd is not None and isinstance(value, gpd.GeoDataFrame):
        path = base + '.parquet'
        value.to_parquet(path)
        return dict(kind='frame', format='parquet', origin='geopandas', path=path,
                    rows=len(value))
    if pd is not None and isinstance(value, (pd.DataFrame, pd.Series)):
        import pyarrow as pa_module

        frame = value.to_frame() if isinstance(value, pd.Series) else value
        table = pa_module.Table.from_pandas(frame, preserve_index=None)
        path = base + '.arrow'
        _write_ipc(table, path)
        origin = 'pandas_series' if isinstance(value, pd.Series) else 'pandas'
        return dict(kind='frame', format='ipc', origin=origin, path=path, rows=len(frame))
    if pa is not None and isinstance(value, pa.Table):
        path = base + '.arrow'
        _write_ipc(value, path)
        return dict(kind='frame', format='ipc', origin='arrow', path=path, rows=value.num_rows)
    if np is not None and isinstance(value, np.ndarray) and value.dtype != object:
        path = base + '.npy'
        np.save(path, value, allow_pickle=False)
        return dict(kind='numpy', path=path)
    if _json_safe(value):
        path = base + '.json'
        with open(path, 'w') as file:
            json.dump(value, file)
        return dict(kind='json', path=path)
    path = base + '.pickle'
    with open(path, 'wb') as file:
        pickle.dump(value, file, protocol=pickle.HIGHEST_PROTOCOL)
    return dict(kind='pickle', path=path, type=f'{type(value).__module__}.{type(value).__name__}')


def _write_ipc(table, path: str) -> None:
    import pyarrow as pa
    import pyarrow.ipc as ipc

    with pa.OSFile(path, 'wb') as sink, ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)


def _ipc_rows(path: str):
    try:
        import pyarrow as pa
        import pyarrow.ipc as ipc

        with pa.memory_map(path) as source:
            reader = ipc.open_file(source)
            return sum(reader.get_batch(i).num_rows for i in range(reader.num_record_batches))
    except Exception:
        return None


def read_input(record: Dict) -> Any:
    """The value one input record stands for, as the upstream block returned it."""
    kind = record.get('kind')
    if kind == 'empty':
        return None
    if kind == 'list':
        return [read_input(item) for item in record.get('items', [])]
    path = record['path']
    if kind == 'json':
        with open(path) as file:
            return json.load(file)
    if kind == 'numpy':
        import numpy as np

        return np.load(path, allow_pickle=False)
    if kind == 'pickle':
        with open(path, 'rb') as file:
            return pickle.load(file)
    if kind != 'frame':
        raise BlockError('input', f'Unknown input kind {kind!r}.')
    origin = record.get('origin') or 'polars'
    fmt = record.get('format', 'ipc')
    if origin == 'geopandas':
        import geopandas

        return geopandas.read_parquet(path)
    if origin == 'polars':
        import polars as pl

        return pl.read_ipc(path) if fmt == 'ipc' else pl.read_parquet(path)
    import pyarrow as pa

    if fmt == 'ipc':
        import pyarrow.ipc as ipc

        with pa.memory_map(path) as source:
            table = ipc.open_file(source).read_all()
    else:
        import pyarrow.parquet as pq

        table = pq.read_table(path)
    if origin == 'arrow':
        return table
    frame = table.to_pandas()
    if origin == 'pandas_series':
        return frame.iloc[:, 0]
    return frame


# ---- blocks -------------------------------------------------------------------------


def _decorator(functions: List[Callable]) -> Callable:
    def decorator(function):
        functions.append(function)
        return function

    return decorator


def _accepts_kwargs(function: Callable) -> bool:
    try:
        return any(p.kind == Parameter.VAR_KEYWORD for p in signature(function).parameters.values())
    except (TypeError, ValueError):
        return False


def _call(function: Callable, args: List, kwargs: Dict) -> Any:
    if kwargs and _accepts_kwargs(function):
        return function(*args, **kwargs)
    return function(*args)


def _kwargs(request: Dict, block: Dict) -> Dict:
    values = dict(request.get('kwargs') or {})
    for key in DATETIME_KWARGS:
        if isinstance(values.get(key), str):
            try:
                parsed = datetime.fromisoformat(values[key].replace('Z', '+00:00'))
                values[key] = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            except ValueError:
                pass
    values.setdefault('configuration', block.get('configuration') or {})
    values.setdefault('context', {})
    values['block_uuid'] = block['uuid']
    values['logger'] = logging.getLogger(f"mage_service.{block['uuid']}")
    return values


def run_block(request: Dict) -> Dict:
    block = request['block']
    block_type = block['type']
    if block_type not in BLOCK_TYPES:
        raise BlockError('load', f'{block_type} blocks are not supported by pipeline services.')
    output_dir = request['output_dir']
    os.makedirs(output_dir, exist_ok=True)

    try:
        inputs = [read_input(record) for record in request.get('inputs', [])]
    except BlockError:
        raise
    except Exception as error:
        raise BlockError('input', f'Reading the upstream outputs failed: {error}',
                         traceback.format_exc())

    functions, tests, preprocessors = [], [], []
    namespace = {
        '__name__': f"mage_block_{block['uuid']}",
        '__file__': block['file'],
        block_type: _decorator(functions),
        'test': _decorator(tests),
        'preprocesser': _decorator(preprocessors),
    }
    try:
        with open(block['file']) as file:
            code = compile(file.read(), block['file'], 'exec')
        exec(code, namespace)
    except Exception as error:
        raise BlockError('load', f'{type(error).__name__}: {error}', traceback.format_exc())

    if not functions:
        raise BlockError(
            'load',
            f"Block {block['uuid']} has no function decorated with @{block_type}.",
        )
    kwargs = _kwargs(request, block)

    started = time.monotonic()
    try:
        for preprocessor in preprocessors:
            _call(preprocessor, inputs, kwargs)
        result = _call(functions[0], inputs, kwargs)
    except Exception as error:
        raise BlockError('run', f'{type(error).__name__}: {error}', _user_traceback())
    seconds = time.monotonic() - started

    if result is None:
        outputs = []
    elif isinstance(result, tuple):
        outputs = list(result)
    elif isinstance(result, list):
        outputs = result
    else:
        outputs = [result]

    try:
        records = [write_output(value, output_dir, i) for i, value in enumerate(outputs)]
    except Exception as error:
        raise BlockError('output', f'Writing the output failed: {type(error).__name__}: {error}',
                         traceback.format_exc())

    test_results = []
    if request.get('run_tests', True):
        for test in tests:
            name = getattr(test, '__name__', 'test')
            try:
                _call(test, list(outputs), kwargs)
                test_results.append(dict(name=name, passed=True))
            except AssertionError as error:
                test_results.append(dict(name=name, passed=False, message=str(error) or name))
            except Exception as error:
                raise BlockError('test', f'{name}: {type(error).__name__}: {error}',
                                 _user_traceback())
        failed = [t for t in test_results if not t['passed']]
        if failed:
            for t in failed:
                print(f"FAIL: {t['name']} (block: {block['uuid']}): {t['message']}", flush=True)
            raise BlockError(
                'test',
                f"{len(failed)} of {len(test_results)} tests failed: "
                + ', '.join(t['name'] for t in failed),
            )
        if test_results:
            print(f'{len(test_results)}/{len(test_results)} tests passed.', flush=True)

    return dict(outputs=records, tests=test_results, seconds=round(seconds, 6))


def _user_traceback() -> str:
    """The traceback without this module's frames, so it starts at the block's code."""
    kind, value, tb = sys.exc_info()
    frames = traceback.extract_tb(tb)
    kept = [f for f in frames if f.filename != __file__]
    lines = ['Traceback (most recent call last):\n'] + traceback.format_list(kept)
    lines += traceback.format_exception_only(kind, value)
    return ''.join(lines)


# ---- protocol -----------------------------------------------------------------------


def _reply(channel, message: Dict) -> None:
    channel.write(json.dumps(message, default=_json_default) + '\n')
    channel.flush()


def serve(requests=None, channel=None) -> None:
    requests = requests or sys.stdin
    channel = channel or io.TextIOWrapper(os.fdopen(3, 'wb', buffering=0), encoding='utf-8')
    _reply(channel, dict(type='ready', protocol=PROTOCOL_VERSION, pid=os.getpid(),
                         python=sys.version.split()[0]))
    for line in requests:
        line = line.strip()
        if not line:
            continue
        request = json.loads(line)
        request_id = request.get('id')
        if request.get('op') == 'shutdown':
            _reply(channel, dict(type='bye', id=request_id))
            return
        try:
            result = run_block(request)
            message = dict(type='result', id=request_id, ok=True, **result)
        except BlockError as error:
            message = dict(type='result', id=request_id, ok=False, error=dict(
                phase=error.phase, message=str(error), details=error.details,
            ))
        except BaseException as error:  # noqa: B036 - the service must get an answer
            message = dict(type='result', id=request_id, ok=False, error=dict(
                phase='worker', message=f'{type(error).__name__}: {error}',
                details=traceback.format_exc(),
            ))
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
        _reply(channel, message)


def main() -> None:
    project = os.environ.get('MAGE_REPO_PATH')
    if project:
        # Blocks import shared code as `<project>.utils...` and as `utils...`.
        for path in (os.path.dirname(project), project):
            if path not in sys.path:
                sys.path.insert(0, path)
        os.chdir(project)
    logging.basicConfig(
        level=os.environ.get('MAGE_SERVICE_LOG_LEVEL', 'INFO'),
        format='%(levelname)s %(name)s: %(message)s',
        stream=sys.stderr,
    )
    serve()


if __name__ == '__main__':
    main()
