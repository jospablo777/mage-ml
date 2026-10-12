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
from typing import Any, Callable, Dict, List, Optional, Tuple

PROTOCOL_VERSION = 1
DATETIME_KWARGS = ('execution_date', 'interval_start_datetime', 'interval_end_datetime')
BLOCK_TYPES = ('data_loader', 'transformer', 'data_exporter', 'custom')


class BlockError(Exception):
    def __init__(self, phase: str, message: str, details: str = '', block_uuid: str = None):
        super().__init__(message)
        self.phase = phase
        self.details = details
        # In a chain, the block that failed.
        self.block_uuid = block_uuid


# The line a chain prints before each block's output, so the service files the lines
# after it under that block.
BLOCK_MARKER = '\x1emage-block '


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


def _has_python_objects(frame) -> bool:
    """Whether an object column holds dicts, lists, sets, tuples or UUIDs."""
    import uuid

    import pandas as pd

    kinds = (dict, list, tuple, set, uuid.UUID)
    for position, dtype in enumerate(frame.dtypes):
        if not pd.api.types.is_object_dtype(dtype):
            continue
        for item in frame.iloc[:, position].array:
            if isinstance(item, kinds):
                return True
    return False


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
        try:
            if _has_python_objects(frame):
                raise pa_module.ArrowInvalid('object columns that Arrow changes')
            table = pa_module.Table.from_pandas(frame, preserve_index=None)
        except (
            pa_module.ArrowInvalid, pa_module.ArrowTypeError, pa_module.ArrowNotImplementedError,
        ):
            # Object columns that Arrow cannot hold or would change (UUIDs come back as
            # bytes, dicts gain the keys of other rows, lists become arrays) keep the frame
            # as it is in a pickle.
            path = base + '.pickle'
            with open(path, 'wb') as file:
                pickle.dump(value, file, protocol=pickle.HIGHEST_PROTOCOL)
            return dict(kind='pickle', path=path, type=f'{type(value).__module__}.'
                        f'{type(value).__name__}', rows=len(frame))
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


def _to_pandas(table):
    """
    The pandas frame a table was written from. pandas cannot read back some ArrowDtype
    names it writes in the metadata, such as 'list<item: int64>[pyarrow]'; those columns
    are rebuilt as ArrowDtype columns from the Arrow data.
    """
    try:
        return table.to_pandas()
    except (TypeError, ValueError):
        metadata = table.schema.pandas_metadata
        if not metadata:
            raise
    import pandas as pd

    arrow_columns = []
    for column in metadata.get('columns', []):
        if str(column.get('numpy_type', '')).endswith('[pyarrow]'):
            arrow_columns.append((column['name'], column.get('field_name') or column['name']))
            column['numpy_type'] = 'object'
    patched = table.replace_schema_metadata({
        **(table.schema.metadata or {}), b'pandas': json.dumps(metadata).encode(),
    })
    frame = patched.to_pandas()
    for name, field in arrow_columns:
        frame[name] = pd.Series(
            pd.arrays.ArrowExtensionArray(table.column(field)), index=frame.index, name=name,
        )
    return frame


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
    frame = _to_pandas(table)
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


_R_EXECUTION = None


def _r_execution():
    """
    Mage's R runner (mage_ai/data_preparation/models/block/r/execution.py), loaded as a
    package of its own: importing it through mage_ai.data_preparation.models.block would
    load Mage's Block and every dependency it has.
    """
    global _R_EXECUTION
    if _R_EXECUTION is None:
        import importlib
        import importlib.util
        import types

        spec = importlib.util.find_spec('mage_ai')
        directory = os.path.join(
            os.path.dirname(spec.origin), 'data_preparation', 'models', 'block', 'r',
        )
        package = types.ModuleType('mage_service_r')
        package.__path__ = [directory]
        sys.modules['mage_service_r'] = package
        _R_EXECUTION = importlib.import_module('mage_service_r.execution')
    return _R_EXECUTION


def _run_tests(block: Dict, results: List[Dict]) -> None:
    """Fails the block when a test failed, as Mage does."""
    failed = [t for t in results if not t['passed']]
    if failed:
        for t in failed:
            print(f"FAIL: {t['name']} (block: {block['uuid']}): {t['message']}", flush=True)
        raise BlockError(
            'test',
            f"{len(failed)} of {len(results)} tests failed: "
            + ', '.join(t['name'] for t in failed),
        )
    if results:
        print(f'{len(results)}/{len(results)} tests passed.', flush=True)


def run_r_block(request: Dict, block: Dict, inputs: List) -> Tuple[List, List, float]:
    try:
        execution = _r_execution()
        with open(block['file'], encoding='utf-8') as file:
            code = file.read()
    except Exception as error:
        raise BlockError('load', f'{type(error).__name__}: {error}', traceback.format_exc())
    variables = {
        key: value for key, value in _kwargs(request, block).items() if key != 'logger'
    }
    started = time.monotonic()
    try:
        run = execution.execute_r_code(
            block['type'],
            code,
            input_vars=inputs,
            global_vars=variables,
            repo_path=os.environ.get('MAGE_REPO_PATH'),
            block_uuid=block['uuid'],
            pipeline_uuid=block.get('pipeline_uuid'),
        )
    except execution.RBlockError as error:
        raise BlockError('run', str(error))
    except Exception as error:
        # The R environment is missing or broken: the image, not the block.
        raise BlockError('load', f'{type(error).__name__}: {error}', traceback.format_exc())
    seconds = time.monotonic() - started
    tests = _r_tests(run.tests)
    if request.get('run_tests', True):
        _run_tests(block, tests)
    return list(run.outputs), tests, seconds


def _r_tests(tests: List[Dict]) -> List[Dict]:
    return [
        dict(name=str(t.get('name') or 'test'), passed=bool(t.get('passed')),
             message=str(t.get('message') or t.get('name') or 'test'))
        for t in tests
    ]


def run_r_chain(
    request: Dict, blocks: List[Dict], inputs: List, keep: bool = False,
) -> Tuple[List, List, List, Dict]:
    """
    Consecutive R blocks in one R session: R starts and loads its packages once, and
    each block takes the previous block's value in memory.
    """
    try:
        execution = _r_execution()
        steps = []
        for block in blocks:
            with open(block['file'], encoding='utf-8') as file:
                steps.append(dict(block_type=block['type'], code=file.read(),
                                  block_uuid=block['uuid'], keep_output=keep))
    except Exception as error:
        raise BlockError('load', f'{type(error).__name__}: {error}', traceback.format_exc(),
                         block_uuid=blocks[0]['uuid'])
    variables = {
        key: value for key, value in _kwargs(request, blocks[0]).items() if key != 'logger'
    }
    try:
        run = execution.execute_r_chain(
            steps,
            input_vars=inputs,
            global_vars=variables,
            repo_path=os.environ.get('MAGE_REPO_PATH'),
            pipeline_uuid=blocks[0].get('pipeline_uuid'),
        )
    except execution.RChainError as error:
        raise BlockError('run', str(error), block_uuid=error.block_uuid)
    except execution.RBlockError as error:
        raise BlockError('run', str(error), block_uuid=blocks[0]['uuid'])
    except Exception as error:
        raise BlockError('load', f'{type(error).__name__}: {error}', traceback.format_exc(),
                         block_uuid=blocks[0]['uuid'])
    # mageml::run_chain stops at a block whose tests fail; that is a RChainError above.
    tests = _r_tests(run.tests)
    seconds = [float(b.get('seconds') or 0) for b in run.blocks]
    return list(run.outputs), tests, seconds, run.kept


def run_sql_block(request: Dict, block: Dict, inputs: List) -> Tuple[List, List, float]:
    from mage_ai.pipeline_services.runtime import sql

    variables = {
        key: value for key, value in _kwargs(request, block).items() if key != 'logger'
    }
    started = time.monotonic()
    try:
        outputs = sql.run(block, inputs, variables, os.environ.get('MAGE_REPO_PATH') or '.')
    except sql.SqlBlockError as error:
        raise BlockError('run', str(error))
    except Exception as error:
        raise BlockError('run', f'{type(error).__name__}: {error}', traceback.format_exc())
    seconds = time.monotonic() - started
    return list(outputs), [], seconds


def _write_outputs(outputs: List, output_dir: str) -> List[Dict]:
    try:
        os.makedirs(output_dir, exist_ok=True)
        return [write_output(value, output_dir, i) for i, value in enumerate(outputs)]
    except Exception as error:
        raise BlockError('output', f'Writing the output failed: {type(error).__name__}: {error}',
                         traceback.format_exc())


def run_block(request: Dict) -> Dict:
    if request.get('chain'):
        return run_chain(request)
    block = request['block']
    output_dir = request['output_dir']
    os.makedirs(output_dir, exist_ok=True)
    inputs = _read_inputs(request)
    if block.get('conditions'):
        skipped = failed_condition(request, block, inputs)
        if skipped or request.get('conditions_only'):
            return dict(outputs=[], tests=[], seconds=0, skipped=skipped)
    outputs, tests, seconds = execute_block(request, block, inputs)
    return dict(outputs=_write_outputs(outputs, output_dir), tests=tests,
                seconds=round(seconds, 6))


def failed_condition(request: Dict, block: Dict, inputs: List) -> Optional[str]:
    """
    Why the block is skipped, or None when every conditional block attached to it returns
    True. As in Mage, each @condition function takes the block's inputs and kwargs, and a
    condition that raises counts as False; its traceback goes to the block's log.
    """
    kwargs = _kwargs(request, block)
    for conditional in block.get('conditions') or []:
        uuid = conditional['uuid']
        functions = []
        namespace = {
            '__name__': f'mage_conditional_{uuid}',
            '__file__': conditional['file'],
            'condition': _decorator(functions),
        }
        try:
            with open(conditional['file']) as file:
                exec(compile(file.read(), conditional['file'], 'exec'), namespace)
            if not functions:
                raise ValueError(f'{conditional["file"]} has no function decorated with '
                                 '@condition')
            for function in functions:
                if not _call(function, list(inputs), kwargs):
                    return f'Condition {uuid} returned False.'
        except Exception as error:
            sys.stderr.write(
                f'Conditional block {uuid} failed, so block {block["uuid"]} is skipped, as '
                f'in Mage:\n{_user_traceback()}'
            )
            return f'Condition {uuid} raised {type(error).__name__}: {error}'
    return None


def _read_inputs(request: Dict) -> List:
    try:
        return [read_input(record) for record in request.get('inputs', [])]
    except BlockError:
        raise
    except Exception as error:
        raise BlockError('input', f'Reading the upstream outputs failed: {error}',
                         traceback.format_exc())


def _passes_in_memory(value: Any) -> bool:
    """
    Whether the next block of a chain may take this value from memory: only when the
    file between two blocks run one by one would give it the same value (as Mage's
    fused stages decide). A Polars DataFrame, or a pandas DataFrame without object
    columns, attrs, an object or multi-level index, or labels that are not text.
    """
    pandas = _module('pandas')
    polars = _module('polars')
    if polars is not None and type(value) is polars.DataFrame:
        return True
    if pandas is not None and type(value) is pandas.DataFrame:
        # Here, not at the top: a worker whose blocks use no pandas may have no NumPy.
        import numpy as np

        object_dtype = np.dtype('O')
        if value.attrs or value.index.dtype == object_dtype:
            return False
        if isinstance(value.index, pandas.MultiIndex) or isinstance(
            value.columns, pandas.MultiIndex,
        ):
            return False
        if not all(isinstance(label, str) for label in value.columns):
            return False
        return not any(dtype == object_dtype for dtype in value.dtypes)
    return False


def _as_exchanged(values: List, directory: str) -> List:
    """
    The values the next block of a chain takes: in memory when that gives the same value
    as the file between blocks run one by one, otherwise written and read back, so a
    block gets the same input with or without chains.
    """
    if len(values) == 1 and _passes_in_memory(values[0]):
        return values
    records = _write_outputs(values, directory)
    return [read_input(record) for record in records]


def run_chain(request: Dict) -> Dict:
    """
    Runs consecutive blocks in this worker, each taking the previous block's outputs in
    memory, as Mage's fused stages do; only the last block's outputs are written.
    Consecutive R blocks run in one R session. Each block's tests run on its outputs;
    a failure names its block.
    """
    blocks = request['chain']
    output_dir = request['output_dir']
    os.makedirs(output_dir, exist_ok=True)
    values = _read_inputs(request)
    tests: List[Dict] = []
    timings: List[Dict] = []
    position = 0
    while position < len(blocks):
        block = blocks[position]
        if block.get('language') == 'r':
            end = position
            while end < len(blocks) and blocks[end].get('language') == 'r':
                end += 1
            keep = bool(request.get('keep_outputs'))
            values, block_tests, seconds, kept = run_r_chain(
                request, blocks[position:end], values, keep,
            )
            tests.extend(block_tests)
            for offset, (b, t) in enumerate(zip(blocks[position:end], seconds)):
                timing = dict(block_uuid=b['uuid'], seconds=round(t, 6))
                if offset in kept and position + offset + 1 < len(blocks):
                    timing['outputs'] = _write_outputs(
                        kept[offset], os.path.join(output_dir, 'kept', str(position + offset)),
                    )
                elif (keep and position + offset + 1 < len(blocks)
                      and offset == end - position - 1):
                    # The last block of an R run passes its value on: keep it too.
                    timing['outputs'] = _write_outputs(
                        values, os.path.join(output_dir, 'kept', str(position + offset)),
                    )
                timings.append(timing)
            position = end
            if position < len(blocks):
                values = _as_exchanged(
                    values, os.path.join(output_dir, 'exchange', str(position)),
                )
            continue
        print(f"{BLOCK_MARKER}{block['uuid']}", flush=True)
        try:
            values, block_tests, seconds = execute_block(request, block, values)
        except BlockError as error:
            error.block_uuid = error.block_uuid or block['uuid']
            raise
        tests.extend(dict(t, name=f"{block['uuid']}: {t['name']}") for t in block_tests)
        timing = dict(block_uuid=block['uuid'], seconds=round(seconds, 6))
        if request.get('keep_outputs') and position + 1 < len(blocks):
            # Every block's outputs, for debugging (MAGE_SERVICE_KEEP_OUTPUTS=on).
            timing['outputs'] = _write_outputs(
                values, os.path.join(output_dir, 'kept', str(position)),
            )
        timings.append(timing)
        position += 1
        if position < len(blocks):
            values = _as_exchanged(values, os.path.join(output_dir, 'exchange', str(position)))
    return dict(outputs=_write_outputs(values, output_dir), tests=tests, blocks=timings,
                seconds=round(sum(t['seconds'] for t in timings), 6))


def execute_block(request: Dict, block: Dict, inputs: List) -> Tuple[List, List, float]:
    """Runs one block on its inputs; returns its outputs, test results and time."""
    block_type = block['type']
    if block_type not in BLOCK_TYPES:
        raise BlockError('load', f'{block_type} blocks are not supported by pipeline services.')
    if block.get('language') == 'r':
        return run_r_block(request, block, inputs)
    if block.get('language') == 'sql':
        return run_sql_block(request, block, inputs)

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
        _run_tests(block, test_results)
    return outputs, test_results, seconds


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
    # Replies go to file descriptor 3, or the one MAGE_WORKER_REPLY_FD names.
    reply_fd = int(os.environ.get('MAGE_WORKER_REPLY_FD') or 3)
    channel = channel or io.TextIOWrapper(
        os.fdopen(reply_fd, 'wb', buffering=0), encoding='utf-8',
    )
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
                block_uuid=error.block_uuid,
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
