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


# ---- streams ------------------------------------------------------------------------

# A stream's blocks are compiled once per worker: their top-level code, such as loading a
# model, runs once instead of for every batch. A changed file (another sha256) replaces its
# entry, so a worker the editor keeps through many edits holds one function per file.
_STREAM_FUNCTIONS: Dict[str, Tuple[str, Callable]] = {}


def _stream_function(block: Dict) -> Callable:
    digest = block.get('sha256') or ''
    cached = _STREAM_FUNCTIONS.get(block['file'])
    if cached is not None and cached[0] == digest:
        return cached[1]
    functions: List[Callable] = []
    namespace = {
        '__name__': f"mage_block_{block['uuid']}",
        '__file__': block['file'],
        'transformer': _decorator(functions),
        'test': _decorator([]),
        'preprocesser': _decorator([]),
    }
    try:
        with open(block['file']) as file:
            exec(compile(file.read(), block['file'], 'exec'), namespace)
    except Exception as error:
        raise BlockError('load', f'{type(error).__name__}: {error}', traceback.format_exc(),
                         block_uuid=block['uuid'])
    if not functions:
        raise BlockError('load', f"Block {block['uuid']} has no function decorated with "
                         '@transformer.', block_uuid=block['uuid'])
    _STREAM_FUNCTIONS[block['file']] = (digest, functions[0])
    return functions[0]


def _input_kind(function: Callable) -> str:
    """
    What the block takes as its messages, by the annotation of its first parameter: a Polars
    or pandas DataFrame (`polars`, `pandas`), or Mage's list of messages (`messages`).
    """
    try:
        parameters = list(signature(function).parameters.values())
    except (TypeError, ValueError):
        return 'messages'
    if not parameters or parameters[0].annotation is Parameter.empty:
        return 'messages'
    annotation = parameters[0].annotation
    if isinstance(annotation, str):
        name = annotation.replace(' ', '')
        return {'pl.DataFrame': 'polars', 'polars.DataFrame': 'polars',
                'pd.DataFrame': 'pandas', 'pandas.DataFrame': 'pandas'}.get(name, 'messages')
    module = getattr(annotation, '__module__', '') or ''
    if getattr(annotation, '__name__', '') == 'DataFrame':
        if module.startswith('polars'):
            return 'polars'
        if module.startswith('pandas'):
            return 'pandas'
    return 'messages'


def _is_polars(value: Any) -> bool:
    return type(value).__module__.startswith('polars') and hasattr(value, 'write_json')


def _as_input(value: Any, kind: str, block_uuid: str) -> Any:
    """The messages as the block takes them: a list, or a Polars or pandas DataFrame."""
    if kind == 'messages':
        if _is_polars(value):
            return value.to_dicts()
        if hasattr(value, 'to_dict') and hasattr(value, 'columns'):
            return value.to_dict('records')
        return value
    if isinstance(value, list):
        if any(not isinstance(m, dict) for m in value):
            raise BlockError('run', f'Block {block_uuid} takes a DataFrame, one row per '
                             'message; its messages must be JSON objects.',
                             block_uuid=block_uuid)
        if kind == 'pandas':
            import pandas as pd

            return pd.DataFrame(value)
        import polars as pl

        return pl.DataFrame(value) if value else pl.DataFrame()
    if kind == 'pandas' and _is_polars(value):
        return value.to_pandas()
    if kind == 'polars' and not _is_polars(value):
        import polars as pl

        return pl.from_pandas(value)
    return value


def _as_list(value: Any) -> List:
    return _as_input(value, 'messages', '')


def _check_finite(value: Any, block_uuid: str, path: str = 'messages') -> None:
    """
    Messages travel as JSON, which has no NaN or infinity. They used to become null without a
    word; the block now fails with the field, so the user decides (None, a string, a filter).
    Called once json.dumps refused a block's messages, to name the field.
    """
    if isinstance(value, float) and not math.isfinite(value):
        raise BlockError('run', f'Block {block_uuid} returned {value} at {path}; stream messages '
                         'are JSON, which has no NaN or infinity. Return None or a string.',
                         block_uuid=block_uuid)
    if isinstance(value, dict):
        for key, item in value.items():
            _check_finite(item, block_uuid, f'{path}.{key}')
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _check_finite(item, block_uuid, f'{path}[{index}]')


def _stream_messages(value: Any, block_uuid: str) -> Any:
    """
    A stream transformer returns a list of messages, a DataFrame, one message or None. A
    Polars DataFrame stays one: the next block can take it as it is, and Polars writes it.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return [value]
    if _is_polars(value):
        return value
    if hasattr(value, 'to_dict') and hasattr(value, 'columns'):  # pandas
        return value.to_dict('records')
    raise BlockError('run', f'Block {block_uuid} returned {type(value).__name__}; a stream '
                     'transformer returns a list of messages or a DataFrame.',
                     block_uuid=block_uuid)


def run_stream_batch(request: Dict) -> Dict:
    """Runs a stream's blocks on one batch (see _stream_batch), with messages as lists."""
    result = _stream_batch(request)
    return dict(result, messages=_as_list(result['messages']),
                outputs={uuid: _as_list(o) for uuid, o in result['outputs'].items()})


def _stream_batch(request: Dict) -> Dict:
    """
    Runs a stream's blocks on one batch. With `isolate` (the stream has a dead-letter sink),
    a batch that fails is split in halves until the messages that fail on their own are found:
    the others' outputs come back with `failures`, each failing message's row and error. A
    block that fails to load, or every message failing, is a failure of the batch, never
    dead letters: a code or infrastructure problem must not empty the stream into them. A
    block can see a message more than once while its batch is split.
    """
    if request.get('messages_file'):
        request = dict(request, messages=_read_messages_file(request))
    if request.get('message_shape') == 'rabbitmq':
        request = dict(request, messages=[_rabbitmq_payload(m) for m in request['messages']])
    elif request.get('message_shape') == 'event_hub':
        # Mage's Azure Event Hubs source hands blocks each body as text, under `data`.
        request = dict(request, messages=[dict(data=m) for m in request['messages']])
    elif request.get('message_shape') == 'raw':
        # Mage's RAW_VALUE: each message's bytes, sent as base64.
        import base64

        request = dict(request, messages=[base64.b64decode(m) for m in request['messages']])
    if not request.get('isolate'):
        return _run_stream_chain(request, request['messages'])
    messages = request['messages']
    try:
        return _run_stream_chain(request, messages)
    except BlockError as error:
        if error.phase != 'run' or len(messages) <= 1:
            raise
        outputs, failures = _isolate(request, messages, 0)
        if len(failures) == len(messages):
            raise
        last = request['chain'][-1]['uuid'] if request['chain'] else None
        return dict(
            outputs=outputs,
            messages=outputs.get(last, []) if last else messages,
            blocks=[],
            failures=failures,
            outputs_json={uuid: _messages_json(o, uuid) for uuid, o in outputs.items()},
        )


def _read_messages_file(request: Dict) -> Any:
    """
    The batch's messages from the JSON lines file the engine wrote for blocks that take a
    DataFrame: Polars reads it in one pass, with no Python object per message. A file Polars
    cannot read as a table (a message that is not an object) is read message by message, so
    the block's error names it.
    """
    with open(request['messages_file'], 'rb') as file:
        data = file.read()
    if not request.get('count'):
        return []
    try:
        import polars as pl

        return pl.read_ndjson(io.BytesIO(data))
    except Exception:
        messages = []
        for line in data.splitlines():
            try:
                messages.append(json.loads(line))
            except ValueError:
                messages.append(line.decode('utf-8', errors='replace'))
        return messages


def _rabbitmq_payload(message: Dict):
    """
    A RabbitMQ delivery as Mage's RabbitMQ source hands it to blocks: Payload(method,
    properties, body), body the message's bytes, so blocks written for Mage run unchanged.
    """
    import base64
    from collections import namedtuple
    from types import SimpleNamespace

    payload = namedtuple('Payload', ['method', 'properties', 'body'])
    if 'body_base64' in message:
        body = base64.b64decode(message['body_base64'])
    else:
        body = (message.get('body') or '').encode('utf-8')
    method = SimpleNamespace(
        delivery_tag=message.get('delivery_tag'),
        routing_key=message.get('routing_key'),
        exchange='',
        redelivered=False,
    )
    properties = SimpleNamespace(
        headers=message.get('headers') or {},
        content_type=None,
        message_id=None,
    )
    return payload(method, properties, body)


def _isolate(request: Dict, messages: List, offset: int) -> Tuple[Dict[str, List], List[Dict]]:
    """The outputs of the messages that run, and the rows of those that fail on their own."""
    try:
        return _run_stream_chain(request, messages)['outputs'], []
    except BlockError as error:
        if error.phase != 'run':
            raise
        if len(messages) == 1:
            return {}, [dict(row=offset, block_uuid=error.block_uuid,
                             message=str(error), details=error.details)]
        middle = len(messages) // 2
        left, left_failures = _isolate(request, messages[:middle], offset)
        right, right_failures = _isolate(request, messages[middle:], offset + middle)
        outputs = {uuid: _as_list(left.get(uuid, [])) + _as_list(right.get(uuid, []))
                   for uuid in dict.fromkeys([*left, *right])}
        return outputs, left_failures + right_failures


def _run_stream_chain(request: Dict, messages: List) -> Dict:
    """
    Runs a stream's blocks on one batch of messages, as Mage's streaming transformers do:
    each gets the list of messages of the block before it (the source's for a block without
    an upstream) and returns a list. Streams branch, so the blocks come in topological order
    with their `upstream`, and `outputs` names the blocks whose messages the sinks write.
    """
    blocks = request['chain']
    results: Dict[str, List] = {}
    timings = []
    # Blocks without any `upstream` key form one chain, each after the one before.
    linear = not any('upstream' in b for b in blocks)
    for position, block in enumerate(blocks):
        if linear:
            upstream = [blocks[position - 1]['uuid']] if position else []
        else:
            upstream = block.get('upstream') or []
        if len(upstream) > 1:
            raise BlockError('load', f"Block {block['uuid']} reads {len(upstream)} blocks; a "
                             'stream block reads one.', block_uuid=block['uuid'])
        block_messages = results.get(upstream[0], []) if upstream else messages
        if upstream and not len(block_messages):
            # The block before it kept nothing of this batch.
            results[block['uuid']] = []
            continue
        function = _stream_function(block)
        kwargs = _kwargs(request, block)
        print(f"{BLOCK_MARKER}{block['uuid']}", flush=True)
        started = time.monotonic()
        block_messages = _as_input(block_messages, _input_kind(function), block['uuid'])
        try:
            result = _call(function, [block_messages], kwargs)
        except Exception as error:
            raise BlockError('run', f'{type(error).__name__}: {error}', _user_traceback(),
                             block_uuid=block['uuid'])
        results[block['uuid']] = _stream_messages(result, block['uuid'])
        timings.append(dict(block_uuid=block['uuid'],
                            seconds=round(time.monotonic() - started, 6)))
    last = blocks[-1]['uuid'] if blocks else None
    wanted = request.get('outputs') or ([last] if last else [])
    outputs = {uuid: results.get(uuid, []) for uuid in wanted}
    return dict(
        outputs=outputs,
        messages=results.get(last, []) if last else messages,
        blocks=timings,
        # The outputs as JSON, made once: the reply reuses it. Only what leaves the worker
        # must be JSON; messages passed between blocks in the worker may hold anything.
        outputs_json={uuid: _messages_json(output, uuid) for uuid, output in outputs.items()},
    )


def _frame_json(frame, block_uuid: str) -> str:
    """A Polars DataFrame's rows as a JSON array; NaN, which Polars writes as null, is refused."""
    import polars as pl

    floats = [name for name, dtype in frame.schema.items() if dtype.is_float()]
    if floats:
        nan = frame.select(pl.col(floats).is_nan().any())
        for name in floats:
            if nan[name][0]:
                row = frame.with_row_index('__row').filter(pl.col(name).is_nan())['__row'][0]
                raise BlockError('run', f'Block {block_uuid} returned NaN at messages[{row}].'
                                 f'{name}; stream messages are JSON, which has no NaN or '
                                 'infinity. Return None or a string.', block_uuid=block_uuid)
    for name, dtype in frame.schema.items():
        if dtype.is_float() and frame[name].is_infinite().any():
            raise BlockError('run', f'Block {block_uuid} returned infinity in {name}; stream '
                             'messages are JSON, which has no NaN or infinity.',
                             block_uuid=block_uuid)
    return frame.write_json() if frame.height else '[]'


def _messages_json(messages: Any, block_uuid: str) -> str:
    if _is_polars(messages):
        return _frame_json(messages, block_uuid)
    try:
        return json.dumps(messages, allow_nan=False, default=_json_default)
    except (TypeError, ValueError) as error:
        _check_finite(messages, block_uuid)
        raise BlockError('run', f'Block {block_uuid} returned messages that are not JSON: '
                         f'{error}.', block_uuid=block_uuid)


# ---- protocol -----------------------------------------------------------------------


def _reply(channel, message: Dict) -> None:
    channel.write(_reply_line(message) + '\n')
    channel.flush()


def _reply_line(message: Dict) -> str:
    """
    A stream batch's reply holds its outputs as the JSON made when they were checked, not
    serialized again; `messages` (the last block's, for callers in this process) is left out.
    """
    outputs = message.pop('outputs_json', None)
    if outputs is None:
        return json.dumps(message, default=_json_default)
    message.pop('messages', None)
    message.pop('outputs', None)
    head = json.dumps(message, default=_json_default)
    body = ','.join(f'{json.dumps(uuid)}:{text}' for uuid, text in outputs.items())
    return f'{head[:-1]},"outputs":{{{body}}}}}'



def _rss_bytes() -> Optional[int]:
    """The worker's resident memory now (Linux), or its peak where only that is known."""
    try:
        with open('/proc/self/statm') as file:
            return int(file.read().split()[1]) * os.sysconf('SC_PAGE_SIZE')
    except (OSError, ValueError, IndexError):
        pass
    try:
        import resource

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Bytes on macOS, kilobytes on other systems.
        return peak if sys.platform == 'darwin' else peak * 1024
    except (ImportError, OSError):
        return None


def _protocol_fd(variable: str, default: Optional[int]) -> Optional[int]:
    """A protocol descriptor, made non-inheritable so programs a block starts cannot use it."""
    value = os.environ.get(variable)
    fd = int(value) if value else default
    if fd is not None:
        os.set_inheritable(fd, False)
    return fd


def serve(requests=None, channel=None) -> None:
    # Requests come on the descriptor MAGE_WORKER_REQUEST_FD names (the service uses 4 and
    # gives the worker /dev/null as stdin), or on stdin when it is not set.
    if requests is None:
        request_fd = _protocol_fd('MAGE_WORKER_REQUEST_FD', None)
        requests = (os.fdopen(request_fd, 'r', encoding='utf-8')
                    if request_fd is not None else sys.stdin)
    # Replies go to file descriptor 3, or the one MAGE_WORKER_REPLY_FD names.
    if channel is None:
        reply_fd = _protocol_fd('MAGE_WORKER_REPLY_FD', 3)
        channel = io.TextIOWrapper(os.fdopen(reply_fd, 'wb', buffering=0), encoding='utf-8')
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
            if request.get('op') == 'stream':
                # The reply holds the outputs' JSON: a Polars output is not made a list.
                result = _stream_batch(request)
            elif request.get('op') == 'r_stream_command':
                # How a stream's R program starts: the R setup lives here, not in the engine.
                import importlib

                _r_execution()
                stream = importlib.import_module('mage_service_r.stream')
                result = stream.stream_command(os.environ.get('MAGE_REPO_PATH') or os.getcwd(),
                                               supervised=False)
            elif request.get('op') == 'stream_load':
                # Compiles a stream's blocks (their top-level code runs) without a batch,
                # and says what each takes: the engine sends a table's rows as a file.
                inputs = {b['uuid']: _input_kind(_stream_function(b)) for b in request['chain']}
                result = dict(loaded=list(inputs), inputs=inputs)
            else:
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
        message['rss_bytes'] = _rss_bytes()
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
