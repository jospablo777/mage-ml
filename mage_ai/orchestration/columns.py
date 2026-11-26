"""
Column checks at compile time: a block that reads a column its upstream block does not
output fails in the compile, not after the upstream blocks ran.

Nothing is declared for it. Each Python block's reads are found in its code: `df['x']`,
`df[['x', 'y']]` and, for a block with one input, `pl.col('x')`, on the parameters its
upstream blocks' outputs arrive in (in order). They are compared with what the upstream
block gives:

- its data contract's columns (configuration.contract): a missing column is an error;
- else the columns of its most recent stored output (the editor's or a run's): a warning,
  since the next run can output other columns.

A column the block assigns before it reads it is its own, and a parameter the block binds
again (`df = df.rename(...)`) is not checked after that: the check reports what it is
sure of.

The other way, `unused` finds an upstream block whose readers use few of its columns: when
every block downstream of it uses its output only as `df['x']` or `df[['x', 'y']]`, the
columns they leave are measured in its last stored output (the Parquet footer's sizes), and
a large share is reported, so the loader can select only what is read.
"""
import ast
import glob
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

DECORATORS = {'transformer', 'data_exporter', 'custom', 'sensor', 'condition'}
CHECKED_TYPES = {'transformer', 'data_exporter', 'custom'}


@dataclass
class ColumnFinding:
    block: str
    upstream: str
    missing: List[str]
    available: List[str]
    # 'contract' (declared, an error) or 'last output' (inferred, a warning).
    source: str
    line: int
    severity: str = 'warning'

    def to_dict(self) -> Dict:
        shown = ', '.join(self.available[:12]) + (
            f' and {len(self.available) - 12} more' if len(self.available) > 12 else '')
        what = 'its contract lists' if self.source == 'contract' else \
            'its last stored output has'
        return dict(
            available=self.available,
            block=self.block,
            line=self.line,
            message=(
                f'{self.block} reads {", ".join(self.missing)} from {self.upstream}, which '
                f'{what} no column of that name (it has {shown or "no columns"}).'
            ),
            missing=self.missing,
            severity=self.severity,
            source=self.source,
            upstream=self.upstream,
        )


@dataclass
class _Reads:
    """Column reads of one parameter, with the line of the first read of each."""
    columns: Dict[str, int] = field(default_factory=dict)


def _decorated_function(tree: ast.AST) -> Optional[ast.FunctionDef]:
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for decorator in node.decorator_list:
                name = decorator.func if isinstance(decorator, ast.Call) else decorator
                if isinstance(name, ast.Name) and name.id in DECORATORS:
                    return node
    return None


def _strings(node: ast.AST) -> List[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, (ast.List, ast.Tuple)):
        values = [_strings(e) for e in node.elts]
        if all(len(v) == 1 for v in values):
            return [v[0] for v in values]
    return []


def _position(node: ast.AST) -> Tuple[int, int]:
    return getattr(node, 'lineno', 0), getattr(node, 'col_offset', 0)


def column_reads(code: str) -> Dict[str, Dict[str, int]]:
    """
    Per input parameter of the block's function, the columns it reads that it did not
    assign first, each with its line. Empty when the code does not parse.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return {}
    function = _decorated_function(tree)
    if function is None:
        return {}
    params = [a.arg for a in function.args.posonlyargs + function.args.args]
    reads = {p: _Reads() for p in params}
    events = []
    for node in ast.walk(function):
        if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name) \
                and node.value.id in reads:
            kind = 'write' if isinstance(node.ctx, ast.Store) else 'read'
            for column in _strings(node.slice):
                events.append((_position(node), kind, node.value.id, column))
        elif isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name) and target.id in reads:
                    # Bound again after its value: reads after it are not of the input.
                    events.append(((node.end_lineno or 0, 1 << 30), 'rebind', target.id, ''))
        elif isinstance(node, ast.Call) and len(params) == 1 \
                and isinstance(node.func, ast.Attribute) and node.func.attr == 'col' \
                and isinstance(node.func.value, ast.Name) and node.func.value.id == 'pl':
            for arg in node.args:
                for column in _strings(arg):
                    events.append((_position(node), 'read', params[0], column))
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                and node.func.attr in ('alias', 'assign'):
            # A Polars alias or pandas assign makes a column: not a read of the input.
            names = [k.arg for k in node.keywords if k.arg] if node.func.attr == 'assign' \
                else [s for a in node.args for s in _strings(a)]
            for name in names:
                for param in params:
                    events.append((_position(node), 'write', param, name))
    written: Dict[str, set] = {p: set() for p in params}
    rebound = set()
    for (line, _), kind, param, column in sorted(events, key=lambda e: e[0]):
        if param in rebound:
            continue
        if kind == 'rebind':
            rebound.add(param)
        elif kind == 'write':
            written[param].add(column)
        elif column not in written[param]:
            reads[param].columns.setdefault(column, line)
    return {p: r.columns for p, r in reads.items() if r.columns}


def column_uses(code: str) -> Dict[str, Optional[set]]:
    """
    Per input parameter, every column the block can read from it: None when the block uses
    the input other than as `param['x']` or `param[['x', 'y']]` (returns it, passes it on,
    calls a method on it), since any column may then be read. Uses after the parameter is
    bound again are not of the input.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return {}
    function = _decorated_function(tree)
    if function is None:
        return {}
    params = [a.arg for a in function.args.posonlyargs + function.args.args]
    parents = {}
    for node in ast.walk(function):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    rebound: Dict[str, Tuple[int, int]] = {}
    for node in ast.walk(function):
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name) and target.id in params:
                    position = (node.end_lineno or 0, 1 << 30)
                    rebound[target.id] = min(rebound.get(target.id, position), position)
    uses: Dict[str, Optional[set]] = {p: set() for p in params}
    for node in ast.walk(function):
        if not (isinstance(node, ast.Name) and node.id in params
                and isinstance(node.ctx, ast.Load)):
            continue
        if node.id in rebound and _position(node) > rebound[node.id]:
            continue
        if uses[node.id] is None:
            continue
        parent = parents.get(node)
        columns = _strings(parent.slice) if isinstance(parent, ast.Subscript) \
            and parent.value is node else []
        if columns and isinstance(parent.ctx, ast.Load):
            uses[node.id].update(columns)
        elif not (columns and isinstance(parent.ctx, ast.Store)):
            uses[node.id] = None
    return uses


# Reported when the columns no block reads are at least this share of a stored output of at
# least this size: smaller savings are not worth a finding.
UNUSED_SHARE = 0.5
UNUSED_MIN_BYTES = 1 << 20


@dataclass
class UnusedColumns:
    upstream: str
    readers: List[str]
    read: List[str]
    columns: int
    unused_bytes: int
    output_bytes: int

    def to_dict(self) -> Dict:
        share = round(100 * self.unused_bytes / self.output_bytes)
        readers = ' and '.join(self.readers)
        return dict(
            columns=self.columns,
            message=(
                f'{readers} read{"s" if len(self.readers) == 1 else ""} {len(self.read)} of '
                f'{self.columns} columns of {self.upstream} ({", ".join(self.read)}); the '
                f'other {self.columns - len(self.read)} are {share}% of its last stored '
                f'output ({_size(self.output_bytes)}). Selecting only these where '
                f'{self.upstream} loads its data reads and passes less.'
            ),
            output_bytes=self.output_bytes,
            read=self.read,
            readers=self.readers,
            unused_bytes=self.unused_bytes,
            upstream=self.upstream,
        )


def _size(n: int) -> str:
    for unit in ('bytes', 'KB', 'MB', 'GB'):
        if n < 1024 or unit == 'GB':
            return f'{n:.0f} {unit}' if unit == 'bytes' else f'{n:.1f} {unit}'
        n /= 1024
    return ''


# Footer sizes by (file, modification time): the compile status is polled.
_sizes: Dict[Tuple[str, float], Dict[str, int]] = {}


def _column_bytes(path: str) -> Dict[str, int]:
    """Each top-level column's compressed bytes in a Parquet file, from its footer."""
    key = (path, os.path.getmtime(path))
    if key not in _sizes:
        if len(_sizes) > 256:
            _sizes.clear()
        _sizes[key] = _read_column_bytes(path)
    return _sizes[key]


def _read_column_bytes(path: str) -> Dict[str, int]:
    import pyarrow.parquet as pq

    metadata = pq.read_metadata(path)
    sizes: Dict[str, int] = {}
    for group in range(metadata.num_row_groups):
        row_group = metadata.row_group(group)
        for index in range(row_group.num_columns):
            column = row_group.column(index)
            name = column.path_in_schema.split('.')[0]
            sizes[name] = sizes.get(name, 0) + column.total_compressed_size
    return {n: b for n, b in sizes.items() if not n.startswith('__index_level_')}


def unused(pipeline) -> List[UnusedColumns]:
    """Upstream blocks whose downstream blocks all read known columns, and few of them."""
    findings = []
    for source in pipeline.blocks_by_uuid.values():
        readers = [pipeline.get_block(u) for u in source.downstream_block_uuids or []]
        if not readers or any(r is None for r in readers):
            continue
        read: set = set()
        for reader in readers:
            language = str(getattr(reader.language, 'value', reader.language))
            if language != 'python':
                read = None
                break
            try:
                code = reader.content or ''
            except Exception:
                read = None
                break
            uses = column_uses(code)
            # The parameters in order, as column_uses keys them: the upstream blocks'
            # outputs arrive in that order.
            names = list(uses)
            position = (reader.upstream_block_uuids or []).index(source.uuid)
            if position >= len(names) or uses[names[position]] is None:
                read = None
                break
            read |= uses[names[position]]
        if not read:
            continue
        path = _stored_file(pipeline, source)
        if path is None:
            continue
        try:
            sizes = _column_bytes(path)
        except Exception:
            continue
        total = sum(sizes.values())
        unused_bytes = sum(b for n, b in sizes.items() if n not in read)
        if total < UNUSED_MIN_BYTES or unused_bytes < UNUSED_SHARE * total:
            continue
        findings.append(UnusedColumns(
            columns=len(sizes),
            output_bytes=total,
            read=sorted(c for c in read if c in sizes),
            readers=[r.uuid for r in readers],
            unused_bytes=unused_bytes,
            upstream=source.uuid,
        ))
    return findings


def _contract_columns(block) -> Optional[List[str]]:
    from mage_ai.data_preparation import contracts

    try:
        binding = contracts.binding_of(block)
        if binding is None or binding.output != 'output_0':
            return None
        contract = contracts.load_contract(binding.name, block.repo_path)
    except Exception:
        return None
    return [c.get('name') for c in contract.get('columns') or [] if c.get('name')]


# Runs looked at for a block's last output, newest first: a pipeline with thousands of runs
# is not walked at each compile status poll.
RECENT_RUNS = 20
CACHE_SECONDS = 30
_cache: Dict[Tuple[str, str], Tuple[float, Optional[List[str]]]] = {}


def _stored_columns(pipeline, block) -> Optional[List[str]]:
    """The columns of the block's most recent stored output_0, read from its footer."""
    import time

    key = (pipeline.uuid, block.uuid)
    cached = _cache.get(key)
    if cached and time.monotonic() - cached[0] < CACHE_SECONDS:
        return cached[1]
    path = _stored_file(pipeline, block)
    columns = None
    if path:
        try:
            import pyarrow.parquet as pq

            names = pq.read_schema(path).names
            # pandas keeps a named index as a column.
            columns = [n for n in names if not n.startswith('__index_level_')]
        except Exception:
            columns = None
    _cache[key] = (time.monotonic(), columns)
    return columns


_files: Dict[Tuple[str, str], Tuple[float, Optional[str]]] = {}


def _stored_file(pipeline, block) -> Optional[str]:
    """The block's most recent stored output_0 Parquet file: the editor's or a run's."""
    import time

    key = (pipeline.uuid, block.uuid)
    cached = _files.get(key)
    if cached and time.monotonic() - cached[0] < CACHE_SECONDS:
        return cached[1]
    path = _find_stored_file(pipeline, block)
    _files[key] = (time.monotonic(), path)
    return path


def _find_stored_file(pipeline, block) -> Optional[str]:
    from mage_ai.column_atlas.sources import DATAFRAME_FILE
    from mage_ai.data_preparation.models.variable import VARIABLE_DIR

    root = os.path.join(pipeline.variable_manager.pipeline_path(pipeline.uuid), VARIABLE_DIR)
    try:
        runs = sorted(
            (e for e in os.scandir(root) if e.is_dir() and e.name != block.uuid),
            key=lambda e: e.stat().st_mtime, reverse=True,
        )[:RECENT_RUNS]
    except OSError:
        runs = []
    # The editor's output, then each recent run's (scheduled runs store under a partition).
    candidates = [os.path.join(root, block.uuid, 'output_0', DATAFRAME_FILE)]
    for run in runs:
        candidates += glob.glob(os.path.join(
            glob.escape(run.path), '**', glob.escape(block.uuid), 'output_0', DATAFRAME_FILE,
        ), recursive=True)
    files = [f for f in candidates if os.path.isfile(f)]
    return max(files, key=os.path.getmtime) if files else None


def check(pipeline) -> List[ColumnFinding]:
    """Each Python block's reads of columns its upstream blocks do not give."""
    findings = []
    for block in pipeline.blocks_by_uuid.values():
        language = str(getattr(block.language, 'value', block.language))
        block_type = str(getattr(block.type, 'value', block.type))
        if language != 'python' or block_type not in CHECKED_TYPES:
            continue
        upstream = [pipeline.get_block(u) for u in block.upstream_block_uuids or []]
        if not upstream or any(u is None for u in upstream):
            continue
        try:
            code = block.content or ''
        except Exception:
            continue
        reads = column_reads(code)
        if not reads:
            continue
        function = _decorated_function(ast.parse(code))
        names = [a.arg for a in function.args.posonlyargs + function.args.args]
        for index, source in enumerate(upstream):
            if index >= len(names) or names[index] not in reads:
                continue
            columns = _contract_columns(source)
            origin = 'contract'
            if columns is None:
                columns = _stored_columns(pipeline, source)
                origin = 'last output'
            if not columns:
                continue
            read = reads[names[index]]
            missing = sorted(c for c in read if c not in columns)
            if missing:
                findings.append(ColumnFinding(
                    available=list(columns),
                    block=block.uuid,
                    line=min(read[c] for c in missing),
                    missing=missing,
                    severity='error' if origin == 'contract' else 'warning',
                    source=origin,
                    upstream=source.uuid,
                ))
    return findings
