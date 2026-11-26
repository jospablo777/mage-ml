"""
Pipeline compilation: checks and builds what a production run of the pipeline needs, in
every language, ahead of the run.

- Python blocks: a syntax check, and a warning for a block without its decorated function.
- R blocks: a parse check, and the R environment and packages they run with.
- Rust blocks: the binaries a run builds on demand: a chain of Rust blocks the first time
  a run plans it, a block the first time it runs alone (block/plans.py, rust/build.py).

SQL blocks are left as they are: their queries run in the database, which checks them.
Consecutive DuckDB SQL blocks of a stage chain in a production run (block/plans.py): the
summary lists those chains and the SQL blocks that stay out of them, with the reason.

It starts on its own when a pipeline or one of its blocks is saved (request), and from
the Compile button (start). Builds are cached, so compiling a compiled pipeline takes a
moment. A run does not wait for it: a run still builds whatever is missing.

The Rust units mirror a fused run: in each stage, consecutive Rust blocks form a chain
until a block whose own output is needed (plans.output_needed) or another connection
profile.
"""
import logging
import os
import re
import threading
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

# Seconds a save waits for more saves before compiling, so typing does not compile.
DEBOUNCE_SECONDS = 3.0

# Block types that do not run in a production run, or that compile on their own (dbt).
SKIPPED_TYPES = frozenset((
    'chart', 'dbt', 'global_data_product', 'group', 'markdown', 'pipeline', 'scratchpad',
))
# Python block types that need a function with their decorator.
DECORATED_TYPES = frozenset((
    'conditional', 'custom', 'data_exporter', 'data_loader', 'sensor', 'transformer',
))
DONE_STATES = ('cached', 'checked', 'compiled', 'failed')
COMPILED_LANGUAGES = ('python', 'r', 'rust')


class CompileError(Exception):
    """What a unit found wrong, one block per paragraph."""


@dataclass
class Unit:
    kind: str  # python_check, r_check, rust_chain, rust_block, r_environment, column_check
    label: str
    language: str
    block_uuids: List[str]
    state: str = 'pending'  # pending, compiling, checked, compiled, cached, failed
    seconds: float = 0.0
    error: Optional[str] = None
    # What may fail at run time without being certain to, such as a block without its
    # decorated function.
    warnings: List[str] = field(default_factory=list)


@dataclass
class Compilation:
    pipeline_uuid: str
    repo_path: str
    units: List[Unit]
    summary: Dict
    automatic: bool = False
    state: str = 'running'  # running, completed, failed, nothing
    last_compiled: Optional[Dict] = None
    started_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    # The code it compiles (fingerprint at the start), and whether a save came while it ran.
    fingerprint: Optional[str] = None
    rerun: bool = False

    def to_dict(self) -> Dict:
        done = sum(1 for u in self.units if u.state in DONE_STATES)
        current = next((u for u in self.units if u.state == 'compiling'), None)
        return dict(
            id=self.pipeline_uuid,
            pipeline_uuid=self.pipeline_uuid,
            state=self.state,
            automatic=self.automatic,
            started_at=self.started_at,
            finished_at=self.finished_at,
            total=len(self.units),
            done=done,
            current=current.label if current else None,
            units=[dict(u.__dict__) for u in self.units],
            summary=self.summary,
            last_compiled=self.last_compiled,
        )


_lock = threading.Lock()
_compilations: Dict[str, Compilation] = {}
_timers: Dict[str, threading.Timer] = {}


def _code(block) -> str:
    try:
        with open(block.file_path, encoding='utf-8') as file:
            return file.read()
    except (OSError, TypeError):
        return block.content or ''


def _rust_tests(block) -> bool:
    from mage_ai.data_preparation.models.block.rust import source as block_source

    try:
        return bool(block_source.analyze(_code(block), block.type).tests)
    except Exception:
        return False


def _profile(block) -> str:
    return (block.configuration or {}).get('data_provider_profile') or 'default'


def _language(block) -> str:
    return str(getattr(block.language, 'value', block.language) or '')


def _block_type(block) -> str:
    return str(getattr(block.type, 'value', block.type) or '')


def code_blocks(pipeline, languages=('python', 'r', 'rust', 'sql')) -> List:
    """The blocks a production run executes, in pipeline order."""
    return [
        b for b in (pipeline.blocks_by_uuid or {}).values()
        if _block_type(b) not in SKIPPED_TYPES and _language(b) in languages
    ]


def compiled_blocks(pipeline) -> List:
    """The blocks compiling checks or builds: Python, R and Rust."""
    return code_blocks(pipeline, COMPILED_LANGUAGES)


def _plural(count: int, word: str) -> str:
    return f'{count} {word}' if count == 1 else f'{count} {word}s'


def units_for(pipeline) -> List[Unit]:
    """What compiling the pipeline checks and builds, in the order it runs them."""
    from mage_ai.data_preparation.models.constants import BlockLanguage
    from mage_ai.orchestration import planning

    blocks = compiled_blocks(pipeline)
    units = []
    for language, name in (('python', 'Python'), ('r', 'R')):
        uuids = [b.uuid for b in blocks if _language(b) == language]
        if uuids:
            units.append(Unit(f'{language}_check',
                              f'Check {_plural(len(uuids), f"{name} block")}', language, uuids))

    plan = planning.plan_pipeline(pipeline)
    for group in plan.groups:
        for uuid in group:
            block = pipeline.get_block(uuid)
            if block is None or block.language != BlockLanguage.RUST:
                continue
            segment = plan.segment_of(uuid)
            if segment is None:
                units.append(Unit('rust_block', uuid, 'rust', [uuid]))
            elif segment['blocks'][0] == uuid:
                uuids = list(segment['blocks'])
                units.append(Unit('rust_chain', ' → '.join(uuids), 'rust', uuids))
    if any(_language(b) == 'r' for b in blocks):
        units.append(Unit('r_environment', 'R environment and packages', 'r', []))
    return units


def summary_for(pipeline) -> Dict:
    """
    The pipeline's chains by language and its language switches: each switch between
    blocks of different languages converts the data, so single-language chains run best.
    The chains, compiled segments and the reasons blocks do not chain come from the
    planner (planning.py).
    """
    from mage_ai.orchestration import fusion, planning

    blocks = pipeline.blocks_by_uuid
    languages = sorted({_language(b) for b in code_blocks(pipeline)})
    switches = sum(
        1 for block in blocks.values() for down in (block.downstream_blocks or [])
        if down.uuid in blocks and down.language != block.language
    )
    fused = fusion.fusion_enabled(pipeline)
    # Only batch Python pipelines run in stages.
    plan = planning.plan_pipeline(pipeline) \
        if str(getattr(pipeline.type, 'value', pipeline.type)) == 'python' else None
    chains = [
        dict(
            blocks=group,
            languages=sorted({
                str(getattr(blocks[uuid].language, 'value', blocks[uuid].language))
                for uuid in group if uuid in blocks
            }),
        )
        for group in (plan.groups if plan is not None and fused else [])
        if len(group) > 1
    ]
    # The languages in pipeline order, consecutive blocks of one language merged:
    # [python ×1, rust ×2, r ×1] for a Python block, two Rust blocks and an R block.
    flow = []
    for block in code_blocks(pipeline):
        language = _language(block)
        if flow and flow[-1]['language'] == language:
            flow[-1]['blocks'] += 1
        else:
            flow.append(dict(language=language, blocks=1))
    summary = dict(
        blocks=len(blocks),
        chains=chains,
        flow=flow,
        fusion=fused,
        languages=languages,
        language_switches=switches,
    )
    if plan is not None and fused:
        summary['segments'] = plan.segments
        summary['findings'] = _with_worker_names(pipeline, plan.findings)
        sql_chains, sql_left_out = _sql_chains(pipeline, plan)
        if sql_chains or sql_left_out:
            summary.update(sql_chains=sql_chains, sql_left_out=sql_left_out)
    if str(getattr(pipeline.type, 'value', pipeline.type)) == 'streaming':
        from mage_ai.streaming.plan import stream_plan

        summary['stream'] = stream_plan(pipeline)
    # Columns blocks read that their upstream blocks do not give (columns.py).
    try:
        from mage_ai.orchestration import columns

        found = [finding.to_dict() for finding in columns.check(pipeline)]
    except Exception:
        found = []
    if found:
        summary['columns'] = found
    # Columns no downstream block reads, measured in the stored output.
    try:
        unused = [finding.to_dict() for finding in columns.unused(pipeline)]
    except Exception:
        unused = []
    if unused:
        summary['unused_columns'] = unused
    return summary


def _with_worker_names(pipeline, findings: List[Dict]) -> List[Dict]:
    """The planner's findings, a switch between Python workers said with the names people
    gave them ("Feast worker") rather than their aliases."""
    from mage_ai.data_preparation import environments

    if not any(f.get('code') == 'worker_switch' for f in findings):
        return findings
    try:
        workers = environments.named_workers(pipeline.repo_path)
    except environments.PipelineEnvironmentError:
        return findings

    def name(uuid):
        alias = environments.worker_selection(pipeline.get_block(uuid))
        if alias in (None, 'mage'):
            return "MageML's Python"
        worker = workers.get(alias)
        return worker.display_name if worker else alias

    named = []
    for finding in findings:
        if finding.get('code') == 'worker_switch':
            finding = dict(finding, reason=(
                f'{finding["block"]} runs in {name(finding["block"])} and {finding["next"]} '
                f'in {name(finding["next"])}: the data moves between two processes.'
            ))
        named.append(finding)
    return named


def _sql_chains(pipeline, plan):
    """
    The SQL chains of a production run (views, then the block that reads them), and each
    SQL block followed in its stage by a SQL block it does not chain with, with the
    planner's reason.
    """
    from mage_ai.data_preparation.models.constants import BlockLanguage

    chains = [
        dict(blocks=list(segment['blocks']), views=list(segment['blocks'][:-1]))
        for segment in plan.segments if segment['kind'] == 'sql'
    ]
    left_out = {}

    def sql(uuid):
        block = pipeline.get_block(uuid)
        return block is not None and block.language == BlockLanguage.SQL

    for finding in plan.findings:
        if finding['level'] == 'segment' and sql(finding['block']) and sql(finding['next']):
            left_out.setdefault(finding['block'], finding['reason'])
    return chains, [dict(block=uuid, reason=reason) for uuid, reason in left_out.items()]


def _check_python(pipeline, unit: Unit) -> None:
    import ast

    errors = []
    for uuid in unit.block_uuids:
        block = pipeline.get_block(uuid)
        code = _code(block)
        try:
            tree = ast.parse(code, filename=block.file_path or uuid)
        except SyntaxError as error:
            errors.append(
                f'{uuid}, line {error.lineno}: {error.msg}'
                + (f'\n    {error.text.rstrip()}' if error.text else ''),
            )
            continue
        block_type = _block_type(block)
        if block_type in DECORATED_TYPES and not any(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
                (isinstance(d, ast.Name) and d.id == block_type)
                or (isinstance(d, ast.Call) and getattr(d.func, 'id', None) == block_type)
                for d in node.decorator_list
            )
            for node in ast.walk(tree)
        ):
            unit.warnings.append(
                f'{uuid}: no function decorated with @{block_type}, so the block has '
                'nothing to run.',
            )
    if errors:
        raise CompileError('\n\n'.join(errors))
    unit.warnings.extend(_missing_imports(pipeline, unit.block_uuids))
    errors = _dynamic_blocks_in_workers(pipeline, unit.block_uuids)
    if errors:
        raise CompileError('\n\n'.join(errors))
    unit.state = 'checked'


def _dynamic_blocks_in_workers(pipeline, uuids: List[str]) -> List[str]:
    """Dynamic and replicated blocks set to run in a Python worker: their runs would fail."""
    from mage_ai.data_preparation import environments
    from mage_ai.data_preparation.models.block.dynamic.utils import (
        is_dynamic_block,
        is_dynamic_block_child,
        is_replicated_block,
    )

    try:
        workers = environments.named_workers(pipeline.repo_path)
    except environments.PipelineEnvironmentError:
        return []
    problems = []
    for uuid in uuids:
        block = pipeline.get_block(uuid)
        worker = workers.get(environments.worker_selection(block) or '')
        if worker is not None and (is_dynamic_block(block) or is_dynamic_block_child(block)
                                   or is_replicated_block(block)):
            problems.append(environments.dynamic_block_problem(uuid, worker))
    return problems


def _imported(tree) -> List[str]:
    """The top-level modules a block imports, but those inside a try: an import there may
    be optional, as when a block falls back to another library."""
    import ast

    found = []

    def visit(node, guarded):
        for child in ast.iter_child_nodes(node):
            inside = guarded or isinstance(child, ast.Try)
            if isinstance(child, ast.Import) and not guarded:
                found.extend(alias.name.split('.')[0] for alias in child.names)
            elif isinstance(child, ast.ImportFrom) and not guarded and not child.level:
                found.append((child.module or '').split('.')[0])
            visit(child, inside)

    visit(tree, False)
    return [m for m in dict.fromkeys(found) if m]


# Prints, as JSON, the modules given that this Python cannot import.
_FIND_SPECS = (
    'import importlib.util, json, sys\n'
    'print(json.dumps([m for m in sys.argv[1:] if importlib.util.find_spec(m) is None]))'
)


def _absent(python: Optional[str], modules: List[str], repo_path: str) -> List[str]:
    """The modules an interpreter cannot import (MageML's own when python is None)."""
    import importlib.util
    import json
    import subprocess

    if not modules:
        return []
    if python is None:
        return [m for m in modules if importlib.util.find_spec(m) is None]
    path = os.pathsep.join([repo_path, os.path.dirname(repo_path)])
    try:
        result = subprocess.run(
            [python, '-I', '-c', _FIND_SPECS, *modules], capture_output=True, text=True,
            timeout=30, env=dict(os.environ, PYTHONPATH=path),
        )
        return json.loads(result.stdout) if result.returncode == 0 else []
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return []


def _missing_imports(pipeline, uuids: List[str]) -> List[str]:
    """
    Each Python block's imports its environment does not have: its Python worker, or
    MageML's own Python. Warnings, not errors (an import may be optional); each names the
    workers that have the module and the two ways to fix it.
    """
    import ast
    import sys

    from mage_ai.data_preparation import environments

    repo_path = pipeline.repo_path
    try:
        workers = environments.named_workers(repo_path)
    except environments.PipelineEnvironmentError:
        return []
    project = {p.stem for p in Path(repo_path).iterdir()} | {Path(repo_path).name}
    # Where each block runs: a worker's alias, or None for MageML's Python.
    needs: Dict[Optional[str], Dict[str, List[str]]] = {}
    for uuid in uuids:
        block = pipeline.get_block(uuid)
        try:
            tree = ast.parse(_code(block))
        except SyntaxError:
            continue
        selection = environments.worker_selection(block)
        if selection not in (None, 'mage') and selection not in workers:
            continue
        if selection is None and getattr(pipeline, 'environment', None):
            # A requirements environment of the pipeline: built at run time.
            continue
        alias = None if selection in (None, 'mage') else selection
        modules = [
            m for m in _imported(tree)
            if m not in sys.stdlib_module_names and m not in project
            and m not in ('mage_ai', '__future__')
        ]
        if modules:
            needs.setdefault(alias, {})[uuid] = modules

    def interpreter(alias):
        return None if alias is None else str(workers[alias].python_executable)

    def name(alias):
        return "MageML's Python" if alias is None else \
            f'{workers[alias].display_name} ({alias})'

    warnings = []
    absent_in: Dict[Optional[str], set] = {}
    for alias, blocks in needs.items():
        if alias is not None and not workers[alias].prepared:
            continue
        wanted = sorted({m for modules in blocks.values() for m in modules})
        absent_in[alias] = set(_absent(interpreter(alias), wanted, repo_path))
    missing_anywhere = sorted(set().union(*absent_in.values())) if absent_in else []
    # Where the missing modules can be found: MageML's Python and each prepared worker.
    found_in: Dict[str, List[str]] = {m: [] for m in missing_anywhere}
    if missing_anywhere:
        candidates = [None] + [a for a, w in workers.items() if w.prepared]
        for alias in candidates:
            absent = set(_absent(interpreter(alias), missing_anywhere, repo_path))
            for module in missing_anywhere:
                if module not in absent:
                    found_in[module].append(alias)
    for alias, blocks in needs.items():
        for uuid, modules in blocks.items():
            for module in modules:
                if module not in absent_in.get(alias, ()):
                    continue
                others = [name(a) for a in found_in.get(module, []) if a != alias]
                where = f'; {", ".join(others)} has it' if others else ''
                choose = ' or choose that worker for the block' if others else ''
                if alias is None:
                    fix = f'install it in the project (requirements.txt){choose}'
                else:
                    fix = (f'add it to {workers[alias].relative_definition()}/pyproject.toml '
                           f'and run uv lock{choose}')
                warnings.append(
                    f'{uuid} imports {module}, which {name(alias)} does not have{where}: {fix}.'
                )
    warnings += _mageml_imports_in_workers(pipeline, uuids, workers)
    return warnings


# The worker gives blocks the decorators; MageML's templates import them only outside one.
_PROVIDED = ('mage_ai.data_preparation.decorators',)
# Imports each module given and prints "<module>\t<missing module>" for each that fails on
# a module the interpreter does not have.
_IMPORT_PROBE = (
    'import importlib, sys\n'
    'for name in sys.argv[1:]:\n'
    '    try:\n'
    '        importlib.import_module(name)\n'
    '    except ModuleNotFoundError as error:\n'
    '        print(f"{name}\\t{error.name}")\n'
    '    except Exception:\n'
    '        pass\n'
)


def _mageml_imports_in_workers(pipeline, uuids: List[str], workers) -> List[str]:
    """
    MageML's own modules a block in a Python worker imports (a connector from a block
    template), imported in the worker: MageML's code is on its path, its packages are not.
    """
    import ast
    import subprocess

    from mage_ai.data_preparation import environments

    wanted: Dict[str, Dict[str, List[str]]] = {}
    for uuid in uuids:
        block = pipeline.get_block(uuid)
        alias = environments.worker_selection(block)
        worker = workers.get(alias) if alias else None
        if worker is None or not worker.prepared:
            continue
        try:
            tree = ast.parse(_code(block))
        except SyntaxError:
            continue
        modules = sorted({
            name for name in _imported_modules(tree)
            if name.split('.')[0] == 'mage_ai' and name not in _PROVIDED
        })
        if modules:
            wanted.setdefault(alias, {})[uuid] = modules
    warnings = []
    mage_root = str(Path(environments.__file__).resolve().parents[2])
    for alias, blocks in wanted.items():
        worker = workers[alias]
        modules = sorted({m for found in blocks.values() for m in found})
        try:
            probe = subprocess.run(
                [str(worker.python_executable), '-c', _IMPORT_PROBE, *modules],
                capture_output=True, text=True, timeout=120, cwd=pipeline.repo_path,
                env={**os.environ, 'PYTHONPATH': mage_root},
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        missing = dict(line.split('\t', 1) for line in probe.stdout.splitlines() if '\t' in line)
        for uuid, found in blocks.items():
            for module in found:
                if module not in missing:
                    continue
                package = missing[module].split('.')[0]
                warnings.append(
                    f"{uuid} imports MageML's {module}, which needs {package}; "
                    f'{worker.display_name} ({alias}) does not have it: add {package} to '
                    f'{worker.relative_definition()}/pyproject.toml and run uv lock, or run '
                    "the block in MageML's Python."
                )
    return warnings


def _imported_modules(tree) -> List[str]:
    """The full names of the modules a file imports."""
    import ast

    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            names.append(node.module)
    return names


# Parses each file given; prints "<path>\t<message>" for each that does not parse, with
# the message's lines joined by \x1f.
_R_PARSE = (
    'for (path in commandArgs(trailingOnly = TRUE)) tryCatch('
    'parse(text = readLines(path, warn = FALSE), keep.source = FALSE), '
    'error = function(e) cat(path, "\\t", gsub("\\n", "\\x1f", conditionMessage(e)), '
    '"\\n", sep = ""))'
)
# <text>:3:32: unexpected ')'
_R_POSITION = re.compile(r'^<text>:(\d+):(\d+): ?')


def _check_r(pipeline, unit: Unit) -> None:
    import os
    import subprocess

    from mage_ai.data_preparation.models.block.r import runtime

    paths = {}
    for uuid in unit.block_uuids:
        block = pipeline.get_block(uuid)
        if block.file_path and os.path.isfile(block.file_path):
            paths[os.path.abspath(block.file_path)] = uuid
    if not paths:
        unit.state = 'checked'
        return
    rscript = runtime.r_config(pipeline.repo_path).rscript
    result = subprocess.run(
        [rscript, '--vanilla', '-e', _R_PARSE, *paths],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        raise CompileError(f'R could not check the blocks:\n{result.stderr.strip()}')
    errors = []
    for line in result.stdout.splitlines():
        path, _, message = line.partition('\t')
        uuid = paths.get(os.path.abspath(path), path)
        lines = message.split('\x1f')
        position = _R_POSITION.match(lines[0])
        if position:
            lines[0] = f'line {position.group(1)}, column {position.group(2)}: ' \
                + lines[0][position.end():]
        errors.append(f'{uuid}: ' + '\n    '.join(lines).rstrip())
    if errors:
        raise CompileError('\n\n'.join(errors))
    unit.state = 'checked'


def _compile_unit(pipeline, unit: Unit) -> None:
    from mage_ai.data_preparation.models.block.rust import build as rust_build

    if unit.kind == 'python_check':
        _check_python(pipeline, unit)
    elif unit.kind == 'r_check':
        _check_r(pipeline, unit)
    elif unit.kind == 'rust_chain':
        members = [
            rust_build.ChainMember(code=_code(pipeline.get_block(uuid)),
                                   block_type=pipeline.get_block(uuid).type, block_uuid=uuid)
            for uuid in unit.block_uuids
        ]
        built = rust_build.build_chain(rust_build.prepare_chain(members, pipeline.repo_path))
        unit.state = 'cached' if built.cached else 'compiled'
    elif unit.kind == 'rust_block':
        block = pipeline.get_block(unit.block_uuids[0])
        prepared = rust_build.prepare(_code(block), block.type, block.uuid, pipeline.repo_path)
        built = rust_build.build(prepared, stream=False)
        unit.state = 'cached' if built.cached else 'compiled'
    elif unit.kind == 'r_environment':
        from mage_ai.data_preparation.models.block.r import runtime

        runtime.prepare(runtime.r_config(pipeline.repo_path))
        unit.state = 'compiled'


def code_fingerprint(pipeline) -> str:
    """The pipeline's part of a compilation: the Python, R and Rust blocks' code,
    configuration and upstream blocks, and the fusion settings."""
    import hashlib
    import json

    from mage_ai.orchestration import fusion

    digest = hashlib.sha256(fusion.block_fusion_mode(pipeline).encode())
    digest.update(b'keep' if getattr(pipeline, 'keep_block_outputs', False) else b'plan')
    for block in sorted(compiled_blocks(pipeline), key=lambda b: b.uuid):
        digest.update(f'\0{block.uuid}\0{_language(block)}\0{_block_type(block)}\0'.encode())
        digest.update(json.dumps(
            [block.configuration or {}, list(block.upstream_block_uuids or [])],
            sort_keys=True, default=str,
        ).encode())
        digest.update(_code(block).encode())
    return digest.hexdigest()[:32]


def environment_fingerprint(pipeline) -> str:
    """The Rust workspace's part: its manifest, toolchain and locked dependencies, without
    the block crates that compiling itself adds to Cargo.lock."""
    import hashlib

    from mage_ai.data_preparation.models.block.rust import build as rust_build
    from mage_ai.data_preparation.models.block.rust import workspace as ws

    digest = hashlib.sha256()
    try:
        root = ws.workspace_root(pipeline.repo_path)
        for name in ('Cargo.toml', 'rust-toolchain.toml'):
            path = root / name
            if path.is_file():
                digest.update(path.read_bytes())
        lock = root / 'Cargo.lock'
        if lock.is_file():
            digest.update(rust_build._lock_fingerprint(lock).encode())
    except Exception:
        pass
    return digest.hexdigest()[:32]


def fingerprint(pipeline) -> str:
    """What a compilation covers; a changed fingerprint means it no longer holds."""
    return f'{code_fingerprint(pipeline)}:{environment_fingerprint(pipeline)}'


def _record_path(pipeline) -> str:
    import os

    from mage_ai.settings.repo import get_variables_dir

    return os.path.join(get_variables_dir(repo_path=pipeline.repo_path), 'compilations',
                        f'{pipeline.uuid}.json')


def _save_record(compilation: Compilation, pipeline, state: str) -> None:
    import json
    import os

    path = _record_path(pipeline)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    record = dict(compilation.to_dict(), state=state, fingerprint=compilation.fingerprint)
    with open(path + '.tmp', 'w', encoding='utf-8') as file:
        json.dump(record, file, default=str)
    os.replace(path + '.tmp', path)


def _read_record(pipeline) -> Optional[Dict]:
    import json

    try:
        with open(_record_path(pipeline), encoding='utf-8') as file:
            record = json.load(file)
    except (OSError, ValueError):
        return None
    return record if isinstance(record, dict) else None


def last_compiled(pipeline, record: Optional[Dict] = None) -> Optional[Dict]:
    """
    The pipeline's last finished compilation, kept across restarts: when it finished, its
    state and whether the code still matches it (up_to_date). None when it never compiled.
    """
    record = record or _read_record(pipeline)
    if record is None:
        return None
    return dict(
        finished_at=record.get('finished_at'),
        state=record.get('state'),
        units=len(record.get('units') or []),
        up_to_date=record.get('state') == 'completed'
        and record.get('fingerprint') == fingerprint(pipeline),
    )


def status(pipeline) -> Dict:
    """
    What the editor shows: the running compilation; else the last one while the code still
    matches it, with each unit's result and error; else what compiling would build now.
    """
    current = latest(pipeline)
    record = _read_record(pipeline)
    last = last_compiled(pipeline, record)
    # The summary describes the pipeline as it is now, whichever compilation is shown.
    summary = summary_for(pipeline)
    if current is not None and current.state == 'running':
        return dict(current.to_dict(), last_compiled=last, summary=summary)
    code = fingerprint(pipeline)
    if current is not None and current.fingerprint == code:
        return dict(current.to_dict(), last_compiled=last, summary=summary)
    if record is not None and record.get('fingerprint') == code:
        shown = {k: v for k, v in record.items() if k != 'fingerprint'}
        return dict(shown, last_compiled=last, summary=summary)
    units = units_for(pipeline)
    return dict(
        Compilation(
            pipeline_uuid=pipeline.uuid,
            repo_path=pipeline.repo_path,
            units=units,
            summary=summary,
            state='none' if units else 'nothing',
            started_at=None,
        ).to_dict(),
        last_compiled=last,
    )


def _run(compilation: Compilation, pipeline) -> None:
    code = (compilation.fingerprint or ':').split(':')[0]
    for unit in compilation.units:
        unit.state = 'compiling'
        started = time.monotonic()
        try:
            _compile_unit(pipeline, unit)
        except Exception as error:
            unit.state = 'failed'
            unit.error = str(error) or traceback.format_exc()[-2000:]
        unit.seconds = round(time.monotonic() - started, 2)
    # A block that reads a column its upstream block's contract does not list fails the
    # compilation; one inferred from a stored output only warns (columns.py).
    try:
        from mage_ai.orchestration import columns

        found = columns.check(pipeline)
    except Exception:
        found = []
    if found:
        declared = [f for f in found if f.severity == 'error']
        compilation.units.append(Unit(
            kind='column_check',
            label='Columns between blocks',
            language='python',
            block_uuids=sorted({f.block for f in found}),
            state='failed' if declared else 'checked',
            error='\n'.join(f.to_dict()['message'] for f in declared) or None,
            warnings=[f.to_dict()['message'] for f in found if f.severity != 'error'],
        ))
    compilation.finished_at = time.time()
    # The code as it started, the Rust workspace as the builds left it: a build adds its
    # crate to Cargo.lock, which is no change to what was compiled.
    compilation.fingerprint = f'{code}:{environment_fingerprint(pipeline)}'
    state = 'failed' if any(u.state == 'failed' for u in compilation.units) else 'completed'
    # The record first, so whoever sees the compilation finish also sees it recorded.
    try:
        _save_record(compilation, pipeline, state)
    except Exception as error:
        logging.getLogger(__name__).warning(
            'Pipeline %s compiled, but its compilation could not be recorded: %s',
            compilation.pipeline_uuid, error,
        )
    compilation.state = state
    if compilation.rerun:
        # The code changed while it compiled: compile what was saved since.
        request(compilation.pipeline_uuid, compilation.repo_path)


def start(pipeline, automatic: bool = False, wait: bool = False) -> Compilation:
    """Compiles the pipeline in the background; returns its compilation, the running one
    when one is running."""
    key = f'{pipeline.repo_path}:{pipeline.uuid}'
    code = fingerprint(pipeline)
    with _lock:
        current = _compilations.get(key)
        if current is not None and current.state == 'running':
            if (current.fingerprint or ':').split(':')[0] != code.split(':')[0]:
                current.rerun = True
            return current
        units = units_for(pipeline)
        compilation = Compilation(
            pipeline_uuid=pipeline.uuid,
            repo_path=pipeline.repo_path,
            units=units,
            summary=summary_for(pipeline),
            automatic=automatic,
            fingerprint=code,
        )
        if not units:
            compilation.state = 'nothing'
            compilation.finished_at = time.time()
        _compilations[key] = compilation
    if units:
        thread = threading.Thread(target=_run, args=(compilation, pipeline), daemon=True)
        thread.start()
        if wait:
            thread.join()
    return compilation


def latest(pipeline) -> Optional[Compilation]:
    return _compilations.get(f'{pipeline.repo_path}:{pipeline.uuid}')


def request(pipeline_uuid: str, repo_path: str) -> None:
    """After a save: compiles the pipeline once saves stop for DEBOUNCE_SECONDS, when it
    has anything to compile."""
    key = f'{repo_path}:{pipeline_uuid}'

    def compile_now():
        from mage_ai.data_preparation.models.pipeline import Pipeline

        with _lock:
            _timers.pop(key, None)
        try:
            pipeline = Pipeline.get(pipeline_uuid, repo_path=repo_path)
            if units_for(pipeline):
                start(pipeline, automatic=True)
        except Exception:
            # Compiling ahead is a convenience: a run still compiles what it needs.
            pass

    with _lock:
        timer = _timers.pop(key, None)
        if timer is not None:
            timer.cancel()
        timer = threading.Timer(DEBOUNCE_SECONDS, compile_now)
        timer.daemon = True
        _timers[key] = timer
        timer.start()


def request_for(pipeline) -> None:
    """After the pipeline or one of its blocks is saved: compile it ahead when it has
    Python, R or Rust blocks."""
    if pipeline is None or not getattr(pipeline, 'uuid', None):
        return
    if compiled_blocks(pipeline):
        request(pipeline.uuid, pipeline.repo_path)


def compile_active_pipelines(repo_path: str) -> Optional[threading.Thread]:
    """
    At server start: compiles, one at a time in the background, the pipelines with an
    active trigger whose last compilation is missing or no longer matches their code, so
    scheduled runs do not build on their first run. MAGE_COMPILE_ON_START=off turns it off.
    """
    import os

    if os.getenv('MAGE_COMPILE_ON_START', 'on').lower() in ('off', '0', 'false'):
        return None

    def compile_all():
        from mage_ai.data_preparation.models.pipeline import Pipeline
        from mage_ai.data_preparation.models.triggers import ScheduleStatus
        from mage_ai.orchestration.db import db_connection
        from mage_ai.orchestration.db.models.schedules import PipelineSchedule

        try:
            db_connection.start_session()
            uuids = sorted({
                schedule.pipeline_uuid for schedule in PipelineSchedule.query.filter(
                    PipelineSchedule.status == ScheduleStatus.ACTIVE,
                ).all()
                if not schedule.repo_path or schedule.repo_path == repo_path
            })
        except Exception as error:
            logging.getLogger(__name__).warning(
                'Could not list the active pipelines to compile: %s', error,
            )
            return
        for uuid in uuids:
            try:
                pipeline = Pipeline.get(uuid, repo_path=repo_path, check_if_exists=True)
                if pipeline is None or not compiled_blocks(pipeline):
                    continue
                last = last_compiled(pipeline)
                if last and last['up_to_date']:
                    continue
                start(pipeline, automatic=True, wait=True)
            except Exception as error:
                logging.getLogger(__name__).warning(
                    'Could not compile pipeline %s: %s', uuid, error,
                )

    thread = threading.Thread(target=compile_all, daemon=True, name='mage-compile-on-start')
    thread.start()
    return thread
