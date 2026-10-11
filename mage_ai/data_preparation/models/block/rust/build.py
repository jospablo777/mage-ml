"""
Building and checking Rust blocks with cargo.

A build compiles the block's generated crate in the project's workspace, with the shared
target directory, and copies the binary to a cache keyed by everything that determines
it. A run finds a cached binary without starting cargo. A check runs `cargo check` and
returns the compiler's diagnostics in the block's own lines, for the editor.
"""
import hashlib
import json
import os
import re
import shutil
import subprocess
import threading
import time
import tomllib
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Callable, Dict, List, Optional

from mage_ai.data_preparation.models.block.rust import source as block_source
from mage_ai.data_preparation.models.block.rust import workspace as ws
from mage_ai.data_preparation.models.constants import BlockType

BUILD_TIMEOUT_SECONDS = int(os.getenv('MAGE_RUST_BUILD_TIMEOUT_SECONDS') or 1800)
# Cached binaries kept per block: the current one and the one before it.
KEEP_PER_BLOCK = 2
BINARY_CACHE_MB = int(os.getenv('MAGE_RUST_BINARY_CACHE_MB') or 5000)
# One build or check at a time per process; cargo also locks the target directory
# across processes.
_LOCK = threading.Lock()


class RustBuildError(Exception):
    def __init__(self, message: str, diagnostics: Optional[List['Diagnostic']] = None):
        super().__init__(message)
        self.diagnostics = diagnostics or []


@dataclass
class Diagnostic:
    level: str
    message: str
    # In the block's file; None for a message about the generated main or a dependency.
    line: Optional[int] = None
    column: Optional[int] = None
    end_line: Optional[int] = None
    end_column: Optional[int] = None
    code: Optional[str] = None
    # The compiler's full message, with the block's file name in it.
    rendered: str = ''

    def to_dict(self) -> Dict:
        return dict(
            code=self.code,
            column=self.column,
            end_column=self.end_column,
            end_line=self.end_line,
            level=self.level,
            line=self.line,
            message=self.message,
            rendered=self.rendered,
        )


@dataclass
class Built:
    binary: Path
    cached: bool
    seconds: float
    warnings: List[Diagnostic] = field(default_factory=list)


def _cargo() -> str:
    cargo = shutil.which('cargo')
    if cargo is None:
        home = Path(os.getenv('CARGO_HOME') or Path.home() / '.cargo') / 'bin' / 'cargo'
        if home.exists():
            return str(home)
        raise RustBuildError(
            'Rust blocks need Rust: install it with rustup (https://rustup.rs), or use the '
            'Mage Docker image, which has it.',
        )
    return cargo


@lru_cache(maxsize=8)
def _rustc_version(root: str) -> str:
    # In the workspace, rustup selects the toolchain of rust-toolchain.toml.
    result = subprocess.run(
        [str(Path(_cargo()).with_name('rustc')), '-vV'],
        capture_output=True,
        cwd=root,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        raise RustBuildError(f'Could not run rustc: {result.stderr.strip()}')
    return result.stdout.strip()


def binary_cache_dir() -> Path:
    return ws.target_dir().parent / 'bin'


def _environment() -> Dict[str, str]:
    env = dict(os.environ)
    env['CARGO_TARGET_DIR'] = str(ws.target_dir())
    env.setdefault('CARGO_TERM_COLOR', 'always')
    return env


# Notes of rustc that point at files in the target directory.
_NOISE = re.compile(
    r'^.*(the full name for the type has been written to|consider using `--verbose` to '
    r'print the full type name).*\n?',
    re.M,
)


def relabel(text: str, prepared: 'Prepared') -> str:
    """Names the block's file as the user knows it, and the generated main as such."""
    directory = prepared.main_path.parent
    for path, label in (
        (directory / block_source.MAIN_FILE, block_source.MAIN_LABEL),
        (prepared.main_path, prepared.label),
    ):
        text = text.replace(str(path), label)
        text = text.replace(os.path.relpath(path, prepared.workspace.root), label)
    text = text.replace(os.path.relpath(prepared.workspace.sdk, prepared.workspace.root), 'mage')
    return _NOISE.sub('', text)


def parse_messages(stdout: str, prepared: 'Prepared') -> List[Diagnostic]:
    """The compiler's messages about the block's crate, in the block's lines."""
    generated = os.path.normpath(os.path.relpath(prepared.main_path, prepared.workspace.root))
    diagnostics = []
    for line in stdout.splitlines():
        if not line.startswith('{'):
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if item.get('reason') != 'compiler-message':
            continue
        if (item.get('target') or {}).get('name') != prepared.crate:
            continue
        message = item.get('message') or {}
        level = message.get('level') or 'error'
        text = message.get('message') or ''
        if level == 'failure-note' or not text or text.startswith('aborting due to'):
            continue
        if text.endswith('warning emitted') or text.endswith('warnings emitted'):
            continue
        diagnostic = Diagnostic(
            code=(message.get('code') or {}).get('code'),
            level=level,
            message=text,
            rendered=relabel(message.get('rendered') or '', prepared),
        )
        for span in message.get('spans') or []:
            if not span.get('is_primary'):
                continue
            if os.path.normpath(span.get('file_name', '')) != generated:
                # In the generated main: the block function's signature does not fit.
                continue
            if span['line_start'] > prepared.source.lines:
                continue
            diagnostic.line = span['line_start']
            diagnostic.column = span['column_start']
            diagnostic.end_line = span['line_end']
            diagnostic.end_column = span['column_end']
            break
        if diagnostic.line is None and level == 'error':
            # A signature error: point at the block function.
            diagnostic.line = prepared.function_line
        diagnostics.append(diagnostic)
    return diagnostics


def _lock_fingerprint(lock: Path) -> str:
    """
    Cargo.lock without the entries of the block crates themselves. Every block is a
    workspace member, so building one adds its entry to the lock; with the whole file in
    the key, adding or renaming any block rebuilt every other block on its next run.
    Nothing depends on a block crate, so the other entries say all a block compiles from.
    """
    try:
        text = lock.read_text(encoding='utf-8')
    except FileNotFoundError:
        return '<missing>'
    try:
        packages = tomllib.loads(text).get('package') or []
    except tomllib.TOMLDecodeError:
        return hashlib.sha256(text.encode()).hexdigest()
    kept = [
        package for package in packages
        if not (str(package.get('name', '')).startswith('block_') and 'source' not in package)
    ]
    return hashlib.sha256(json.dumps(kept, sort_keys=True).encode()).hexdigest()


def _cache_key(workspace: ws.Workspace, crate: str, main: str, sdk: str) -> str:
    """Everything that determines the binary: code, dependencies, compiler and runtime."""
    return ws.digest_files(
        [workspace.cargo_toml, workspace.toolchain_toml],
        dict(
            crate=crate,
            lock=_lock_fingerprint(workspace.cargo_lock),
            main=main,
            rustc=_rustc_version(str(workspace.root)),
            sdk=sdk,
            target=str(ws.target_dir()),
        ),
    )


@dataclass
class Prepared:
    workspace: ws.Workspace
    crate: str
    main_path: Path
    source: block_source.BlockSource
    key: str
    label: str
    function_line: Optional[int] = None
    # A chain: its blocks and their crate names, and the text its cache key covers.
    members: List = field(default_factory=list)
    key_text: Optional[Callable[[], str]] = None


def prepare(
    code: str,
    block_type: BlockType,
    block_uuid: str,
    repo_path: str,
    label: Optional[str] = None,
) -> Prepared:
    source = block_source.analyze(code, block_type)
    workspace = ws.init(repo_path)
    sdk = ws.sync_sdk(workspace)
    crate = ws.crate_name(block_uuid)
    main = block_source.main_rs(source)
    generated = block_source.generated_main(source)
    directory = ws.write_block_crate(
        workspace, crate, {'main.rs': main, block_source.MAIN_FILE: generated},
    )
    key = _cache_key(workspace, crate, main + generated, sdk)
    return Prepared(
        crate=crate,
        key=key,
        label=label or f'{block_uuid}.rs',
        function_line=block_source.function_line(code, source.function),
        main_path=directory / 'src' / 'main.rs',
        source=source,
        workspace=workspace,
    )


def _run_cargo(
    arguments: List[str], prepared: Prepared, stream: bool,
) -> subprocess.CompletedProcess:
    command = [
        _cargo(), *arguments, '-p', prepared.crate,
        '--message-format=json-diagnostic-rendered-ansi',
    ]
    process = subprocess.Popen(
        command,
        cwd=prepared.workspace.root,
        env=_environment(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        errors='replace',
        start_new_session=hasattr(os, 'killpg'),
    )
    # One reader per pipe: a reader that shares a pipe can take the bytes another is
    # waiting for, and cargo then blocks writing to the other pipe.
    stdout_parts: List[str] = []
    stderr_lines: List[str] = []

    def read_stdout():
        stdout_parts.append(process.stdout.read())

    def read_stderr():
        for line in process.stderr:
            stderr_lines.append(line)
            # Progress such as "Compiling polars v0.55.2", useful on a first build.
            if stream and line.lstrip().startswith(('Compiling', 'Downloaded', 'Updating')):
                print(line.rstrip(), flush=True)

    readers = [
        threading.Thread(target=read_stdout, daemon=True),
        threading.Thread(target=read_stderr, daemon=True),
    ]
    for reader in readers:
        reader.start()
    try:
        process.wait(timeout=BUILD_TIMEOUT_SECONDS)
    except BaseException:
        _kill(process)
        raise
    for reader in readers:
        reader.join(timeout=30)
    stdout = ''.join(stdout_parts)
    return subprocess.CompletedProcess(command, process.returncode, stdout, ''.join(stderr_lines))


def _kill(process: subprocess.Popen) -> None:
    try:
        if hasattr(os, 'killpg'):
            os.killpg(process.pid, 9)
        else:
            process.kill()
    except (ProcessLookupError, PermissionError):
        pass
    process.wait()


def _chain_errors(stdout: str) -> List[str]:
    """The compiler's errors in any file of a chain's crate, rendered."""
    errors = []
    for line in stdout.splitlines():
        if not line.startswith('{'):
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        message = item.get('message') or {}
        if item.get('reason') == 'compiler-message' and message.get('level') == 'error':
            errors.append((message.get('rendered') or message.get('message') or '').rstrip())
    return errors


def _failure(prepared: Prepared, result: subprocess.CompletedProcess, verb: str) -> RustBuildError:
    if prepared.members:
        errors = _chain_errors(result.stdout)
        if errors:
            return RustBuildError(
                'The chain of Rust blocks does not compile:\n\n'
                + chain_failure_text('\n'.join(errors), prepared),
            )
    diagnostics = parse_messages(result.stdout, prepared)
    errors = [item for item in diagnostics if item.level == 'error']
    if errors:
        text = '\n'.join(item.rendered.rstrip() for item in errors)
        return RustBuildError(f'The Rust block does not compile:\n\n{text}', diagnostics)
    # A dependency, the workspace or the toolchain failed: cargo says why on stderr.
    tail = '\n'.join(result.stderr.strip().splitlines()[-40:])
    return RustBuildError(f'{verb} the Rust block failed:\n\n{tail}', diagnostics)


def _used_marker(binary: Path) -> Path:
    return binary.with_name(f'.{binary.name}.used')


def _mark_used(binary: Path) -> None:
    """
    Records that the binary ran, for the cache's least-recently-used cleanup, in a file
    beside it. Never in the binary's own modification time: macOS checks an executable
    again on its next run after that changes, half a second for a block.
    """
    marker = _used_marker(binary)
    try:
        os.utime(marker)
    except FileNotFoundError:
        try:
            marker.touch()
        except OSError:
            pass
    except OSError:
        pass


def _last_used(binary: Path, stat: os.stat_result) -> float:
    try:
        return _used_marker(binary).stat().st_mtime
    except OSError:
        return stat.st_mtime


def prune_binary_cache(crate: Optional[str] = None) -> None:
    """
    Removes cached binaries: those of `crate` beyond the newest ones, then the least
    recently used until the cache fits its size. Every edit of a block caches a new
    binary of tens of megabytes. A binary that a run is executing stays usable after its
    file is removed.
    """
    root = binary_cache_dir()
    if not root.is_dir():
        return
    entries = []
    for directory in root.iterdir():
        if not directory.is_dir() or directory.name.startswith('.'):
            continue
        for binary in directory.iterdir():
            if binary.name.startswith('.'):
                continue
            try:
                stat = binary.stat()
            except OSError:
                continue
            entries.append((_last_used(binary, stat), stat.st_size, binary))
    entries.sort(reverse=True)

    def remove(binary: Path) -> None:
        try:
            binary.unlink()
            _used_marker(binary).unlink(missing_ok=True)
            binary.parent.rmdir()
        except OSError:
            pass

    kept = []
    seen: Dict[str, int] = {}
    for modified, size, binary in entries:
        if crate is not None and binary.name == crate:
            seen[crate] = seen.get(crate, 0) + 1
            if seen[crate] > KEEP_PER_BLOCK:
                remove(binary)
                continue
        kept.append((modified, size, binary))
    total = sum(size for _, size, _ in kept)
    limit = BINARY_CACHE_MB * 1024 * 1024
    for _, size, binary in reversed(kept):
        if total <= limit:
            break
        remove(binary)
        total -= size


def build(prepared: Prepared, stream: bool = True) -> Built:
    """The block's binary, from the cache or built now."""
    cached = binary_cache_dir() / prepared.key / prepared.crate
    if cached.exists():
        _mark_used(cached)
        return Built(binary=cached, cached=True, seconds=0.0)
    started = time.monotonic()
    with _LOCK:
        if cached.exists():
            return Built(binary=cached, cached=True, seconds=0.0)
        result = _run_cargo(['build', '--release'], prepared, stream)
        if result.returncode != 0:
            raise _failure(prepared, result, 'Building')
        binary = None
        for line in result.stdout.splitlines():
            if '"compiler-artifact"' not in line:
                continue
            item = json.loads(line)
            if (item.get('target') or {}).get('name') == prepared.crate and item.get('executable'):
                binary = Path(item['executable'])
        if binary is None or not binary.exists():
            raise RustBuildError('cargo reported no binary for the block.')
        # The cache key includes Cargo.lock, which the first build writes.
        key_text = prepared.key_text() if prepared.key_text else (
            prepared.main_path.read_text(encoding='utf-8')
            + (prepared.main_path.parent / block_source.MAIN_FILE).read_text(encoding='utf-8')
        )
        prepared.key = _cache_key(
            prepared.workspace, prepared.crate, key_text, ws.sync_sdk(prepared.workspace),
        )
        cached = binary_cache_dir() / prepared.key / prepared.crate
        cached.parent.mkdir(parents=True, exist_ok=True)
        staging = cached.with_name(f'.{prepared.crate}.{os.getpid()}.tmp')
        shutil.copy2(binary, staging)
        os.replace(staging, cached)
        _mark_used(cached)
        prune_binary_cache(prepared.crate)
    warnings = [
        item for item in parse_messages(result.stdout, prepared)
        if item.level == 'warning'
    ]
    return Built(
        binary=cached, cached=False, seconds=time.monotonic() - started, warnings=warnings,
    )


def check(prepared: Prepared) -> List[Diagnostic]:
    """The compiler's errors and warnings for the block, without building it."""
    with _LOCK:
        result = _run_cargo(['check', '--release'], prepared, stream=False)
    diagnostics = parse_messages(result.stdout, prepared)
    if result.returncode != 0 and not any(item.level == 'error' for item in diagnostics):
        raise _failure(prepared, result, 'Checking')
    return diagnostics


# ---- chains -------------------------------------------------------------------------------


@dataclass
class ChainMember:
    code: str
    block_type: BlockType
    block_uuid: str


def chain_crate_name(block_uuids: List[str]) -> str:
    # A block_ name keeps chain crates out of the lock fingerprint, as block crates are.
    digest = hashlib.sha256('\0'.join(block_uuids).encode()).hexdigest()[:12]
    return f'block_chain_{digest}'


def chain_module(source: block_source.BlockSource) -> str:
    """A block's code as a module of a chain crate, with the function the chain calls."""
    code = source.code if source.code.endswith('\n') else source.code + '\n'
    tests = ''.join(f'\n        mage::test("{name}", {name}),' for name in source.tests)
    return (
        f'{code}\n{block_source.GENERATED_MARKER}\n'
        'pub fn mage_block() -> mage::ChainBlock {\n'
        f'    mage::chain_block({source.function}, vec![{tests}\n    ])\n'
        '}\n'
    )


def prepare_chain(members: List[ChainMember], repo_path: str) -> Prepared:
    """
    A crate that runs a chain of Rust blocks in one process (mage::run_chain): each
    block's code is a module of it, so the blocks' crates and code stay as they are.
    """
    workspace = ws.init(repo_path)
    sdk = ws.sync_sdk(workspace)
    crates = [ws.crate_name(m.block_uuid) for m in members]
    files = {}
    sources = []
    for member, crate in zip(members, crates):
        source = block_source.analyze(member.code, member.block_type)
        sources.append(source)
        files[f'{crate}.rs'] = chain_module(source)
    entries = ''.join(f'        ("{crate}", {crate}::mage_block),\n' for crate in crates)
    files['main.rs'] = (
        '// Generated by Mage: a chain of Rust blocks run in one process.\n'
        + ''.join(f'mod {crate};\n' for crate in crates)
        + '\nfn main() {\n    mage::run_chain(vec![\n' + entries + '    ]);\n}\n'
    )
    crate = chain_crate_name([m.block_uuid for m in members])
    directory = ws.write_block_crate(workspace, crate, files)
    key = _cache_key(
        workspace, crate, ''.join(files[name] for name in sorted(files)), sdk,
    )
    labels = ' + '.join(f'{m.block_uuid}.rs' for m in members)
    return Prepared(
        crate=crate,
        key=key,
        label=labels,
        function_line=None,
        main_path=directory / 'src' / 'main.rs',
        source=sources[0],
        workspace=workspace,
        members=list(zip(members, crates)),
        key_text=lambda: ''.join(files[name] for name in sorted(files)),
    )


def chain_failure_text(text: str, prepared: Prepared) -> str:
    """Compiler output with each block's module named as the block's file."""
    directory = prepared.main_path.parent
    for member, crate in prepared.members:
        path = directory / f'{crate}.rs'
        text = text.replace(str(path), f'{member.block_uuid}.rs')
        text = text.replace(os.path.relpath(path, prepared.workspace.root),
                            f'{member.block_uuid}.rs')
    return relabel(text, prepared)


def build_chain(prepared: Prepared) -> Built:
    """The chain's binary, from the cache or built now; errors name the blocks' files."""
    return build(prepared, stream=False)
