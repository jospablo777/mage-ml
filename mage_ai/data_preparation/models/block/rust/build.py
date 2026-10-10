"""
Building and checking Rust blocks with cargo.

A build compiles the block's generated crate in the project's workspace, with the shared
target directory, and copies the binary to a cache keyed by everything that determines
it. A run finds a cached binary without starting cargo. A check runs `cargo check` and
returns the compiler's diagnostics in the block's own lines, for the editor.
"""
import json
import os
import re
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

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


def _cache_key(workspace: ws.Workspace, crate: str, main: str, sdk: str) -> str:
    """Everything that determines the binary: code, dependencies, compiler and runtime."""
    return ws.digest_files(
        [workspace.cargo_toml, workspace.cargo_lock, workspace.toolchain_toml],
        dict(
            crate=crate,
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


def _failure(prepared: Prepared, result: subprocess.CompletedProcess, verb: str) -> RustBuildError:
    diagnostics = parse_messages(result.stdout, prepared)
    errors = [item for item in diagnostics if item.level == 'error']
    if errors:
        text = '\n'.join(item.rendered.rstrip() for item in errors)
        return RustBuildError(f'The Rust block does not compile:\n\n{text}', diagnostics)
    # A dependency, the workspace or the toolchain failed: cargo says why on stderr.
    tail = '\n'.join(result.stderr.strip().splitlines()[-40:])
    return RustBuildError(f'{verb} the Rust block failed:\n\n{tail}', diagnostics)


def _touch(path: Path) -> None:
    try:
        os.utime(path)
    except OSError:
        pass


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
            entries.append((stat.st_mtime, stat.st_size, binary))
    entries.sort(reverse=True)

    def remove(binary: Path) -> None:
        try:
            binary.unlink()
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
        # Marks it as used, for the cache's least-recently-used cleanup.
        _touch(cached)
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
        prepared.key = _cache_key(
            prepared.workspace,
            prepared.crate,
            prepared.main_path.read_text(encoding='utf-8')
            + (prepared.main_path.parent / block_source.MAIN_FILE).read_text(encoding='utf-8'),
            ws.sync_sdk(prepared.workspace),
        )
        cached = binary_cache_dir() / prepared.key / prepared.crate
        cached.parent.mkdir(parents=True, exist_ok=True)
        staging = cached.with_name(f'.{prepared.crate}.{os.getpid()}.tmp')
        shutil.copy2(binary, staging)
        os.replace(staging, cached)
        _touch(cached)
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
