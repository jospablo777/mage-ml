"""
The Rust environment of a Mage project: a Cargo workspace in <project>/rust.

    rust/Cargo.toml           the dependencies every Rust block can use; users add crates
    rust/Cargo.lock           the resolved versions, written by the first build
    rust/rust-toolchain.toml  the Rust version
    rust/.mage/sdk            a copy of Mage's block runtime (the `mage` crate)
    rust/.mage/blocks/<name>  a generated crate per block: the block's code and a main

Commit the first three; .mage is generated and ignored. Compiled code goes to a target
directory outside the project, shared by every project on the machine, so dependencies
compile once.
"""
import hashlib
import os
import re
import shutil
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

SDK_SOURCE = Path(__file__).parent / 'sdk'
RUST_VERSION = '1.98.0'
# Folders of a block crate that cargo writes; never copied with the SDK.
_SKIP = {'target', '.git', '__pycache__'}

CARGO_TOML = '''# The Rust environment of this Mage project. Every Rust block can use the crates
# under [workspace.dependencies]: add one there, such as `regex = "1"`, and run the block.
# Polars comes with Mage's `mage` crate (use mage::prelude::*); to enable more Polars
# features, add polars here with the same version and the features you need.

[workspace]
resolver = "3"
members = [".mage/blocks/*"]

[workspace.dependencies]
mage-block = { path = ".mage/sdk" }
anyhow = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
rayon = "1"

# Fast rebuilds with optimized code: a block recompiles in about a second once the
# dependencies are built.
[profile.release]
opt-level = 3
codegen-units = 16
incremental = true
debug = false
lto = false
# A third smaller binaries; panics still name the file and line. Remove for function
# names in RUST_BACKTRACE traces.
strip = "symbols"
'''

TOOLCHAIN_TOML = f'''[toolchain]
channel = "{RUST_VERSION}"
profile = "minimal"
'''

GITIGNORE = '.mage/\n'


class RustWorkspaceError(Exception):
    pass


@dataclass(frozen=True)
class Workspace:
    root: Path

    @property
    def cargo_toml(self) -> Path:
        return self.root / 'Cargo.toml'

    @property
    def cargo_lock(self) -> Path:
        return self.root / 'Cargo.lock'

    @property
    def toolchain_toml(self) -> Path:
        return self.root / 'rust-toolchain.toml'

    @property
    def generated(self) -> Path:
        return self.root / '.mage'

    @property
    def sdk(self) -> Path:
        return self.generated / 'sdk'

    def block_dir(self, crate: str) -> Path:
        return self.generated / 'blocks' / crate


def workspace_root(repo_path: str) -> Path:
    override = os.getenv('MAGE_RUST_PROJECT_DIR')
    if override:
        return Path(override)
    return Path(repo_path) / 'rust'


def target_dir() -> Path:
    """Compiled dependencies and blocks, shared by the projects on this machine."""
    override = os.getenv('MAGE_RUST_TARGET_DIR')
    if override:
        return Path(override)
    cache = os.getenv('XDG_CACHE_HOME') or os.path.join(os.path.expanduser('~'), '.cache')
    return Path(cache) / 'mage' / 'rust' / 'target'


def _write_if_changed(path: Path, text: str) -> bool:
    """Writes text unless the file holds it already; cargo rebuilds what changed on disk."""
    try:
        if path.read_text(encoding='utf-8') == text:
            return False
    except FileNotFoundError:
        pass
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f'.{path.name}.{os.getpid()}.tmp')
    temporary.write_text(text, encoding='utf-8')
    os.replace(temporary, path)
    return True


def init(repo_path: str) -> Workspace:
    """Creates the project's Rust environment; existing files are kept."""
    workspace = Workspace(workspace_root(repo_path))
    workspace.root.mkdir(parents=True, exist_ok=True)
    for path, text in (
        (workspace.cargo_toml, CARGO_TOML),
        (workspace.toolchain_toml, TOOLCHAIN_TOML),
        (workspace.root / '.gitignore', GITIGNORE),
    ):
        if not path.exists():
            path.write_text(text, encoding='utf-8')
    if not workspace.cargo_lock.exists() and (SDK_SOURCE / 'Cargo.lock').exists():
        # The versions the SDK is tested with, which the Docker image compiles in
        # advance: a new project then reuses those compiled crates.
        shutil.copyfile(SDK_SOURCE / 'Cargo.lock', workspace.cargo_lock)
    sync_sdk(workspace)
    return workspace


def sdk_digest() -> str:
    digest = hashlib.sha256()
    for path in sorted(SDK_SOURCE.rglob('*')):
        relative = path.relative_to(SDK_SOURCE)
        if path.is_dir() or any(part in _SKIP for part in relative.parts):
            continue
        if relative.name == 'Cargo.lock':
            continue
        digest.update(relative.as_posix().encode())
        digest.update(b'\0')
        digest.update(path.read_bytes())
    return digest.hexdigest()


def sync_sdk(workspace: Workspace) -> str:
    """Copies Mage's block runtime into the workspace when it changed; returns its digest."""
    digest = sdk_digest()
    marker = workspace.sdk / '.digest'
    if marker.exists() and marker.read_text().strip() == digest:
        return digest
    staging = workspace.generated / f'.sdk.{os.getpid()}.tmp'
    shutil.rmtree(staging, ignore_errors=True)
    shutil.copytree(
        SDK_SOURCE,
        staging,
        ignore=shutil.ignore_patterns(*_SKIP, 'Cargo.lock', 'rust-toolchain.toml', '.gitignore'),
    )
    (staging / '.digest').write_text(digest)
    previous = workspace.generated / f'.sdk.{os.getpid()}.old'
    if workspace.sdk.exists():
        os.replace(workspace.sdk, previous)
    os.replace(staging, workspace.sdk)
    shutil.rmtree(previous, ignore_errors=True)
    return digest


def dependencies(workspace: Workspace) -> List[str]:
    """The crates under [workspace.dependencies], which every block depends on."""
    try:
        manifest = tomllib.loads(workspace.cargo_toml.read_text(encoding='utf-8'))
    except tomllib.TOMLDecodeError as error:
        raise RustWorkspaceError(f'{workspace.cargo_toml} is not valid TOML: {error}') from error
    names = list((manifest.get('workspace') or {}).get('dependencies') or {})
    if 'mage-block' not in names:
        raise RustWorkspaceError(
            f'{workspace.cargo_toml} must list mage-block under [workspace.dependencies]: '
            'mage-block = { path = ".mage/sdk" }',
        )
    return names


def crate_name(block_uuid: str) -> str:
    name = re.sub(r'[^a-z0-9_]', '_', block_uuid.lower()).strip('_') or 'block'
    # Block uuids that differ only in punctuation get different crates.
    suffix = hashlib.sha256(block_uuid.encode()).hexdigest()[:8]
    return f'block_{name[:48]}_{suffix}'


def block_manifest(crate: str, crates: List[str]) -> str:
    lines = [
        '# Generated by Mage; edit the project\'s Cargo.toml instead.',
        '[package]',
        f'name = "{crate}"',
        'version = "0.0.0"',
        'edition = "2024"',
        'publish = false',
        '',
        '[[bin]]',
        f'name = "{crate}"',
        'path = "src/main.rs"',
        '',
        '[dependencies]',
    ]
    lines += [f'{name} = {{ workspace = true }}' for name in crates]
    lines += [
        '',
        '[lints.rust]',
        # Helpers that a block defines and does not use yet are not worth a warning.
        'dead_code = "allow"',
        '',
    ]
    return '\n'.join(lines)


def write_block_crate(
    workspace: Workspace,
    crate: str,
    files: Dict[str, str],
    crates: Optional[List[str]] = None,
) -> Path:
    """Writes the block's crate: its manifest and the files of src/, by name."""
    directory = workspace.block_dir(crate)
    manifest = block_manifest(crate, crates or dependencies(workspace))
    _write_if_changed(directory / 'Cargo.toml', manifest)
    for name, text in files.items():
        _write_if_changed(directory / 'src' / name, text)
    return directory


def digest_files(paths: List[Path], extra: Dict[str, str]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(str(path.name).encode())
        digest.update(b'\0')
        digest.update(path.read_bytes() if path.exists() else b'<missing>')
    for key in sorted(extra):
        digest.update(f'{key}={extra[key]}'.encode())
        digest.update(b'\0')
    return digest.hexdigest()
