"""The Rust blocks of a project: finding them, building them all and reporting status."""
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from mage_ai.data_preparation.models.block.rust import build as rust_build
from mage_ai.data_preparation.models.block.rust import workspace as ws
from mage_ai.data_preparation.models.block.rust.source import RustSourceError
from mage_ai.data_preparation.models.constants import BlockType

FOLDERS = {
    'custom': BlockType.CUSTOM,
    'data_exporters': BlockType.DATA_EXPORTER,
    'data_loaders': BlockType.DATA_LOADER,
    'transformers': BlockType.TRANSFORMER,
}


@dataclass
class RustBlockFile:
    block_type: BlockType
    path: Path
    # As Mage names the block: its path in the folder, without the extension.
    uuid: str
    label: str


@dataclass
class BuildReport:
    block: RustBlockFile
    ok: bool
    cached: bool = False
    seconds: float = 0.0
    error: Optional[str] = None


def block_files(repo_path: str) -> List[RustBlockFile]:
    root = Path(repo_path)
    files = []
    for folder, block_type in FOLDERS.items():
        directory = root / folder
        if not directory.is_dir():
            continue
        for path in sorted(directory.rglob('*.rs')):
            relative = path.relative_to(directory).with_suffix('')
            files.append(RustBlockFile(
                block_type=block_type,
                label=str(path.relative_to(root)),
                path=path,
                uuid=relative.as_posix(),
            ))
    return files


def build_all(repo_path: str, stream: bool = True) -> List[BuildReport]:
    """Builds each Rust block file in every environment its pipelines run it in (ADR-0040),
    or in the default one when no pipeline uses it."""
    from mage_ai.data_preparation import block_environments

    used = block_environments.of_block_files(repo_path, 'rust')
    reports = []
    for block in block_files(repo_path):
        for environment in used.get(block.uuid) or [block_environments.DEFAULT]:
            label = block.label if environment == block_environments.DEFAULT \
                else f'{block.label} ({environment})'
            labelled = RustBlockFile(block_type=block.block_type, label=label, path=block.path,
                                     uuid=block.uuid)
            try:
                prepared = rust_build.prepare(
                    block.path.read_text(encoding='utf-8'),
                    block.block_type,
                    block.uuid,
                    repo_path,
                    label=block.label,
                    environment=environment,
                )
                built = rust_build.build(prepared, stream=stream)
                reports.append(BuildReport(
                    block=labelled, cached=built.cached, ok=True, seconds=built.seconds,
                ))
            except (RustSourceError, rust_build.RustBuildError) as error:
                reports.append(BuildReport(block=labelled, error=str(error), ok=False))
    return reports


def _version(tool: str, cwd: Optional[Path]) -> Optional[str]:
    executable = shutil.which(tool)
    if executable is None:
        return None
    result = subprocess.run(
        [executable, '--version'], capture_output=True, cwd=cwd, text=True, timeout=600,
    )
    return result.stdout.strip() or result.stderr.strip() or None


def status(repo_path: str) -> Dict:
    workspace = ws.Workspace(ws.workspace_root(repo_path))
    cwd = workspace.root if workspace.root.is_dir() else None
    details = {
        'cargo': _version('cargo', cwd) or 'not found',
        'rustc': _version('rustc', cwd) or 'not found',
        'workspace': str(workspace.root) if workspace.cargo_toml.exists() else 'not created',
        'target directory': str(ws.target_dir()),
        'Rust blocks': str(len(block_files(repo_path))),
    }
    problems = []
    if details['cargo'] == 'not found':
        problems.append(
            'Rust is not installed: install it with rustup (https://rustup.rs), or use the '
            'Mage Docker image.',
        )
    if workspace.cargo_toml.exists():
        try:
            details['crates'] = ', '.join(ws.dependencies(workspace))
        except ws.RustWorkspaceError as error:
            problems.append(str(error))
    return dict(details=details, problems=problems)


def target_size_bytes() -> int:
    total = 0
    for directory, _, names in os.walk(ws.target_dir()):
        for name in names:
            try:
                total += os.path.getsize(os.path.join(directory, name))
            except OSError:
                pass
    return total
