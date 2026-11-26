"""
The R and Rust environments of a project, for the Workers page: one per language, which every
block of that language runs with (Python blocks choose among named workers instead).

- R: the rv project in <project>/r (rproject.toml, rv.lock and its library;
  models/block/r/runtime.py).
- Rust: the Cargo workspace in <project>/rust (Cargo.toml, Cargo.lock and the toolchain;
  models/block/rust/workspace.py).

summary() reads files only, for the list. detail() also asks the tools (Rscript, rv, cargo)
and says what stops blocks from running. save_manifest() checks a new rproject.toml or
Cargo.toml in a copy first (rv plan, cargo metadata): a file that does not resolve is refused
with the tool's reason and nothing changes, as with Python workers.
"""
import os
import re
import shlex
import shutil
import subprocess
import tempfile
import tomllib
from pathlib import Path
from typing import Dict, List, Optional

LANGUAGES = ('r', 'rust')
BLOCK_FOLDERS = ('custom', 'data_exporters', 'data_loaders', 'transformers')
CHECK_TIMEOUT = 300
# The last lines of a tool's error: what it says went wrong, not its progress.
ERROR_LINES = 12
_ESCAPES = re.compile(r'\x1b\[[0-9;?]*[A-Za-z]')


class LanguageEnvironmentError(Exception):
    pass


def _language(language: str) -> str:
    language = (language or '').strip().lower()
    if language not in LANGUAGES:
        raise LanguageEnvironmentError(
            f'There is no {language or "unnamed"} environment: the languages are R and Rust.',
        )
    return language


def _relative(repo_path: str, path: Optional[Path]) -> Optional[str]:
    if path is None:
        return None
    try:
        return str(Path(path).resolve().relative_to(Path(repo_path).resolve()))
    except ValueError:
        return str(path)


def _block_files(repo_path: str, suffixes) -> List[str]:
    found = []
    for folder in BLOCK_FOLDERS:
        root = Path(repo_path, folder)
        if not root.is_dir():
            continue
        for path in root.rglob('*'):
            if path.is_file() and path.suffix in suffixes and '.mage' not in path.parts:
                found.append(str(path.relative_to(repo_path)))
    return sorted(found)


def _toml(path: Path) -> Dict:
    try:
        with open(path, 'rb') as file:
            return tomllib.load(file)
    except FileNotFoundError:
        return {}
    except tomllib.TOMLDecodeError as error:
        raise LanguageEnvironmentError(f'{path.name} is not valid TOML: {error}') from error


def _plain(text: str) -> str:
    return _ESCAPES.sub('', text or '')


# Progress lines and the check's own temporary files say nothing about the change.
_NOISE = re.compile(r'^\s*(Updating|Locking|Downloading|Adding|Fetching)\b'
                    r'|mageml_check|mage_cargo_|mage_rproject_')


def _last_lines(text: str) -> str:
    lines = [line.rstrip() for line in _plain(text).splitlines()
             if line.strip() and not _NOISE.search(line)]
    return '\n'.join(lines[-ERROR_LINES:])


def _tool_version(command: List[str], cwd: Optional[Path] = None) -> Optional[str]:
    if shutil.which(command[0]) is None:
        return None
    try:
        result = subprocess.run(command, capture_output=True, text=True, cwd=cwd, timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        return None
    output = (result.stdout or result.stderr or '').strip().splitlines()
    return output[0] if result.returncode == 0 and output else None


# R


def _r_config(repo_path: str):
    from mage_ai.data_preparation.models.block.r import runtime

    try:
        return runtime.r_config(repo_path)
    except runtime.REnvironmentError as error:
        raise LanguageEnvironmentError(str(error)) from error


def _r_project_dir(repo_path: str) -> Optional[Path]:
    """Where r_config finds the rv project, from files only (r_config also asks rig)."""
    from mage_ai.data_preparation.models.block.r import runtime

    configured = os.getenv('MAGE_R_PROJECT_DIR')
    if configured:
        path = Path(configured)
        candidates = [path if path.is_absolute() else Path(repo_path) / path]
    else:
        candidates = [Path(repo_path) / runtime.R_PROJECT_DIRECTORY, Path(repo_path)]
    return next((c.resolve() for c in candidates if (c / 'rproject.toml').exists()), None)


def _r_packages(project_dir: Path) -> List[Dict]:
    manifest = _toml(project_dir / 'rproject.toml')
    locked = {
        package.get('name'): package.get('version')
        for package in _toml(project_dir / 'rv.lock').get('packages') or []
    }
    packages = []
    for entry in (manifest.get('project') or {}).get('dependencies') or []:
        name = entry if isinstance(entry, str) else entry.get('name', '')
        source = None if isinstance(entry, str) else next(
            (f'{key} {entry[key]}' for key in ('repository', 'git', 'path', 'url') if key in entry),
            None,
        )
        packages.append(dict(name=name, locked=locked.get(name), source=source))
    return packages


def _r_summary(repo_path: str) -> Dict:
    blocks = _block_files(repo_path, {'.r', '.R'})
    project_dir = _r_project_dir(repo_path)
    summary = dict(
        language='r', label='R', blocks=blocks, block_count=len(blocks),
        exists=project_dir is not None, definition=_relative(repo_path, project_dir),
        manifest='rproject.toml',
    )
    if project_dir is None:
        return dict(summary, state='none', packages=[], package_count=0, locked_count=0,
                    version=None)
    manifest = _toml(project_dir / 'rproject.toml')
    lock = _toml(project_dir / 'rv.lock')
    packages = _r_packages(project_dir)
    return dict(
        summary, state='ready', packages=packages, package_count=len(packages),
        locked_count=len(lock.get('packages') or []),
        version=(manifest.get('project') or {}).get('r_version'),
    )


def _r_detail(repo_path: str) -> Dict:
    from mage_ai.data_preparation.models.block.r import runtime

    summary = _r_summary(repo_path)
    config = _r_config(repo_path)
    tools = dict(
        rscript=_tool_version([config.rscript, '--version']),
        rv=_tool_version([config.rv, '--version']),
        rig=_tool_version(['rig', '--version']),
    )
    if summary['exists']:
        problems = runtime.status(config)['problems']
        manifest_text = (config.project_dir / 'rproject.toml').read_text(encoding='utf-8')
        # With the project's paths: the terminal may be anywhere.
        commands = [
            f'cd {shlex.quote(str(config.project_dir))}',
            'rv add <package>',
            f'mage r sync {shlex.quote(str(repo_path))}',
        ]
    else:
        problems = [] if not summary['blocks'] else [
            'The project has no R environment: R blocks use the R library of the Rscript '
            'they run with. Create one with `mage r init`.',
        ]
        manifest_text = None
        commands = ['mage r setup', f'mage r init {shlex.quote(str(repo_path))}']
    if not tools['rscript']:
        problems.insert(0, 'R is not installed or Rscript is not on PATH: `mage r setup` says '
                           'how to install it.')
    return dict(summary, tools=tools, problems=problems, commands=commands,
                manifest_text=manifest_text,
                state=_state(summary, problems))


def _check_rproject(config, text: str) -> None:
    """rv plan on a copy of the environment with the new rproject.toml."""
    from mage_ai.data_preparation.models.block.r import runtime

    try:
        manifest = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        raise LanguageEnvironmentError(f'rproject.toml is not valid TOML: {error}') from error
    project = manifest.get('project')
    if not isinstance(project, dict) or not isinstance(project.get('dependencies', []), list):
        raise LanguageEnvironmentError(
            'rproject.toml needs a [project] table whose dependencies is a list.',
        )
    names = {d if isinstance(d, str) else (d or {}).get('name')
             for d in project.get('dependencies') or []}
    missing = sorted(set(runtime.EXCHANGE_PACKAGES) - names)
    if missing:
        raise LanguageEnvironmentError(
            f'R blocks need {", ".join(missing)}: keep {"it" if len(missing) == 1 else "them"} '
            'in dependencies.',
        )
    project_dir = config.project_dir
    with tempfile.TemporaryDirectory(prefix='mage_rproject_') as scratch:
        scratch = Path(scratch)
        (scratch / 'rproject.toml').write_text(text, encoding='utf-8')
        if (project_dir / 'rv.lock').exists():
            shutil.copy2(project_dir / 'rv.lock', scratch / 'rv.lock')

        def plan():
            try:
                return subprocess.run(
                    [config.rv, 'plan', '--json'], capture_output=True, text=True, cwd=scratch,
                    timeout=CHECK_TIMEOUT, env=dict(runtime.tool_env(config), NO_COLOR='1'),
                )
            except FileNotFoundError as error:
                raise LanguageEnvironmentError(
                    'rv is not installed: `mage r setup` says how to install it.',
                ) from error
            except subprocess.TimeoutExpired as error:
                raise LanguageEnvironmentError(
                    f'rv did not resolve the packages in {CHECK_TIMEOUT} s; nothing changed.',
                ) from error

        result = plan()
        if result.returncode != 0 and 'Lockfile error' in (result.stderr + result.stdout):
            # A damaged rv.lock: resolved without it, as the next sync rewrites it.
            (scratch / 'rv.lock').unlink()
            result = plan()
    if result.returncode != 0:
        raise LanguageEnvironmentError(
            'rv cannot resolve these packages; nothing changed.\n\n'
            + _last_lines(result.stderr or result.stdout),
        )


# Rust


def _rust_workspace(repo_path: str):
    from mage_ai.data_preparation.models.block.rust import workspace as ws

    return ws.Workspace(ws.workspace_root(repo_path))


def _requirement(spec) -> str:
    if isinstance(spec, str):
        return spec
    if not isinstance(spec, dict):
        return str(spec)
    parts = [spec.get('version') or (f'path {spec["path"]}' if 'path' in spec else '')
             or (f'git {spec["git"]}' if 'git' in spec else '')]
    if spec.get('features'):
        parts.append(f'features: {", ".join(spec["features"])}')
    return '; '.join(p for p in parts if p)


def _rust_packages(root: Path) -> List[Dict]:
    manifest = _toml(root / 'Cargo.toml')
    locked: Dict[str, List[str]] = {}
    for package in _toml(root / 'Cargo.lock').get('package') or []:
        locked.setdefault(package.get('name'), []).append(package.get('version'))
    packages = []
    for name, spec in ((manifest.get('workspace') or {}).get('dependencies') or {}).items():
        package = spec.get('package', name) if isinstance(spec, dict) else name
        versions = locked.get(package) or []
        packages.append(dict(
            name=name, requirement=_requirement(spec),
            locked=', '.join(sorted(set(versions))) if versions else None,
        ))
    return packages


def _rust_summary(repo_path: str) -> Dict:
    workspace = _rust_workspace(repo_path)
    blocks = _block_files(repo_path, {'.rs'})
    exists = workspace.cargo_toml.exists()
    summary = dict(
        language='rust', label='Rust', blocks=blocks, block_count=len(blocks), exists=exists,
        definition=_relative(repo_path, workspace.root) if exists else None,
        manifest='Cargo.toml',
    )
    if not exists:
        return dict(summary, state='none', packages=[], package_count=0, locked_count=0,
                    version=None)
    toolchain = _toml(workspace.root / 'rust-toolchain.toml').get('toolchain') or {}
    packages = _rust_packages(workspace.root)
    return dict(
        summary, state='ready', packages=packages, package_count=len(packages),
        locked_count=len(_toml(workspace.root / 'Cargo.lock').get('package') or []),
        version=toolchain.get('channel'),
    )


def _rust_detail(repo_path: str) -> Dict:
    from mage_ai.data_preparation.models.block.rust import project

    summary = _rust_summary(repo_path)
    workspace = _rust_workspace(repo_path)
    status = project.status(repo_path)
    tools = {name: None if status['details'][name] == 'not found' else status['details'][name]
             for name in ('cargo', 'rustc')}
    problems = list(status['problems'])
    if summary['exists']:
        manifest_text = workspace.cargo_toml.read_text(encoding='utf-8')
        commands = [f'cd {shlex.quote(str(workspace.root))}', 'cargo search <crate>',
                    f'mage rust build {shlex.quote(str(repo_path))}']
    else:
        manifest_text = None
        commands = [f'mage rust init {shlex.quote(str(repo_path))}']
        if summary['blocks']:
            problems.append('The project has no Rust environment yet: the first Rust block '
                            'creates it, or `mage rust init` does.')
    return dict(summary, tools=tools, problems=problems, commands=commands,
                manifest_text=manifest_text,
                state=_state(summary, problems))


def _check_cargo_toml(root: Path, text: str) -> None:
    """
    cargo metadata on a copy of the workspace with the new Cargo.toml and the current
    Cargo.lock. Its only member depends on every crate, as each block does, so every crate
    resolves whether or not a block was built. The project's Cargo.lock is left to the next
    build, which keeps the locked versions that still fit.
    """
    from mage_ai.data_preparation.models.block.rust import workspace as ws

    try:
        manifest = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        raise LanguageEnvironmentError(f'Cargo.toml is not valid TOML: {error}') from error
    dependencies = (manifest.get('workspace') or {}).get('dependencies')
    if not isinstance(dependencies, dict) or 'mage-block' not in dependencies:
        raise LanguageEnvironmentError(
            'Cargo.toml must keep mage-block under [workspace.dependencies]: it is how Rust '
            'blocks reach MageML.',
        )
    if shutil.which('cargo') is None:
        raise LanguageEnvironmentError(
            'Rust is not installed: install it with rustup (https://rustup.rs) to change the '
            'crates.',
        )
    with tempfile.TemporaryDirectory(prefix='mage_cargo_') as scratch:
        scratch = Path(scratch)
        for name in ('Cargo.lock', 'rust-toolchain.toml'):
            if (root / name).exists():
                shutil.copy2(root / name, scratch / name)
        (scratch / 'Cargo.toml').write_text(text, encoding='utf-8')
        ws.sync_sdk(ws.Workspace(scratch))
        member = scratch / '.mage' / 'blocks' / 'mageml_check'
        (member / 'src').mkdir(parents=True)
        (member / 'Cargo.toml').write_text(
            ws.block_manifest('mageml_check', list(dependencies)), encoding='utf-8',
        )
        (member / 'src' / 'main.rs').write_text('fn main() {}\n', encoding='utf-8')
        try:
            result = subprocess.run(
                ['cargo', 'metadata', '--format-version', '1', '--manifest-path',
                 str(scratch / 'Cargo.toml')],
                capture_output=True, text=True, cwd=scratch, timeout=CHECK_TIMEOUT,
                env=dict(os.environ, CARGO_TERM_COLOR='never'),
            )
        except subprocess.TimeoutExpired as error:
            raise LanguageEnvironmentError(
                f'cargo did not resolve the crates in {CHECK_TIMEOUT} s; nothing changed.',
            ) from error
    if result.returncode != 0:
        raise LanguageEnvironmentError(
            'cargo cannot resolve these crates; nothing changed.\n\n'
            + _last_lines(result.stderr),
        )


# Both


def _state(summary: Dict, problems: List[str]) -> str:
    if problems:
        return 'problems'
    return summary['state']


def summary(repo_path: str) -> List[Dict]:
    """Both languages, from their files only."""
    rows = []
    for read in (_r_summary, _rust_summary):
        try:
            rows.append(read(repo_path))
        except LanguageEnvironmentError as error:
            language = 'r' if read is _r_summary else 'rust'
            rows.append(dict(language=language, label='R' if language == 'r' else 'Rust',
                             state='problems', problems=[str(error)], exists=True,
                             packages=[], package_count=0, block_count=0, blocks=[]))
    return rows


def detail(repo_path: str, language: str) -> Dict:
    language = _language(language)
    try:
        return _r_detail(repo_path) if language == 'r' else _rust_detail(repo_path)
    except LanguageEnvironmentError as error:
        row = next(r for r in summary(repo_path) if r['language'] == language)
        return dict(row, problems=[str(error)], state='problems', tools={}, commands=[])


def save_manifest(repo_path: str, language: str, text: str) -> Dict:
    """
    Writes the language's rproject.toml or Cargo.toml once it resolves in a copy: refused
    with the tool's reason otherwise, and nothing changes. The locks follow at the next
    sync (rv.lock) or build (Cargo.lock).
    """
    language = _language(language)
    if not isinstance(text, str) or not text.strip():
        raise LanguageEnvironmentError('The file is empty.')
    if language == 'r':
        config = _r_config(repo_path)
        if not config.uses_rv:
            raise LanguageEnvironmentError(
                'The project has no R environment to change: create it with `mage r init`.',
            )
        _check_rproject(config, text)
        (config.project_dir / 'rproject.toml').write_text(text, encoding='utf-8')
    else:
        workspace = _rust_workspace(repo_path)
        if not workspace.cargo_toml.exists():
            raise LanguageEnvironmentError(
                'The project has no Rust environment to change: create it with '
                '`mage rust init`.',
            )
        _check_cargo_toml(workspace.root, text)
        workspace.cargo_toml.write_text(text, encoding='utf-8')
    return detail(repo_path, language)


def init_rust(repo_path: str) -> Dict:
    """Creates the Rust environment (`mage rust init`); an existing one is kept."""
    from mage_ai.data_preparation.models.block.rust import workspace as ws

    ws.init(repo_path)
    return detail(repo_path, 'rust')


def sync_r(repo_path: str) -> Dict:
    """Installs the R environment's packages as rproject.toml asks (rv sync)."""
    from mage_ai.data_preparation.models.block.r import runtime

    config = _r_config(repo_path)
    if not config.uses_rv:
        raise LanguageEnvironmentError(
            'The project has no R environment to sync: create it with `mage r init`.',
        )
    try:
        runtime.sync(config)
    except runtime.REnvironmentError as error:
        raise LanguageEnvironmentError(str(error)) from error
    return detail(repo_path, 'r')
