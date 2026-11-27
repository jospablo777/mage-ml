"""
The R and Rust environments of a project, for the Workers page. Each language has its
`default` environment and any named ones (ADR-0040, block_environments.py); a block selects
one as Python blocks select workers.

- R: an rv project (rproject.toml, rv.lock and its library; models/block/r/runtime.py), the
  default in <project>/r, named ones in r_environments/<name>.
- Rust: a Cargo workspace (Cargo.toml, Cargo.lock and the toolchain;
  models/block/rust/workspace.py), the default in <project>/rust, named ones in
  rust_environments/<name>.

Ids: `r` and `rust` for the defaults, `r.<name>` and `rust.<name>` for named ones (a name has
no dot).

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
from typing import Dict, List, Optional, Tuple

from mage_ai.data_preparation import block_environments

DEFAULT = block_environments.DEFAULT
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


def environment_id(language: str, environment: str) -> str:
    return language if environment == DEFAULT else f'{language}.{environment}'


def parse_id(pk: str) -> Tuple[str, str]:
    """(language, environment) of an id: `r`, `rust`, `r.<name>` or `rust.<name>`."""
    language, _, name = (pk or '').partition('.')
    language = _language(language)
    try:
        return language, block_environments.normalized(name or None)
    except block_environments.EnvironmentNameError as error:
        raise LanguageEnvironmentError(str(error)) from error


def _require(repo_path: str, language: str, environment: str) -> None:
    try:
        block_environments.require(repo_path, language, environment)
    except block_environments.EnvironmentNameError as error:
        raise LanguageEnvironmentError(str(error)) from error


def _relative(repo_path: str, path: Optional[Path]) -> Optional[str]:
    if path is None:
        return None
    try:
        return str(Path(path).resolve().relative_to(Path(repo_path).resolve()))
    except ValueError:
        return str(path)


def _blocks_in(repo_path: str, language: str, environment: str, suffixes) -> List[str]:
    """The block files that run in the environment: those its pipelines' blocks select, and
    for `default` also the files no pipeline runs in another environment."""
    used = block_environments.of_block_files(repo_path, language)
    files = []
    for path in _block_files(repo_path, suffixes):
        uuid = Path(*Path(path).parts[1:]).with_suffix('').as_posix()
        environments = used.get(uuid) or [DEFAULT]
        if environment in environments:
            files.append(path)
    return files


def _base(repo_path: str, language: str, environment: str, usage: Dict) -> Dict:
    label = block_environments.LABELS[language]
    return dict(
        id=environment_id(language, environment),
        language=language,
        environment=environment,
        label=label if environment == DEFAULT else f'{label} {environment}',
        pipelines=usage.get(environment) or [],
    )


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


def _r_config(repo_path: str, environment: str = DEFAULT):
    from mage_ai.data_preparation.models.block.r import runtime

    try:
        return runtime.r_config(repo_path, environment)
    except runtime.REnvironmentError as error:
        raise LanguageEnvironmentError(str(error)) from error


def _r_project_dir(repo_path: str, environment: str = DEFAULT) -> Optional[Path]:
    """Where r_config finds the rv project, from files only (r_config also asks rig)."""
    from mage_ai.data_preparation.models.block.r import runtime

    if environment != DEFAULT:
        folder = block_environments.folder(repo_path, 'r', environment)
        return folder.resolve() if (folder / 'rproject.toml').exists() else None
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


def _r_summary(repo_path: str, environment: str = DEFAULT, usage: Optional[Dict] = None) -> Dict:
    if usage is None:
        usage = block_environments.usage(repo_path, 'r')
    blocks = _blocks_in(repo_path, 'r', environment, {'.r', '.R'})
    project_dir = _r_project_dir(repo_path, environment)
    summary = dict(
        _base(repo_path, 'r', environment, usage), blocks=blocks, block_count=len(blocks),
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


def _r_detail(repo_path: str, environment: str = DEFAULT) -> Dict:
    from mage_ai.data_preparation.models.block.r import runtime

    summary = _r_summary(repo_path, environment)
    config = _r_config(repo_path, environment)
    tools = dict(
        rscript=_tool_version([config.rscript, '--version']),
        rv=_tool_version([config.rv, '--version']),
        rig=_tool_version(['rig', '--version']),
    )
    if summary['exists']:
        problems = runtime.status(config)['problems']
        manifest_text = (config.project_dir / 'rproject.toml').read_text(encoding='utf-8')
        # With the project's paths: the terminal may be anywhere.
        sync = f'mage r sync {shlex.quote(str(repo_path))}'
        if environment != DEFAULT:
            sync += f' --environment {environment}'
        commands = [
            f'cd {shlex.quote(str(config.project_dir))}',
            'rv add <package>',
            sync,
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


def _rust_workspace(repo_path: str, environment: str = DEFAULT):
    from mage_ai.data_preparation.models.block.rust import workspace as ws

    return ws.of_environment(repo_path, environment)


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


def _rust_summary(
    repo_path: str, environment: str = DEFAULT, usage: Optional[Dict] = None,
) -> Dict:
    if usage is None:
        usage = block_environments.usage(repo_path, 'rust')
    workspace = _rust_workspace(repo_path, environment)
    blocks = _blocks_in(repo_path, 'rust', environment, {'.rs'})
    exists = workspace.cargo_toml.exists()
    summary = dict(
        _base(repo_path, 'rust', environment, usage), blocks=blocks, block_count=len(blocks),
        exists=exists,
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


def _rust_detail(repo_path: str, environment: str = DEFAULT) -> Dict:
    from mage_ai.data_preparation.models.block.rust import project

    summary = _rust_summary(repo_path, environment)
    workspace = _rust_workspace(repo_path, environment)
    status = project.status(repo_path, environment)
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
    """Every environment of both languages, from their files only: each language's default
    first, then its named ones."""
    rows = []
    for language, read in (('r', _r_summary), ('rust', _rust_summary)):
        usage = block_environments.usage(repo_path, language)
        for environment in [DEFAULT, *block_environments.named(repo_path, language)]:
            try:
                rows.append(read(repo_path, environment, usage))
            except LanguageEnvironmentError as error:
                rows.append(dict(
                    _base(repo_path, language, environment, usage), state='problems',
                    problems=[str(error)], exists=True, packages=[], package_count=0,
                    block_count=0, blocks=[],
                ))
    return rows


def detail(repo_path: str, pk: str) -> Dict:
    language, environment = parse_id(pk)
    _require(repo_path, language, environment)
    try:
        if language == 'r':
            return _r_detail(repo_path, environment)
        return _rust_detail(repo_path, environment)
    except LanguageEnvironmentError as error:
        row = next(r for r in summary(repo_path) if r['id'] == environment_id(language,
                                                                               environment))
        return dict(row, problems=[str(error)], state='problems', tools={}, commands=[])


def save_manifest(repo_path: str, pk: str, text: str) -> Dict:
    """
    Writes the environment's rproject.toml or Cargo.toml once it resolves in a copy: refused
    with the tool's reason otherwise, and nothing changes. The locks follow at the next
    sync (rv.lock) or build (Cargo.lock).
    """
    language, environment = parse_id(pk)
    _require(repo_path, language, environment)
    if not isinstance(text, str) or not text.strip():
        raise LanguageEnvironmentError('The file is empty.')
    if language == 'r':
        config = _r_config(repo_path, environment)
        if not config.uses_rv:
            raise LanguageEnvironmentError(
                'The project has no R environment to change: create it with `mage r init`.',
            )
        _check_rproject(config, text)
        (config.project_dir / 'rproject.toml').write_text(text, encoding='utf-8')
    else:
        workspace = _rust_workspace(repo_path, environment)
        if not workspace.cargo_toml.exists():
            raise LanguageEnvironmentError(
                'The project has no Rust environment to change: create it with '
                '`mage rust init`.',
            )
        _check_cargo_toml(workspace.root, text)
        workspace.cargo_toml.write_text(text, encoding='utf-8')
    return detail(repo_path, pk)


def init_rust(repo_path: str, environment: str = DEFAULT) -> Dict:
    """Creates a Rust environment (`mage rust init`); an existing one is kept."""
    from mage_ai.data_preparation.models.block.rust import workspace as ws

    ws.init(repo_path, environment)
    return detail(repo_path, environment_id('rust', environment))


def sync_r(repo_path: str, environment: str = DEFAULT) -> Dict:
    """Installs an R environment's packages as rproject.toml asks (rv sync)."""
    from mage_ai.data_preparation.models.block.r import runtime

    _require(repo_path, 'r', environment)
    config = _r_config(repo_path, environment)
    if not config.uses_rv:
        raise LanguageEnvironmentError(
            'The project has no R environment to sync: create it with `mage r init`.',
        )
    try:
        runtime.sync(config)
    except runtime.REnvironmentError as error:
        raise LanguageEnvironmentError(str(error)) from error
    return detail(repo_path, environment_id('r', environment))


START_FROM = ('default', 'minimal', 'file')


def _edited(repo_path: str, language: str, environment: str, packages: List[str],
            snapshot: Optional[str]) -> Optional[str]:
    """The environment's manifest text with the packages and the R snapshot; None when
    nothing changes."""
    from mage_ai.data_preparation import environment_packages as ep

    if not packages and not snapshot:
        return None
    try:
        if language == 'r':
            path = _r_config(repo_path, environment).project_dir / 'rproject.toml'
            text, _ = ep.add_r_packages(path.read_text(encoding='utf-8'), packages)
            if snapshot:
                text = ep.with_r_snapshot(text, snapshot)
        else:
            if snapshot:
                raise ep.PackageError('A snapshot date is for R; Rust crates take versions.')
            path = _rust_workspace(repo_path, environment).cargo_toml
            text, _ = ep.add_crates(path.read_text(encoding='utf-8'), packages)
    except ep.PackageError as error:
        raise LanguageEnvironmentError(str(error)) from error
    return text


def _set_toolchain(repo_path: str, environment: str, toolchain: str) -> None:
    from mage_ai.data_preparation import environment_packages as ep

    path = _rust_workspace(repo_path, environment).toolchain_toml
    try:
        text = ep.with_toolchain(path.read_text(encoding='utf-8') if path.exists() else '',
                                 toolchain)
    except ep.PackageError as error:
        raise LanguageEnvironmentError(str(error)) from error
    path.write_text(text, encoding='utf-8')


def add_packages(repo_path: str, pk: str, packages: List[str],
                 snapshot: Optional[str] = None, toolchain: Optional[str] = None) -> Dict:
    """
    Adds R packages or Rust crates to an environment (environment_packages.py), and sets an R
    environment's snapshot date or a Rust environment's toolchain. The manifest is saved as
    save_manifest saves one: once it resolves in a copy. R packages install with the next
    sync; crates compile at the next build.
    """
    language, environment = parse_id(pk)
    _require(repo_path, language, environment)
    if toolchain and language != 'rust':
        raise LanguageEnvironmentError('A toolchain is for Rust; R environments take r_version.')
    text = _edited(repo_path, language, environment, packages or [], snapshot)
    if text is not None:
        save_manifest(repo_path, pk, text)
    if toolchain:
        _set_toolchain(repo_path, environment, toolchain)
    return detail(repo_path, pk)


def create(
    repo_path: str,
    language: str,
    name: str,
    start_from: str = 'default',
    r_version: Optional[str] = None,
    packages: Optional[List[str]] = None,
    snapshot: Optional[str] = None,
    toolchain: Optional[str] = None,
    manifest_text: Optional[str] = None,
) -> Dict:
    """
    `packages`: R packages or Rust crates to add (environment_packages.py); `snapshot`: an R
    environment's CRAN snapshot date; `toolchain`: a Rust environment's Rust version.
    `start_from='file'` starts from `manifest_text`, an rproject.toml or Cargo.toml people
    bring, with what MageML's blocks need added.
    A new named environment. `start_from`: `default` copies the default environment's
    manifest, lock and (Rust) toolchain, so its versions hold until changed; `minimal` has
    only what blocks need (R: the packages MageML exchanges data with; Rust: MageML's
    crate and its usual companions). Writes files only: R packages install with Sync, Rust
    crates compile at the first build. `r_version` sets an R environment's R (default:
    the default environment's, else MageML's default).
    """
    language = _language(language)
    try:
        name = block_environments.normalized(name)
    except block_environments.EnvironmentNameError as error:
        raise LanguageEnvironmentError(str(error)) from error
    if name == DEFAULT:
        raise LanguageEnvironmentError(
            'default is the project\'s own environment: give the new one another name.',
        )
    if start_from not in START_FROM:
        raise LanguageEnvironmentError('Start from default, minimal or a file.')
    if start_from == 'file' and not (manifest_text or '').strip():
        raise LanguageEnvironmentError(
            'Paste the ' + ('rproject.toml' if language == 'r' else 'Cargo.toml')
            + ' to start from.',
        )
    if toolchain and language != 'rust':
        raise LanguageEnvironmentError('A toolchain is for Rust; R environments take r_version.')
    folder = block_environments.folder(repo_path, language, name)
    if folder.exists():
        raise LanguageEnvironmentError(
            f'{_relative(repo_path, folder)} exists already: choose another name, or remove '
            'the folder.',
        )
    if language == 'r':
        _create_r(repo_path, folder, start_from, r_version, manifest_text)
    else:
        _create_rust(repo_path, name, start_from, manifest_text)
    try:
        text = _edited(repo_path, language, name, packages or [], snapshot)
        if text is not None:
            manifest = folder / ('rproject.toml' if language == 'r' else 'Cargo.toml')
            manifest.write_text(text, encoding='utf-8')
        if toolchain:
            _set_toolchain(repo_path, name, toolchain)
    except LanguageEnvironmentError:
        # A package that is not one leaves no environment behind.
        shutil.rmtree(folder, ignore_errors=True)
        raise
    return detail(repo_path, environment_id(language, name))


def _create_r(repo_path: str, folder: Path, start_from: str, r_version: Optional[str],
              manifest_text: Optional[str] = None) -> None:
    from mage_ai.data_preparation import environment_packages as ep
    from mage_ai.data_preparation.models.block.r import runtime

    source = _r_project_dir(repo_path) if start_from == 'default' else None
    if start_from == 'default' and source is None:
        raise LanguageEnvironmentError(
            'The project has no default R environment to start from: start from minimal, '
            'or create the default with `mage r init`.',
        )
    if start_from == 'file':
        try:
            # What R blocks exchange data with, whatever the file lists.
            manifest, _ = ep.add_r_packages(manifest_text, runtime.EXCHANGE_PACKAGES)
        except Exception as error:
            raise LanguageEnvironmentError(f'rproject.toml: {error}') from error
        if r_version:
            manifest = _with_r_version(manifest, r_version)
    elif source is not None:
        manifest = (source / 'rproject.toml').read_text(encoding='utf-8')
        if r_version:
            manifest = _with_r_version(manifest, r_version)
    else:
        version = r_version or runtime.DEFAULT_R_VERSION
        dependencies = ',\n'.join(
            runtime._dependency_line(p) for p in runtime.EXCHANGE_PACKAGES
        )
        repositories = ',\n'.join(
            f'    {{alias = "{r["alias"]}", url = "{r["url"]}"}}'
            for r in runtime.DEFAULT_REPOSITORIES
        )
        manifest = (
            '[project]\n'
            f'name = "{folder.name}"\n'
            f'r_version = "{version}"\n'
            f'repositories = [\n{repositories},\n]\n'
            f'dependencies = [\n{dependencies},\n]\n'
        )
    try:
        tomllib.loads(manifest)
    except tomllib.TOMLDecodeError as error:
        raise LanguageEnvironmentError(f'rproject.toml is not valid TOML: {error}') from error
    folder.mkdir(parents=True)
    try:
        (folder / 'rproject.toml').write_text(manifest, encoding='utf-8')
        if source is not None and (source / 'rv.lock').exists() and not r_version:
            # Another R version resolves its own versions at the first sync.
            shutil.copy2(source / 'rv.lock', folder / 'rv.lock')
        (folder / '.gitignore').write_text('rv/library/\nrv/staging/\n', encoding='utf-8')
    except OSError:
        shutil.rmtree(folder, ignore_errors=True)
        raise


_R_VERSION_LINE = re.compile(r'^(\s*r_version\s*=\s*)"[^"]*"', re.MULTILINE)


def _with_r_version(manifest: str, version: str) -> str:
    if not re.fullmatch(r'\d+\.\d+(\.\d+)?', version.strip()):
        raise LanguageEnvironmentError(f'{version!r} is not an R version such as 4.4.')
    if _R_VERSION_LINE.search(manifest):
        return _R_VERSION_LINE.sub(lambda m: f'{m.group(1)}"{version.strip()}"', manifest, 1)
    return manifest.replace('[project]\n', f'[project]\nr_version = "{version.strip()}"\n', 1)


def _create_rust(repo_path: str, name: str, start_from: str,
                 manifest_text: Optional[str] = None) -> None:
    from mage_ai.data_preparation import environment_packages as ep
    from mage_ai.data_preparation.models.block.rust import workspace as ws

    target = ws.of_environment(repo_path, name)
    if start_from == 'file':
        try:
            text = ep.with_rust_essentials(manifest_text)
        except Exception as error:
            raise LanguageEnvironmentError(f'Cargo.toml: {error}') from error
        target.root.mkdir(parents=True)
        target.cargo_toml.write_text(text, encoding='utf-8')
    elif start_from == 'default':
        source = _rust_workspace(repo_path)
        if not source.cargo_toml.exists():
            raise LanguageEnvironmentError(
                'The project has no default Rust environment to start from: start from '
                'minimal, or create the default on this page.',
            )
        target.root.mkdir(parents=True)
        for file in (source.cargo_toml, source.cargo_lock, source.toolchain_toml):
            if file.exists():
                shutil.copy2(file, target.root / file.name)
    ws.init(repo_path, name)


def delete(repo_path: str, pk: str) -> Dict:
    """Removes a named environment's folder (its manifest, lock and library). Refused for
    `default` and while blocks select it: the refusal names them."""
    language, environment = parse_id(pk)
    if environment == DEFAULT:
        raise LanguageEnvironmentError('The default environment cannot be removed.')
    _require(repo_path, language, environment)
    users = block_environments.usage(repo_path, language).get(environment) or []
    if users:
        listed = '; '.join(f'{u["pipeline"]}: {", ".join(u["blocks"])}' for u in users[:5])
        more = f' and {len(users) - 5} more pipelines' if len(users) > 5 else ''
        raise LanguageEnvironmentError(
            f'Blocks run in {environment}: {listed}{more}. Move them to another environment '
            'first.',
        )
    folder = block_environments.folder(repo_path, language, environment)
    shutil.rmtree(folder)
    return dict(id=pk, language=language, environment=environment, deleted=True)
