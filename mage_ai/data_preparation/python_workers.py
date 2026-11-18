"""
Managing a project's Python workers (python_workers/<name>/), for the Workers page and its
API: what each installs and where it is used, its packages changed safely, its packages
updated and its vulnerabilities fixed after a preview, and a worker removed only when no
block uses it. Preparing and running a worker is environments.py's.

Every change is locked in a copy of the worker first: the worker's own files change only
when uv locked the change, so a bad version range never leaves a worker that cannot be
prepared. A preview is applied as it was shown, or not at all when the worker changed since.
"""
import hashlib
import json
import os
import platform
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from mage_ai.data_preparation import environments
from mage_ai.data_preparation.environments import PipelineEnvironmentError
from mage_ai.shared import vulnerabilities

# A security check of a lock is kept this long: advisories appear every day, uv audit
# takes seconds.
AUDIT_SECONDS = 12 * 3600
# A preview is applied within this time, or previewed again.
PREVIEW_SECONDS = 30 * 60


# ---- what a worker is and where it is used ----------------------------------------------


def server_platform() -> str:
    """The platform this server prepares workers for, as the lock sizes name it."""
    machine = platform.machine().lower()
    if sys.platform == 'darwin':
        return 'macos-arm64'
    return 'linux-aarch64' if machine in ('aarch64', 'arm64') else 'linux-x86_64'


# The packages shown for MageML's own Python, the ones blocks most often care about.
MAGEML_PACKAGES = ('mage-ml', 'pandas', 'polars', 'pyarrow', 'numpy', 'scikit-learn',
                   'sqlalchemy', 'mlflow')


def mageml_environment() -> Dict:
    """
    MageML's own Python, the default worker: where it comes from and how to change it. Read
    only in the app on purpose: changing it means uv syncing the environment the server runs
    in, which can leave a running process with half of a package's files, or pin a version
    MageML cannot start with, and then the page to undo it is gone; in a Docker image the
    change is lost on restart. So the page says how to change it with uv, then restart, and
    that a worker is the place for the packages of some blocks.
    """
    import importlib.metadata

    prefix = Path(sys.prefix)
    project = None
    for folder in (prefix.parent, Path.cwd()):
        candidate = folder / 'pyproject.toml'
        try:
            text = candidate.read_text()
        except OSError:
            continue
        if 'mage-ml' in text or 'mage_ml' in text or 'mage-ai' in text:
            project = candidate
            break
    if Path('/.dockerenv').exists() or str(prefix) == '/opt/mage':
        kind = 'docker'
    elif project is not None:
        kind = 'project'
    elif f'{os.sep}uv{os.sep}tools{os.sep}' in str(prefix):
        kind = 'uv_tool'
    else:
        kind = 'environment'
    example = 'scikit-learn'
    if kind == 'project':
        folder = str(project.parent)
        commands = [f'cd "{folder}"', f'uv add {example}', f'uv remove {example}',
                    'uv lock --upgrade-package pandas && uv sync']
    elif kind == 'uv_tool':
        commands = [f'uv tool install mage-ml --with {example} --reinstall']
    elif kind == 'docker':
        commands = ['# In your own Dockerfile, from the MageML image you run:',
                    f'RUN uv pip install --python {sys.executable} {example}']
    else:
        commands = [f'uv pip install --python {sys.executable} {example}']
    # By name: a package can be listed twice (an egg-info next to its installed copy).
    versions = {}
    for distribution in importlib.metadata.distributions():
        name = (distribution.metadata.get('Name') or '').lower().replace('_', '-')
        if name:
            versions.setdefault(name, distribution.version)
    count = len(versions)
    packages = [dict(name=n, version=versions[n]) for n in MAGEML_PACKAGES if n in versions]
    return dict(
        name='mage',
        builtin=True,
        label='MageML (default)',
        python=f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}',
        prefix=str(prefix),
        project=str(project) if project else None,
        kind=kind,
        commands=commands,
        package_count=count,
        main_packages=packages,
    )


def usage(repo_path: str) -> Dict[str, List[Dict]]:
    """
    The pipelines whose Python blocks run in each worker, from their metadata.yaml (blocks
    are not loaded): {alias: [{pipeline, name, blocks, default}]}, default when the
    pipeline's own setting chose it.
    """
    import yaml

    found: Dict[str, List[Dict]] = {}
    root = Path(repo_path) / 'pipelines'
    if not root.is_dir():
        return found
    for metadata in sorted(root.glob('*/metadata.yaml')):
        try:
            config = yaml.safe_load(metadata.read_text()) or {}
        except (OSError, yaml.YAMLError):
            continue
        default = ((config.get('environment') or {}).get('worker')
                   if isinstance(config.get('environment'), dict) else None)
        by_worker: Dict[str, List[str]] = {}
        for block in config.get('blocks') or []:
            if (block.get('language') or 'python') != 'python':
                continue
            if block.get('type') not in environments.SUPPORTED_BLOCK_TYPES:
                continue
            chosen = (block.get('configuration') or {}).get('python_worker') or default
            if not chosen:
                continue
            alias = environments.normalized_alias(chosen)
            if alias in environments.BUILTIN_WORKERS:
                continue
            alias = environments._folder_alias(repo_path, alias)
            by_worker.setdefault(alias, []).append(block.get('uuid'))
        for alias, blocks in by_worker.items():
            found.setdefault(alias, []).append(dict(
                pipeline=metadata.parent.name,
                name=config.get('name') or metadata.parent.name,
                blocks=blocks,
                default=bool(default) and environments._folder_alias(
                    repo_path, environments.normalized_alias(default)) == alias,
            ))
    return found


def _project(worker) -> Dict:
    """The worker's [project] table, its dependencies a list of strings whatever the file says."""
    import tomllib

    project = tomllib.loads(
        (Path(worker.definition) / 'pyproject.toml').read_text()).get('project')
    project = dict(project) if isinstance(project, dict) else {}
    dependencies = project.get('dependencies')
    project['dependencies'] = ([str(d) for d in dependencies]
                               if isinstance(dependencies, list) else [])
    return project


def _lock_text(worker) -> Optional[str]:
    path = Path(worker.definition) / 'uv.lock'
    return path.read_text(errors='replace') if path.is_file() else None


def _digest(text: Optional[str]) -> str:
    return hashlib.sha256((text or '').encode()).hexdigest()


def overview(repo_path: str) -> List[Dict]:
    """Every worker for the Workers page, with where it is used and its last security
    check (kept from before: listing runs no audit)."""
    import column_atlas_native

    used = usage(repo_path)
    described = {w['name']: w for w in environments.describe_workers(repo_path)}
    workers = []
    for alias, worker in environments.named_workers(repo_path).items():
        lock = _lock_text(worker)
        size = None
        if lock:
            try:
                size = json.loads(column_atlas_native.worker_lock_size(
                    lock, server_platform(), worker.python))['bytes']
            except ValueError:
                size = None
        item = dict(described.get(alias) or {})
        item.update(
            name=alias,
            label=worker.label,
            description=worker.description,
            python=worker.python,
            combine=worker.combine,
            memory_limit=worker.memory_limit,
            definition=worker.relative_definition(),
            locked=lock is not None,
            prepared=worker.prepared,
            dependencies=list(_project(worker).get('dependencies') or []),
            size_bytes=size,
            used_by=used.get(alias, []),
            security=cached_audit(worker),
            held_back=cached_held_back(worker),
        )
        workers.append(item)
    # Listed with why, so the page can say it and open the file to fix it.
    for alias, info in environments.broken_workers(repo_path).items():
        workers.append(dict(
            name=alias, label=None, description=None, python=None,
            definition=info['definition'], broken=info['reason'], locked=False,
            prepared=False, dependencies=[], size_bytes=None,
            used_by=used.get(alias, []), security=dict(state='unchecked'), held_back=[],
        ))
    return sorted(workers, key=lambda w: w['name'])


def detail(repo_path: str, alias: str) -> Dict:
    """One worker with each package it locks: its version, size, and whether the worker
    declares it or another package needs it."""
    import column_atlas_native

    worker = _worker(repo_path, alias, broken_ok=True)
    [item] = [w for w in overview(repo_path) if w['name'] == worker.alias]
    item['pyproject'] = (Path(worker.definition) / 'pyproject.toml').read_text(
        errors='replace')
    if item.get('broken'):
        item['packages'] = []
        return item
    lock = _lock_text(worker)
    declared = {
        environments._requirement_name(d) for d in item['dependencies']
    }
    packages = []
    if lock:
        try:
            packages = json.loads(column_atlas_native.worker_lock_packages(
                lock, server_platform(), worker.python))
        except ValueError as error:
            # Shown on its page; saving the packages or the file locks it again.
            item['lock_problem'] = (
                f'{worker.relative_definition()}/{error} Save the packages again to lock '
                'them anew.')
    for package in packages:
        package['declared'] = package['name'] in declared
    item['packages'] = packages
    return item


def _worker(repo_path: str, alias: str, broken_ok: bool = False):
    """
    The worker named alias. With broken_ok, also one whose pyproject.toml cannot be read,
    for the calls that fix or remove it (detail, save_pyproject, delete): it has the alias,
    folder and Python those use.
    """
    workers, broken = environments._scan_workers(repo_path)
    name = environments.normalized_alias(alias or '')
    worker = workers.get(name)
    if worker is None and broken_ok and name in broken:
        return _BrokenWorker(repo_path, name, broken[name])
    if worker is None and name in broken:
        raise PipelineEnvironmentError(
            f'Python worker {name} cannot be used: {broken[name]["reason"]}')
    if worker is None:
        raise PipelineEnvironmentError(
            f'There is no Python worker {alias}; workers: {", ".join(workers) or "none"}.')
    return worker


class _BrokenWorker:
    """A worker whose pyproject.toml cannot be read, as far as fixing or removing it goes."""

    def __init__(self, repo_path: str, alias: str, info: Dict[str, str]):
        self.alias = alias
        self.definition = str(Path(repo_path) / info['definition'])
        self.broken = info['reason']
        version_file = Path(self.definition) / '.python-version'
        try:
            self.python = version_file.read_text().strip()
        except OSError:
            self.python = f'{sys.version_info.major}.{sys.version_info.minor}'
        self.directory = None

    def relative_definition(self) -> str:
        return f'{environments.WORKERS_DIR}/{Path(self.definition).name}'


# ---- adding a worker: locked before it exists --------------------------------------------


def create(
    repo_path: str,
    name: str,
    template: str = 'empty',
    label: Optional[str] = None,
    description: Optional[str] = None,
    dependencies: Optional[List[str]] = None,
    python: Optional[str] = None,
    accept_older: bool = False,
) -> Dict:
    """
    Adds a worker: its pyproject.toml from a template (its packages replaced by
    dependencies when given), locked by uv in a scratch folder, and only then
    python_workers/<name>/ with both files, prepared in the background. A package that does
    not resolve leaves no worker behind. Unless accept_older, a lock with a package below
    its newest release that does not fit (_older_releases) adds nothing and comes back in
    confirm, for people to say whether to go on.
    """
    import tomlkit

    notes = []
    if dependencies is not None:
        dependencies, note = _with_pyarrow(_check_requirements(dependencies))
        if note:
            notes.append(note)
    with tempfile.TemporaryDirectory(prefix='mage_new_worker_') as directory:
        scratch = Path(directory)
        # new_worker checks the name and the template, and writes the pyproject.toml.
        folder = environments.new_worker(str(scratch), name, template, label=label,
                                         description=description)
        if environments.normalized_alias(name) in environments.named_workers(repo_path):
            raise PipelineEnvironmentError(f'A Python worker named {folder.name} exists already.')
        document = tomlkit.parse((folder / 'pyproject.toml').read_text())
        if dependencies is not None:
            array = tomlkit.array()
            array.multiline(True)
            for dependency in dependencies:
                array.append(dependency)
            document['project']['dependencies'] = array
        if python:
            python = python.strip()
            if not python.replace('.', '').isdigit():
                raise PipelineEnvironmentError(
                    f'"{python}" is not a Python version, such as 3.12.')
            document['project']['requires-python'] = f'>={python}'
            (folder / '.python-version').write_text(f'{python}\n')
        (folder / 'pyproject.toml').write_text(tomlkit.dumps(document))
        result = _uv_lock(folder)
        if result.returncode != 0:
            reason = ('uv could not reach the package index.'
                      if environments._offline(result.stderr)
                      else _conflict(result.stderr, (folder / 'pyproject.toml').read_text()))
            raise PipelineEnvironmentError(
                f'uv could not lock these packages, so no worker was added: {reason}'
                + _uv_output(result.stderr))
        if not accept_older:
            older = _older_releases(
                (folder / 'pyproject.toml').read_text(), (folder / 'uv.lock').read_text(),
                python or f'{sys.version_info.major}.{sys.version_info.minor}',
                lambda text: _lock_in(folder, text))
            if older:
                return dict(name=folder.name, job=None, confirm=older, notes=notes)
        target = Path(repo_path) / environments.WORKERS_DIR / folder.name
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise PipelineEnvironmentError(f'{environments.WORKERS_DIR}/{folder.name} exists '
                                           'already.')
        # Within the project's file system in one step: the worker appears complete.
        staging = target.with_name(f'.{folder.name}.{os.getpid()}.new')
        shutil.copytree(folder, staging)
        os.replace(staging, target)
    return dict(name=folder.name, job=environments.start_prepare(repo_path, folder.name),
                notes=notes)


# ---- changing a worker: locked in a copy first -------------------------------------------


def _locked_copy(worker, pyproject: Optional[str] = None,
                 upgrade: Optional[List[str]] = None, python: Optional[str] = None) -> str:
    """
    Locks the worker in a temporary copy, with pyproject (the new pyproject.toml), python
    (its new .python-version, '' for none) and upgrade (["--upgrade"] or
    ["--upgrade-package", "name>=1.2"...]); returns the new uv.lock. Raises with uv's
    reason when it does not lock; the worker is not touched.
    """
    return _lock_in(Path(worker.definition), pyproject, upgrade=upgrade, python=python)


def _lock_in(definition: Path, pyproject: Optional[str] = None,
             upgrade: Optional[List[str]] = None, python: Optional[str] = None) -> str:
    """_locked_copy of a worker's folder, or of a new worker's before it exists."""
    with tempfile.TemporaryDirectory(prefix='mage_worker_') as directory:
        copy = Path(directory) / definition.name
        shutil.copytree(definition, copy, ignore=shutil.ignore_patterns('.venv'))
        if pyproject is not None:
            (copy / 'pyproject.toml').write_text(pyproject)
        if python is not None:
            (copy / '.python-version').unlink(missing_ok=True)
            if python:
                (copy / '.python-version').write_text(f'{python}\n')
        result = _uv_lock(copy, upgrade)
        if result.returncode != 0 and 'failed to parse `uv.lock`' in result.stderr.lower():
            # The worker's uv.lock is broken (an edit, a merge conflict): locked anew from
            # pyproject.toml, which is what saving the change writes anyway.
            (copy / 'uv.lock').unlink(missing_ok=True)
            result = _uv_lock(copy, upgrade)
        if result.returncode != 0:
            if environments._offline(result.stderr):
                raise PipelineEnvironmentError(
                    'uv could not reach the package index to lock the change; nothing '
                    'changed.')
            error = PipelineEnvironmentError(
                'uv could not lock the change, so nothing changed: '
                + _conflict(result.stderr, (copy / 'pyproject.toml').read_text())
                + _uv_output(result.stderr))
            # uv's own explanation, for _held.
            error.uv_reason = environments._resolver_reason(result.stderr)
            raise error
        return (copy / 'uv.lock').read_text()


LOCK_TIMEOUT = 600


def _uv_lock(folder: Path, upgrade: Optional[List[str]] = None):
    """uv lock in folder; a lock that takes over LOCK_TIMEOUT seconds says so."""
    try:
        return subprocess.run(
            [environments._uv(), 'lock', '--project', str(folder), *(upgrade or [])],
            capture_output=True, text=True, timeout=LOCK_TIMEOUT, env=environments.uv_env(),
        )
    except subprocess.TimeoutExpired:
        raise PipelineEnvironmentError(
            f'uv took over {LOCK_TIMEOUT // 60} minutes to lock the packages and was stopped; '
            'nothing changed. A slow package index, or packages whose versions uv has to '
            'try one by one: give the large ones a version range.')


def _uv_output(stderr: str, lines: int = 40) -> str:
    """uv's own words after the summary, for the page's "Show uv's output": a blank line,
    then its last lines without the progress ones."""
    kept = [line.rstrip() for line in environments.plain(stderr).splitlines()
            if line.strip() and not line.strip().startswith(('Using ', 'Resolved ', 'Updating '))]
    return '\n\n' + '\n'.join(kept[-lines:]) if kept else ''


def _changes(old: Optional[str], new: str) -> List[Dict]:
    import column_atlas_native

    try:
        return json.loads(column_atlas_native.worker_lock_diff(old or '', new))
    except ValueError:
        # The old lock could not be read (a broken file): every package is new.
        return json.loads(column_atlas_native.worker_lock_diff('', new))


def _check_requirements(dependencies: List[str]) -> List[str]:
    from packaging.requirements import InvalidRequirement, Requirement

    checked = []
    for line in dependencies:
        text = (line or '').strip()
        if not text:
            continue
        try:
            Requirement(text)
        except InvalidRequirement as error:
            raise PipelineEnvironmentError(
                f'"{text}" is not a package requirement ({error}). Write a name with an '
                'optional version range, such as pandas>=2.2,<3 or feast[redis]==0.50.0.')
        checked.append(text)
    return checked


PYARROW_NOTE = ('pyarrow was added: tables reach a worker as Arrow, and pandas reads Arrow '
                'with pyarrow.')


def _with_pyarrow(dependencies: List[str]) -> Tuple[List[str], Optional[str]]:
    """
    The dependencies with pyarrow when they list pandas without it: a table reaches a worker
    as Arrow, which pandas reads only with pyarrow (Polars reads it itself). Without it, the
    first block that takes a table fails.
    """
    names = {(environments._requirement_name(d) or '').lower().replace('_', '-')
             for d in dependencies}
    if 'pandas' in names and 'pyarrow' not in names:
        return [*dependencies, 'pyarrow'], PYARROW_NOTE
    return dependencies, None


def _lacks_pyarrow(lock: str, python: str) -> bool:
    """Whether a lock installs pandas without pyarrow."""
    try:
        names = {p['name'] for p in json.loads(_lock_packages(lock, python))}
    except ValueError:
        return False
    return 'pandas' in names and 'pyarrow' not in names


def update(
    repo_path: str,
    alias: str,
    dependencies: Optional[List[str]] = None,
    label: Optional[str] = None,
    description: Optional[str] = None,
    python: Optional[str] = None,
    combine: Optional[bool] = None,
    memory: Optional[str] = None,
    accept_older: bool = False,
) -> Dict:
    """
    Changes a worker's packages (dependencies, the full list), its Python, how people name
    it, whether it may be combined, or the memory its processes may use ('' for no limit).
    A change of packages or Python is locked in a copy first and then prepared; the result
    has the packages that moved and the prepare job. Unless accept_older, a change that
    would lock a package below its newest release because the newest does not fit changes
    nothing and comes back in confirm (_older_releases).
    """
    import tomlkit

    worker = _worker(repo_path, alias)
    folder = Path(worker.definition)
    document = tomlkit.parse((folder / 'pyproject.toml').read_text())
    project = document.setdefault('project', tomlkit.table())
    needs_lock = False
    upgrade: List[str] = []
    notes = []
    if dependencies is not None:
        checked, note = _with_pyarrow(_check_requirements(dependencies))
        if note:
            notes.append(note)
        # A package whose line changed gets the newest release its new line allows: uv lock
        # keeps a locked version that still fits, so "six" after "six==1.16.0" stayed 1.16.0.
        before = {environments._requirement_name(d): d.strip()
                  for d in project.get('dependencies') or []}
        for line in checked:
            name = environments._requirement_name(line)
            if name in before and before[name] != line:
                upgrade += ['--upgrade-package', name]
        names = {environments._requirement_name(d) for d in checked}
        own = (project.get('name') or '').lower().replace('_', '-').replace('.', '-')
        if own in names:
            project['name'] = f'mageml-worker-{worker.alias}'
        array = tomlkit.array()
        array.multiline(True)
        for dependency in checked:
            array.append(dependency)
        project['dependencies'] = array
        needs_lock = True
    if python is not None:
        python = python.strip()
        if python and not python.replace('.', '').isdigit():
            raise PipelineEnvironmentError(f'"{python}" is not a Python version, such as 3.12.')
        requires = project.get('requires-python')
        if python and requires:
            from packaging.specifiers import SpecifierSet

            allowed = SpecifierSet(str(requires))
            if not (allowed.contains(f'{python}.0') or allowed.contains(f'{python}.99')):
                project['requires-python'] = f'>={python}'
                notes.append(f'requires-python was {requires}, which excludes Python {python}; '
                             f'it is >={python} now.')
        needs_lock = True
    if memory:
        try:
            environments.parse_bytes(memory)
        except ValueError as error:
            raise PipelineEnvironmentError(f'Memory: {error}')
    if (label, description, combine, memory) == (None, None, None, None):
        settings = {}
    else:
        settings = document.setdefault('tool', tomlkit.table()).setdefault(
            'mageml', tomlkit.table()).setdefault('worker', tomlkit.table())
    for key, value in (('label', label), ('description', description)):
        if value is not None:
            if value.strip():
                settings[key] = value.strip()
            elif key in settings:
                del settings[key]
    if combine is not None:
        if combine:
            settings.pop('combine', None)
        else:
            settings['combine'] = False
    if memory is not None:
        if memory.strip():
            settings['memory'] = memory.strip()
        else:
            settings.pop('memory', None)
    text = tomlkit.dumps(document)
    result = dict(name=worker.alias, changes=[], job=None, notes=notes)
    if not needs_lock:
        # How people name it: no new lock, and the prepared environment stays valid.
        _write(folder / 'pyproject.toml', text)
        return result
    old_lock = _lock_text(worker)
    new_lock = _locked_copy(worker, text, python=python, upgrade=upgrade)
    if not accept_older:
        older = _older_releases(text, new_lock, python or worker.python,
                                lambda trial: _locked_copy(worker, trial, python=python),
                                old_lock=old_lock)
        if older:
            return dict(result, confirm=older)
    _write(folder / 'pyproject.toml', text)
    _write(folder / 'uv.lock', new_lock)
    if python is not None:
        (folder / '.python-version').unlink(missing_ok=True)
        if python:
            _write(folder / '.python-version', f'{python}\n')
    result['changes'] = _changes(old_lock, new_lock)
    result['job'] = environments.start_prepare(repo_path, worker.alias)
    return result


def save_pyproject(repo_path: str, alias: str, text: str, accept_older: bool = False) -> Dict:
    """
    Replaces the worker's pyproject.toml with text, as people edit it on the Workers page:
    refused when it is not TOML or names the project like one of its packages, locked in a
    copy first, then written with its new lock and prepared. Unless accept_older, a lock
    with a package newly below its newest release comes back in confirm, as in update.
    """
    import tomllib

    worker = _worker(repo_path, alias, broken_ok=True)
    try:
        parsed = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        raise PipelineEnvironmentError(f'pyproject.toml is not valid TOML: {error}.')
    project = parsed.get('project')
    if not isinstance(project, dict):
        raise PipelineEnvironmentError(
            'pyproject.toml needs a [project] table with its name, version and dependencies.')
    if not isinstance(project.get('name'), str) or not project['name'].strip():
        raise PipelineEnvironmentError(
            f'pyproject.toml\'s [project] needs a name, such as name = '
            f'"mageml-worker-{worker.alias}".')
    if not isinstance(project.get('dependencies', []), list):
        raise PipelineEnvironmentError(
            'dependencies in pyproject.toml is a list of packages, such as dependencies = '
            '["pandas", "scikit-learn>=1.5"].')
    _check_requirements(project.get('dependencies') or [])
    name = str(project.get('name') or '').lower().replace('_', '-').replace('.', '-')
    for dependency in project.get('dependencies') or []:
        if name and (environments._requirement_name(dependency) or '').replace('.', '-') == name:
            raise PipelineEnvironmentError(
                f'The project is named "{project["name"]}", like the package it needs, so uv '
                f'would take the project for the package: name it '
                f'"mageml-worker-{worker.alias}".')
    old_lock = _lock_text(worker)
    new_lock = _locked_copy(worker, text)
    if not accept_older:
        older = _older_releases(text, new_lock, worker.python,
                                lambda trial: _locked_copy(worker, trial), old_lock=old_lock)
        if older:
            return dict(name=worker.alias, changes=[], job=None, confirm=older)
    folder = Path(worker.definition)
    _write(folder / 'pyproject.toml', text)
    _write(folder / 'uv.lock', new_lock)
    notes = []
    if _lacks_pyarrow(new_lock, worker.python):
        notes.append('It installs pandas without pyarrow: a block that takes a table fails. '
                     'Add pyarrow to its dependencies.')
    return dict(name=worker.alias, changes=_changes(old_lock, new_lock), notes=notes,
                job=environments.start_prepare(repo_path, worker.alias))


def _write(path: Path, text: str) -> None:
    """Replaces a file in one step: a reader sees the old or the new one."""
    staging = path.with_name(f'.{path.name}.{os.getpid()}.tmp')
    staging.write_text(text)
    os.replace(staging, path)


# ---- updates and fixes: previewed, then applied as shown ---------------------------------

_previews: Dict[str, Dict] = {}
_previews_lock = threading.Lock()


def preview(
    repo_path: str,
    alias: str,
    packages: Optional[List[str]] = None,
    fix_vulnerabilities: bool = False,
) -> Dict:
    """
    What updating a worker would change, without changing it: every package to the newest
    release its ranges allow, the packages named, or the releases that fix its known
    vulnerabilities. Returns an id to apply it with, the packages that move, and for fixes
    those that stay because another package's range holds them back (uv's reason).
    """
    worker = _worker(repo_path, alias)
    old_lock = _lock_text(worker)
    if old_lock is None:
        raise PipelineEnvironmentError(
            f'Python worker {worker.alias} has no uv.lock: add a package to lock it first.')
    held: List[Dict] = []
    if fix_vulnerabilities:
        report = audit(repo_path, alias)
        targets = [(p['package'], p['fix']) for p in report['packages'] if p['fix']]
        if not targets:
            return dict(id=None, name=worker.alias, changes=[], held=[],
                        message='Nothing to fix: no known vulnerability has a fixed release.')
        upgrade = []
        for name, version in targets:
            upgrade += ['--upgrade-package', f'{name}>={version}']
        try:
            new_lock = _locked_copy(worker, upgrade=upgrade)
        except PipelineEnvironmentError:
            # One held package should not keep the others back: each alone, then the ones
            # that locked together.
            fixable = []
            for name, version in targets:
                try:
                    _locked_copy(worker, upgrade=['--upgrade-package', f'{name}>={version}'])
                    fixable.append((name, version))
                except PipelineEnvironmentError as error:
                    held.append(dict(package=name, to=version, reason=str(error)))
            upgrade = []
            for name, version in fixable:
                upgrade += ['--upgrade-package', f'{name}>={version}']
            new_lock = _locked_copy(worker, upgrade=upgrade) if upgrade else old_lock
    elif packages:
        upgrade = []
        for name in packages:
            upgrade += ['--upgrade-package', name]
        new_lock = _locked_copy(worker, upgrade=upgrade)
    else:
        new_lock = _locked_copy(worker, upgrade=['--upgrade'])
    changes = _changes(old_lock, new_lock)
    if not changes:
        return dict(id=None, name=worker.alias, changes=[], held=held,
                    message='Every package is at the newest release its ranges allow.'
                    if not fix_vulnerabilities else 'Nothing could be fixed.')
    token = secrets.token_urlsafe(12)
    with _previews_lock:
        now = time.time()
        for key in [k for k, v in _previews.items() if now - v['at'] > PREVIEW_SECONDS]:
            _previews.pop(key, None)
        _previews[token] = dict(
            at=now, repo=str(Path(repo_path).resolve()), name=worker.alias,
            base=_digest(old_lock), lock=new_lock,
        )
    return dict(id=token, name=worker.alias, changes=changes, held=held)


def apply(repo_path: str, token: str) -> Dict:
    """Writes the lock a preview showed and prepares the worker; refused when the worker's
    lock changed since the preview, or the preview is too old."""
    with _previews_lock:
        shown = _previews.pop(token, None)
    if shown is None or time.time() - shown['at'] > PREVIEW_SECONDS:
        raise PipelineEnvironmentError('This preview has expired: preview the update again.')
    if shown['repo'] != str(Path(repo_path).resolve()):
        raise PipelineEnvironmentError('This preview belongs to another project.')
    worker = _worker(repo_path, shown['name'])
    if _digest(_lock_text(worker)) != shown['base']:
        raise PipelineEnvironmentError(
            f'Python worker {worker.alias} changed since the preview: preview it again.')
    _write(Path(worker.definition) / 'uv.lock', shown['lock'])
    return dict(name=worker.alias, job=environments.start_prepare(repo_path, worker.alias))


# ---- security --------------------------------------------------------------------------

_audits_running: Dict[str, threading.Thread] = {}
_audits_lock = threading.Lock()


def _audit_path(lock: str) -> Path:
    return environments.environments_dir() / 'audits' / f'{_digest(lock)[:32]}.json'


def cached_audit(worker) -> Dict:
    """The last security check of the worker's current lock: state 'clean', 'vulnerable',
    'unchecked' (never, or uv could not reach OSV) or 'stale' (older than AUDIT_SECONDS)."""
    lock = _lock_text(worker)
    if lock is None:
        return dict(state='unchecked', reason='The worker has no uv.lock yet.')
    try:
        report = json.loads(_audit_path(lock).read_text())
    except (OSError, ValueError):
        return dict(state='unchecked')
    summary = {k: report[k] for k in ('state', 'checked_at', 'advisories', 'fixable')
               if k in report}
    if time.time() - report.get('checked_at', 0) > AUDIT_SECONDS:
        summary['stale'] = True
    return summary


def audit(repo_path: str, alias: str, refresh: bool = False) -> Dict:
    """
    The worker's known vulnerabilities from uv audit (OSV): each vulnerable package with
    its advisories and the release that fixes them all. Kept per lock for AUDIT_SECONDS.
    """
    worker = _worker(repo_path, alias)
    lock = _lock_text(worker)
    if lock is None:
        return dict(name=worker.alias, state='unchecked', packages=[],
                    reason='The worker has no uv.lock yet.')
    path = _audit_path(lock)
    if not refresh:
        try:
            report = json.loads(path.read_text())
            if time.time() - report['checked_at'] <= AUDIT_SECONDS:
                return dict(report, name=worker.alias)
        except (OSError, ValueError, KeyError):
            pass
    try:
        found = vulnerabilities.audit(worker.definition, uv=environments._uv())
    except vulnerabilities.AuditError as error:
        reason = ('uv could not reach the vulnerability database (OSV).' if error.offline
                  else f'uv could not check the worker: {error}')
        return dict(name=worker.alias, state='unchecked', packages=[], reason=reason)
    packages = [
        dict(
            package=p.package, version=p.version, fix=p.fix, unfixed=p.unfixed,
            advisories=[a.__dict__ for a in p.advisories],
        )
        for p in found
    ]
    report = dict(
        state='vulnerable' if packages else 'clean',
        checked_at=time.time(),
        advisories=sum(len(p['advisories']) for p in packages),
        fixable=sum(1 for p in packages if p['fix']),
        packages=packages,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    _write(path, json.dumps(report))
    return dict(report, name=worker.alias)


def audit_stale_in_background(repo_path: str) -> None:
    """Checks, one at a time in a thread, the workers whose last security check or held-back
    check is missing or old, so the Workers page and its badge are current without waiting
    on uv."""
    key = str(Path(repo_path).resolve())
    with _audits_lock:
        running = _audits_running.get(key)
        if running is not None and running.is_alive():
            return

        def run():
            for alias, worker in environments.named_workers(repo_path).items():
                # A worker removed or changed meanwhile is checked on the next visit.
                try:
                    if _lock_text(worker) is None:
                        continue
                    cached = cached_audit(worker)
                    if cached.get('state') == 'unchecked' or cached.get('stale'):
                        audit(repo_path, alias)
                    # Read from its cache when fresh; a new lock is checked once.
                    held_back(repo_path, alias)
                except (PipelineEnvironmentError, OSError, ValueError):
                    continue

        thread = threading.Thread(target=run, daemon=True, name='worker-audits')
        _audits_running[key] = thread
        thread.start()


# ---- packages held back ------------------------------------------------------------------

# Checked at most this many listed packages per lock: each is one uv lock.
HELD_BACK_CHECKS = 10


def _held_back_path(lock: str, pyproject: str) -> Path:
    return (environments.environments_dir() / 'held_back'
            / f'{_digest(lock + pyproject)[:32]}.json')


def held_back(repo_path: str, alias: str, refresh: bool = False) -> Dict:
    """
    The listed packages locked below their newest release although their own line allows
    it: uv chose an older release because the newest does not fit the other packages (or
    the worker's Python). With an old release whose requirements were written before the
    other package's new major version (feast 0.20.0 lists pandas>=1 and locks with pandas 3;
    feast 0.68.0 needs pandas<3) the lock resolves and the code breaks, so each is named
    with why: uv's answer when a copy is locked with the newest release required. A package
    whose newest release locks is only behind (an update brings it). Kept per lock and
    pyproject.toml for AUDIT_SECONDS.
    """
    worker = _worker(repo_path, alias)
    lock = _lock_text(worker)
    if lock is None:
        return dict(name=worker.alias, state='unchecked', packages=[],
                    reason='The worker has no uv.lock yet.')
    pyproject = (Path(worker.definition) / 'pyproject.toml').read_text()
    try:
        _lock_packages(lock, worker.python)
    except ValueError as error:
        return dict(name=worker.alias, state='unchecked', packages=[],
                    reason=f'{worker.relative_definition()}/{error}')
    path = _held_back_path(lock, pyproject)
    if not refresh:
        try:
            report = json.loads(path.read_text())
            if time.time() - report['checked_at'] <= AUDIT_SECONDS:
                return dict(report, name=worker.alias)
        except (OSError, ValueError, KeyError):
            pass
    packages, unreachable = _held(pyproject, lock, worker.python,
                                  lambda trial: _locked_copy(worker, trial))
    report = dict(
        state='held' if any(p['state'] == 'held' for p in packages) else 'current',
        checked_at=time.time(),
        packages=packages,
    )
    if unreachable:
        report['reason'] = ('The package index could not be reached for '
                            + ', '.join(unreachable) + '.')
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        _write(path, json.dumps(report))
    return dict(report, name=worker.alias)


def _held(pyproject: str, lock: str, python: str, lock_trial) -> Tuple[List[Dict], List[str]]:
    """
    held_back's check of a pyproject.toml and its lock: lock_trial(text) locks a copy with
    text as its pyproject.toml, raising when it does not lock. Returns the packages found
    and the ones the package index did not answer for.
    """
    import tomlkit
    from packaging.requirements import InvalidRequirement, Requirement
    from packaging.utils import canonicalize_name
    from packaging.version import InvalidVersion, Version

    from mage_ai.data_preparation import package_index

    locked = {
        canonicalize_name(p['name']): p['version']
        for p in json.loads(_lock_packages(lock, python))
    }
    document = tomlkit.parse(pyproject)
    listed = list(document.get('project', {}).get('dependencies') or [])
    candidates, unreachable = [], []
    for index, line in enumerate(listed):
        try:
            requirement = Requirement(str(line))
        except InvalidRequirement:
            continue
        name = canonicalize_name(requirement.name)
        version = locked.get(name)
        if requirement.url or requirement.marker or not version:
            continue
        try:
            found = package_index.releases(name)
        except package_index.PackageIndexError:
            unreachable.append(name)
            continue
        latest = found.get('latest')
        try:
            behind = latest and Version(version) < Version(latest)
        except InvalidVersion:
            continue
        # A line that leaves the newest release out keeps the package where it was asked.
        if behind and requirement.specifier.contains(latest, prereleases=True):
            candidates.append((index, requirement, name, version, found))
    packages = []
    for index, requirement, name, version, found in candidates[:HELD_BACK_CHECKS]:
        latest = found['latest']
        extras = f'[{",".join(sorted(requirement.extras))}]' if requirement.extras else ''
        trial = tomlkit.parse(pyproject)
        trial['project']['dependencies'][index] = f'{requirement.name}{extras}>={latest}'
        entry = dict(
            package=name, locked=version, latest=latest,
            locked_uploaded=(found.get('uploaded') or {}).get(version),
            latest_uploaded=(found.get('uploaded') or {}).get(latest),
            behind=(found['versions'].index(version) if version in found['versions'] else None),
        )
        try:
            lock_trial(tomlkit.dumps(trial))
        except PipelineEnvironmentError as error:
            uv_reason = getattr(error, 'uv_reason', None)
            if uv_reason is None:
                # The package index did not answer.
                unreachable.append(name)
                continue
            entry.update(state='held', uv_reason=uv_reason,
                         reason=_held_reason(name, latest, uv_reason, listed))
        else:
            entry.update(state='behind', reason=(
                f'{latest} fits the other packages: check for updates to move to it.'))
        packages.append(entry)
    return packages, unreachable


def _older_releases(pyproject: str, lock: str, python: str, lock_trial,
                    old_lock: Optional[str] = None) -> List[Dict]:
    """
    The packages a change would lock below their newest release because the newest does
    not fit (_held, state held), with the question to ask: "uv would lock feast 0.20.0 from
    April 2022, 4 years old; the newest is 0.68.0...". A package held at the same version
    before the change was already accepted and is not asked about again.
    """
    from packaging.utils import canonicalize_name

    packages, _ = _held(pyproject, lock, python, lock_trial)
    before = {}
    if old_lock:
        try:
            before = {canonicalize_name(p['name']): p['version']
                      for p in json.loads(_lock_packages(old_lock, python))}
        except ValueError:
            before = {}
    older = []
    for package in packages:
        if package['state'] != 'held' or before.get(package['package']) == package['locked']:
            continue
        age = _age(package.get('locked_uploaded'))
        newest = _month(package.get('latest_uploaded'))
        package['question'] = (
            f"uv would lock {package['package']} {package['locked']}"
            + (f' from {_month(package["locked_uploaded"])}' if package.get('locked_uploaded')
               else '')
            + (f', {age} old' if age else '')
            + f"; the newest is {package['latest']}"
            + (f' ({newest})' if newest else '')
            + f". {package['reason']}"
        )
        older.append(package)
    return older


def _month(iso: Optional[str]) -> Optional[str]:
    from datetime import datetime

    try:
        return datetime.fromisoformat(iso.replace('Z', '+00:00')).strftime('%B %Y')
    except (AttributeError, ValueError):
        return None


def _age(iso: Optional[str]) -> Optional[str]:
    """How long ago a release came out, in years or months: "4 years"."""
    from datetime import datetime, timezone

    try:
        then = datetime.fromisoformat(iso.replace('Z', '+00:00'))
    except (AttributeError, ValueError):
        return None
    months = int((datetime.now(timezone.utc) - then).days / 30.44)
    if months >= 24:
        return f'{months // 12} years'
    if months >= 12:
        return '1 year'
    return f'{months} months' if months >= 2 else None


def _conflict(stderr: str, pyproject: str) -> str:
    """
    Why uv could not lock, in one sentence where uv says which package needs what
    ("feast 0.68.0 needs pandas>=1.4.3,<3, and the worker lists pandas==3.1.1"), or that a
    version does not exist; else uv's own explanation (environments._resolver_reason).
    """
    import re
    import tomllib

    reason = environments._resolver_reason(stderr)
    text = ' '.join(reason.split())
    try:
        listed = [str(d) for d in (tomllib.loads(pyproject).get('project') or {}).get(
            'dependencies') or []]
    except tomllib.TOMLDecodeError:
        listed = []
    missing = re.search(r'there is no version of ([^\s,]+)', text, re.IGNORECASE)
    if missing:
        return f'There is no release {missing.group(1).rstrip(".")} on the package index.'
    needs = re.search(r'([A-Za-z0-9_.\-]+)==([^\s,]+) depends on ', text)
    if needs:
        return _held_reason(needs.group(1), needs.group(2), reason, listed)
    return reason


def cached_held_back(worker) -> List[str]:
    """The packages the last check of the worker's lock found held back; none unchecked."""
    lock = _lock_text(worker)
    if lock is None:
        return []
    try:
        pyproject = (Path(worker.definition) / 'pyproject.toml').read_text()
        report = json.loads(_held_back_path(lock, pyproject).read_text())
    except (OSError, ValueError):
        return []
    return [p['package'] for p in report.get('packages') or [] if p.get('state') == 'held']


def _lock_packages(lock: str, python: str) -> str:
    import column_atlas_native

    return column_atlas_native.worker_lock_packages(lock, server_platform(), python)


def _held_reason(name: str, latest: str, uv_reason: str, listed: List[str]) -> str:
    """
    Why the newest release of a package does not fit, in one sentence: what it needs and the
    listed package that rules it out, from uv's explanation ("feast==0.68.0 depends on
    pandas>=1.4.3,<3"); uv's own sentence when it says something else.
    """
    import re

    from packaging.utils import canonicalize_name

    text = ' '.join(uv_reason.split())
    needs = re.search(
        rf'{re.escape(name)}=={re.escape(latest)} depends on ([A-Za-z0-9_.\-\[\]]+)'
        r'((?:[<>=!~]=?[^,\s]+)(?:,[<>=!~]=?[^,\s]+)*)', text, re.IGNORECASE)
    if not needs:
        return f'{name} {latest} does not fit: {uv_reason}'
    other, spec = needs.group(1), needs.group(2).rstrip('.,')
    sentence = f'{name} {latest} needs {other}{spec}'
    if other.lower() == 'python':
        return sentence + ', and the worker runs another Python.'
    for line in listed:
        line_name = environments._requirement_name(str(line)) or ''
        if canonicalize_name(line_name) == canonicalize_name(other):
            return sentence + f', and the worker lists {line}.'
    return sentence + ', which another package rules out.'


# ---- removing a worker ------------------------------------------------------------------


def delete(repo_path: str, alias: str) -> Dict:
    """Removes the worker's folder and its prepared environments; refused while a block
    runs in it, naming those blocks."""
    worker = _worker(repo_path, alias, broken_ok=True)
    used = usage(repo_path).get(worker.alias) or []
    if used:
        where = '; '.join(f'{u["pipeline"]} ({", ".join(u["blocks"])})' for u in used)
        raise PipelineEnvironmentError(
            f'Python worker {worker.alias} runs blocks of {len(used)} pipeline'
            f'{"" if len(used) == 1 else "s"}: {where}. Choose another worker for them '
            'first.')
    source = str(Path(worker.definition).resolve())
    removed = 0
    for marker in environments.environments_dir().glob(f'*/{environments.MARKER}'):
        try:
            recorded = json.loads(marker.read_text())
        except (OSError, ValueError):
            continue
        if recorded.get('source') == source or (
                worker.directory is not None and marker.parent == worker.directory):
            marker.unlink(missing_ok=True)
            shutil.rmtree(marker.parent, ignore_errors=True)
            removed += 1
    shutil.rmtree(worker.definition)
    return dict(name=worker.alias, environments_removed=removed)
