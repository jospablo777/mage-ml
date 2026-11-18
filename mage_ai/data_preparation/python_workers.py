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
from typing import Dict, List, Optional

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
    import tomllib

    return tomllib.loads(
        (Path(worker.definition) / 'pyproject.toml').read_text()).get('project') or {}


def _lock_text(worker) -> Optional[str]:
    path = Path(worker.definition) / 'uv.lock'
    return path.read_text() if path.is_file() else None


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
            definition=worker.relative_definition(),
            locked=lock is not None,
            prepared=worker.prepared,
            dependencies=list(_project(worker).get('dependencies') or []),
            size_bytes=size,
            used_by=used.get(alias, []),
            security=cached_audit(worker),
        )
        workers.append(item)
    return workers


def detail(repo_path: str, alias: str) -> Dict:
    """One worker with each package it locks: its version, size, and whether the worker
    declares it or another package needs it."""
    import column_atlas_native

    worker = _worker(repo_path, alias)
    [item] = [w for w in overview(repo_path) if w['name'] == worker.alias]
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
            raise PipelineEnvironmentError(
                f'{worker.relative_definition()}/uv.lock cannot be read: {error}')
    for package in packages:
        package['declared'] = package['name'] in declared
    item['packages'] = packages
    return item


def _worker(repo_path: str, alias: str):
    workers = environments.named_workers(repo_path)
    name = environments.normalized_alias(alias or '')
    worker = workers.get(name)
    if worker is None:
        raise PipelineEnvironmentError(
            f'There is no Python worker {alias}; workers: {", ".join(workers) or "none"}.')
    return worker


# ---- adding a worker: locked before it exists --------------------------------------------


def create(
    repo_path: str,
    name: str,
    template: str = 'empty',
    label: Optional[str] = None,
    description: Optional[str] = None,
    dependencies: Optional[List[str]] = None,
    python: Optional[str] = None,
) -> Dict:
    """
    Adds a worker: its pyproject.toml from a template (its packages replaced by
    dependencies when given), locked by uv in a scratch folder, and only then
    python_workers/<name>/ with both files, prepared in the background. A package that does
    not resolve leaves no worker behind.
    """
    import tomlkit

    if dependencies is not None:
        dependencies = _check_requirements(dependencies)
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
        result = subprocess.run(
            [environments._uv(), 'lock', '--project', str(folder)],
            capture_output=True, text=True, timeout=600,
        )
        if result.returncode != 0:
            reason = ('uv could not reach the package index.'
                      if environments._offline(result.stderr)
                      else environments._resolver_reason(result.stderr))
            raise PipelineEnvironmentError(
                f'uv could not lock these packages, so no worker was added: {reason}')
        target = Path(repo_path) / environments.WORKERS_DIR / folder.name
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise PipelineEnvironmentError(f'{environments.WORKERS_DIR}/{folder.name} exists '
                                           'already.')
        # Within the project's file system in one step: the worker appears complete.
        staging = target.with_name(f'.{folder.name}.{os.getpid()}.new')
        shutil.copytree(folder, staging)
        os.replace(staging, target)
    return dict(name=folder.name, job=environments.start_prepare(repo_path, folder.name))


# ---- changing a worker: locked in a copy first -------------------------------------------


def _locked_copy(worker, pyproject: Optional[str] = None,
                 upgrade: Optional[List[str]] = None, python: Optional[str] = None) -> str:
    """
    Locks the worker in a temporary copy, with pyproject (the new pyproject.toml), python
    (its new .python-version, '' for none) and upgrade (["--upgrade"] or
    ["--upgrade-package", "name>=1.2"...]); returns the new uv.lock. Raises with uv's
    reason when it does not lock; the worker is not touched.
    """
    with tempfile.TemporaryDirectory(prefix='mage_worker_') as directory:
        copy = Path(directory) / Path(worker.definition).name
        shutil.copytree(worker.definition, copy, ignore=shutil.ignore_patterns('.venv'))
        if pyproject is not None:
            (copy / 'pyproject.toml').write_text(pyproject)
        if python is not None:
            (copy / '.python-version').unlink(missing_ok=True)
            if python:
                (copy / '.python-version').write_text(f'{python}\n')
        result = subprocess.run(
            [environments._uv(), 'lock', '--project', str(copy), *(upgrade or [])],
            capture_output=True, text=True, timeout=600,
        )
        if result.returncode != 0:
            if environments._offline(result.stderr):
                raise PipelineEnvironmentError(
                    'uv could not reach the package index to lock the change; nothing '
                    'changed.')
            raise PipelineEnvironmentError(
                f'uv could not lock the change, so nothing changed: '
                f'{environments._resolver_reason(result.stderr)}')
        return (copy / 'uv.lock').read_text()


def _changes(old: Optional[str], new: str) -> List[Dict]:
    import column_atlas_native

    return json.loads(column_atlas_native.worker_lock_diff(old or '', new))


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


def update(
    repo_path: str,
    alias: str,
    dependencies: Optional[List[str]] = None,
    label: Optional[str] = None,
    description: Optional[str] = None,
    python: Optional[str] = None,
    combine: Optional[bool] = None,
) -> Dict:
    """
    Changes a worker's packages (dependencies, the full list), its Python, how people name
    it, or whether it may be combined. A change of packages or Python is locked in a copy
    first and then prepared; the result has the packages that moved and the prepare job.
    """
    import tomlkit

    worker = _worker(repo_path, alias)
    folder = Path(worker.definition)
    document = tomlkit.parse((folder / 'pyproject.toml').read_text())
    project = document.setdefault('project', tomlkit.table())
    needs_lock = False
    if dependencies is not None:
        checked = _check_requirements(dependencies)
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
    notes = []
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
    if (label, description, combine) == (None, None, None):
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
    text = tomlkit.dumps(document)
    result = dict(name=worker.alias, changes=[], job=None, notes=notes)
    if not needs_lock:
        # How people name it: no new lock, and the prepared environment stays valid.
        _write(folder / 'pyproject.toml', text)
        return result
    old_lock = _lock_text(worker)
    new_lock = _locked_copy(worker, text, python=python)
    _write(folder / 'pyproject.toml', text)
    _write(folder / 'uv.lock', new_lock)
    if python is not None:
        (folder / '.python-version').unlink(missing_ok=True)
        if python:
            _write(folder / '.python-version', f'{python}\n')
    result['changes'] = _changes(old_lock, new_lock)
    result['job'] = environments.start_prepare(repo_path, worker.alias)
    return result


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
    """Checks, one at a time in a thread, the workers whose last check is missing or old,
    so the Workers page and its badge are current without waiting on uv."""
    key = str(Path(repo_path).resolve())
    with _audits_lock:
        running = _audits_running.get(key)
        if running is not None and running.is_alive():
            return

        def run():
            for alias, worker in environments.named_workers(repo_path).items():
                cached = cached_audit(worker)
                if cached.get('state') == 'unchecked' or cached.get('stale'):
                    if _lock_text(worker) is not None:
                        audit(repo_path, alias)

        thread = threading.Thread(target=run, daemon=True, name='worker-audits')
        _audits_running[key] = thread
        thread.start()


# ---- removing a worker ------------------------------------------------------------------


def delete(repo_path: str, alias: str) -> Dict:
    """Removes the worker's folder and its prepared environments; refused while a block
    runs in it, naming those blocks."""
    worker = _worker(repo_path, alias)
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
        if recorded.get('source') == source or marker.parent == worker.directory:
            marker.unlink(missing_ok=True)
            shutil.rmtree(marker.parent, ignore_errors=True)
            removed += 1
    shutil.rmtree(worker.definition)
    return dict(name=worker.alias, environments_removed=removed)
