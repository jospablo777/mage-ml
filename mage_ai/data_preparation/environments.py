"""
Pipeline environments: the Python blocks of a pipeline that declares an environment run
in their own virtual environment, with the packages its requirements file lists, instead
of Mage's. Pipelines that need different versions of a library no longer share one set
of packages.

    # pipelines/<uuid>/metadata.yaml
    environment:
      requirements: requirements.txt   # relative to the pipeline's folder
      python: '3.12'                    # optional; the server's version by default

A Python worker is a named environment a block selects: a uv project in
`python_workers/<alias>/` (pyproject.toml and uv.lock, in Git). A block's configuration
names it (`python_worker: feast`), or the pipeline does for its blocks
(`environment: {worker: feast}`); `python_worker: mage` runs a block in MageML's own
Python. A worker is prepared before runs use it (`mage workers prepare`), with
`uv sync --locked`, so a run never installs packages; a block whose worker is not
prepared fails with what to do.

uv builds the environment once per set of inputs: a hash of the Python version, the
requirements, the versions of the packages that pass tables between Mage and the block
(pandas, pyarrow, Polars and NumPy, pinned to Mage's unless the requirements name them),
the platform and uv's version names its directory. The build goes to a staging
directory, under a file lock, and is renamed into place when complete, so a crashed
build is never used and two processes never build the same environment.

A block runs as the pipeline services' worker runs it (mage_ai/pipeline_services/
runtime/worker.py): its file runs in the environment's interpreter with Mage's
decorators, its upstream outputs and variables come in as Arrow, JSON or pickle files,
its outputs and test results go back the same way and Mage stores them as for any
block. Everything the block prints reaches its logs as it runs.
"""
import functools
import hashlib
import importlib.metadata
import json
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from mage_ai.shared.processes import (
    HAS_PROCESS_GROUPS,
    stop_process_group,
    supervised_command,
)

logger = logging.getLogger(__name__)

ENVIRONMENTS_DIR_ENV = 'MAGE_ENVIRONMENTS_DIR'
MARKER = 'mage-environment.json'
# The packages that pass tables between Mage and the block, pinned to Mage's versions.
EXCHANGE_PACKAGES = ('pandas', 'pyarrow', 'polars', 'numpy')
BUILD_TIMEOUT_SECONDS = int(os.getenv('MAGE_ENVIRONMENT_BUILD_TIMEOUT_SECONDS') or 1800)
SUPPORTED_BLOCK_TYPES = ('data_loader', 'transformer', 'data_exporter', 'custom')
WORKER = Path(__file__).resolve().parents[1] / 'pipeline_services' / 'runtime' / 'worker.py'

_locks: Dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


class PipelineEnvironmentError(Exception):
    pass


@dataclass(frozen=True)
class Environment:
    python: str
    requirements: Tuple[str, ...]
    pins: Tuple[str, ...]

    @property
    def identity(self) -> str:
        payload = json.dumps(dict(
            version=1,
            python=self.python,
            requirements=list(self.requirements),
            pins=list(self.pins),
            platform=f'{sys.platform}-{platform.machine()}',
            uv=_uv_version(),
        ), sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    @property
    def directory(self) -> Path:
        return environments_dir() / self.identity[:24]

    @property
    def python_executable(self) -> Path:
        if sys.platform == 'win32':
            return self.directory / 'Scripts' / 'python.exe'
        return self.directory / 'bin' / 'python'


WORKERS_DIR = 'python_workers'
# What selects MageML's own Python for a block in a pipeline with a worker.
BUILTIN_WORKERS = ('mage', 'builtin')


def normalized_alias(name: str) -> str:
    import re

    return re.sub(r'[-_.]+', '-', str(name).strip()).lower()


@dataclass(frozen=True)
class NamedWorker:
    """A Python worker: a uv project in python_workers/<folder>/, prepared with its lock.
    Its pyproject.toml may name it for people, under [tool.mageml.worker]: label ("Feast
    worker"), description, and combine = false to keep it apart from other workers."""

    alias: str
    definition: str
    python: str
    digest: str
    label: Optional[str] = None
    description: Optional[str] = None
    combine: bool = True
    # The memory its processes may use, in bytes ([tool.mageml.worker] memory = "4 GiB").
    memory_limit: Optional[int] = None

    @property
    def display_name(self) -> str:
        return self.label or self.alias

    @property
    def identity(self) -> str:
        payload = json.dumps(dict(
            version=1,
            kind='worker',
            digest=self.digest,
            python=self.python,
            platform=f'{sys.platform}-{platform.machine()}',
            uv=_uv_version(),
        ), sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    @property
    def directory(self) -> Path:
        return environments_dir() / self.identity[:24]

    @property
    def python_executable(self) -> Path:
        if sys.platform == 'win32':
            return self.directory / 'Scripts' / 'python.exe'
        return self.directory / 'bin' / 'python'

    @property
    def prepared(self) -> bool:
        return (self.directory / MARKER).is_file()

    def relative_definition(self) -> str:
        return f'{WORKERS_DIR}/{Path(self.definition).name}'


def named_workers(repo_path: str) -> Dict[str, NamedWorker]:
    """
    The project's Python workers, by alias: the normalized name of the worker's folder,
    python_workers/<name>/. Its [project].name is free, so a worker named feast can depend
    on feast (_check_project_name). A worker that cannot be read is left out, and only it:
    broken_workers says why.
    """
    return _scan_workers(repo_path)[0]


def broken_workers(repo_path: str) -> Dict[str, Dict[str, str]]:
    """
    The workers named_workers leaves out, by alias, with why: a pyproject.toml that is not
    TOML, a memory setting that is not a size, two folders with one name. One broken
    worker keeps every other one working; a block that selects it says why it cannot run.
    """
    return _scan_workers(repo_path)[1]


def _scan_workers(repo_path: str) -> Tuple[Dict[str, NamedWorker], Dict[str, Dict[str, str]]]:
    import tomllib

    root = Path(repo_path) / WORKERS_DIR
    workers: Dict[str, NamedWorker] = {}
    broken: Dict[str, Dict[str, str]] = {}
    if not root.is_dir():
        return workers, broken
    for folder in sorted(p for p in root.iterdir() if (p / 'pyproject.toml').is_file()):
        relative = f'{WORKERS_DIR}/{folder.name}'
        alias = normalized_alias(folder.name)
        if alias in workers or alias in broken:
            # Neither runs a block: which one was meant is not known.
            first = workers.pop(alias, None)
            other = first.relative_definition() if first else broken[alias]['definition']
            broken[alias] = dict(definition=relative, reason=(
                f'Python workers {other} and {relative} are both named {alias}; folder names '
                'compare without case, "-", "_" or ".". Rename one of the folders.'))
            continue
        try:
            text = (folder / 'pyproject.toml').read_text()
            parsed = tomllib.loads(text)
        except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
            broken[alias] = dict(definition=relative,
                                 reason=f'{relative}/pyproject.toml cannot be read: {error}.')
            continue
        lock = folder / 'uv.lock'
        digest = hashlib.sha256()
        digest.update(environment_bytes((folder / 'pyproject.toml').read_bytes()))
        digest.update(lock.read_bytes() if lock.is_file() else b'')
        version_file = folder / '.python-version'
        project = parsed.get('project') if isinstance(parsed.get('project'), dict) else {}
        python = (version_file.read_text().strip() if version_file.is_file()
                  else _worker_python(project.get('requires-python')))
        tool = parsed.get('tool') if isinstance(parsed.get('tool'), dict) else {}
        mageml = tool.get('mageml') if isinstance(tool.get('mageml'), dict) else {}
        settings = mageml.get('worker') if isinstance(mageml.get('worker'), dict) else {}
        label = settings.get('label')
        description = settings.get('description')
        try:
            memory_limit = parse_bytes(settings.get('memory'))
        except ValueError as error:
            broken[alias] = dict(definition=relative,
                                 reason=f'{relative}/pyproject.toml: memory: {error}')
            continue
        workers[alias] = NamedWorker(
            alias=alias, definition=str(folder), python=python, digest=digest.hexdigest(),
            label=str(label).strip() or None if label else None,
            description=str(description).strip() or None if description else None,
            combine=settings.get('combine', True) is not False,
            memory_limit=memory_limit,
        )
    return workers, broken


_UNITS = dict(b=1, k=10 ** 3, m=10 ** 6, g=10 ** 9, t=10 ** 12,
              ki=2 ** 10, mi=2 ** 20, gi=2 ** 30, ti=2 ** 40)


def parse_bytes(value) -> Optional[int]:
    """'4 GiB', '512MB', '2g' or a number of bytes; None for nothing."""
    if value is None or value == '':
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number, unit = float(value), 'b'
    else:
        match = re.fullmatch(r'\s*([0-9]+(?:\.[0-9]+)?)\s*([kmgt]i?)?b?\s*', str(value).lower())
        if not match:
            raise ValueError(f'"{value}" is not a size, such as 4 GiB or 512 MB.')
        number, unit = float(match.group(1)), match.group(2) or 'b'
    size = int(number * _UNITS[unit])
    if size < 64 * 2 ** 20:
        raise ValueError(f'"{value}" is less than 64 MiB, too little for Python to start.')
    return size


def format_bytes(size: int) -> str:
    for unit, factor in (('GiB', 2 ** 30), ('MiB', 2 ** 20)):
        if size >= factor:
            return f'{size / factor:.1f}'.rstrip('0').rstrip('.') + f' {unit}'
    return f'{size} bytes'


def _worker_python(requires: Optional[str]) -> str:
    """
    The Python a worker without a .python-version runs on: MageML's when its
    requires-python allows it, else the closest one it allows (the lowest above MageML's,
    or the highest below). Without this, uv sync gave a worker with requires-python >= 3.13
    the newest Python it had while the worker said MageML's.
    """
    own = f'{sys.version_info.major}.{sys.version_info.minor}'
    if not requires:
        return own
    from packaging.specifiers import InvalidSpecifier, SpecifierSet

    try:
        allowed = SpecifierSet(requires)
    except InvalidSpecifier:
        return own
    minors = [m for m in range(8, 30)
              if allowed.contains(f'3.{m}.0') or allowed.contains(f'3.{m}.99')]
    if sys.version_info.minor in minors or not minors:
        return own
    above = [m for m in minors if m > sys.version_info.minor]
    return f'3.{min(above) if above else max(minors)}'


def environment_bytes(pyproject: bytes) -> bytes:
    """A worker's pyproject.toml without its [tool.mageml...] tables, which name and
    describe it for people: changing them does not rebuild the environment. Unchanged for
    a file without them."""
    lines = pyproject.splitlines(keepends=True)
    kept, skipping = [], False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(b'['):
            skipping = stripped.startswith(b'[tool.mageml')
            if skipping:
                # The blank lines that set the table apart go with it.
                while kept and not kept[-1].strip():
                    kept.pop()
        if not skipping:
            kept.append(line)
    return b''.join(kept)


def worker_selection(block) -> Optional[str]:
    """
    The worker a Python block selects: its configuration's python_worker, else its
    pipeline's environment worker. 'mage' for MageML's own Python; None when nothing is
    selected.
    """
    selected = (block.configuration or {}).get('python_worker')
    if isinstance(selected, dict):
        selected = 'mage' if selected.get('kind') == 'builtin' else selected.get('alias')
    if not selected:
        config = getattr(getattr(block, 'pipeline', None), 'environment', None)
        if isinstance(config, dict):
            selected = config.get('worker')
    if not selected:
        return None
    alias = normalized_alias(selected)
    if alias in BUILTIN_WORKERS:
        return 'mage'
    repo_path = getattr(block, 'repo_path', None) or os.getcwd()
    return _folder_alias(repo_path, alias)


def _folder_alias(repo_path: str, alias: str) -> str:
    """
    The worker a selection names. Workers were named by their [project].name before their
    folder: a block that selects a worker by its project name selects that worker.
    """
    import tomllib

    root = Path(repo_path) / WORKERS_DIR
    if not root.is_dir():
        return alias
    folders = {
        normalized_alias(p.name): p for p in root.iterdir() if (p / 'pyproject.toml').is_file()
    }
    if alias in folders:
        return alias
    for folder_alias, folder in folders.items():
        try:
            project = tomllib.loads((folder / 'pyproject.toml').read_text()).get('project')
        except (OSError, tomllib.TOMLDecodeError):
            continue
        if normalized_alias((project or {}).get('name') or '') == alias:
            return folder_alias
    return alias


def block_worker(block) -> Optional[NamedWorker]:
    """The prepared worker the block runs in; None for MageML's own Python."""
    alias = worker_selection(block)
    if alias is None or alias == 'mage':
        return None
    repo_path = getattr(block, 'repo_path', None) or os.getcwd()
    workers, broken = _scan_workers(repo_path)
    worker = workers.get(alias)
    if worker is None and alias in broken:
        raise PipelineEnvironmentError(
            f'Block {block.uuid} selects Python worker "{alias}", which cannot be used: '
            f'{broken[alias]["reason"]} Fix it on the Workers page or in the file.'
        )
    if worker is None:
        known = ', '.join(sorted(workers)) or f'none (add one in {WORKERS_DIR}/<name>/)'
        raise PipelineEnvironmentError(
            f'Block {block.uuid} selects Python worker "{alias}", which does not exist. '
            f'Workers in this project: {known}.'
        )
    if not worker.prepared:
        raise PipelineEnvironmentError(
            f'Python worker "{alias}" is not prepared for its current pyproject.toml and '
            f'uv.lock. Run "mage workers prepare {alias}" in the project.'
        )
    return worker


def environments_dir() -> Path:
    configured = os.getenv(ENVIRONMENTS_DIR_ENV)
    if configured:
        return Path(configured)
    cache = os.getenv('XDG_CACHE_HOME') or os.path.join(os.path.expanduser('~'), '.cache')
    return Path(cache) / 'mage' / 'environments'


def uv_env(base: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    The environment to run uv in: without colors, which uv prints with FORCE_COLOR set (as
    Playwright sets it for the server) even without a terminal, and which would reach
    messages as escape codes.
    """
    return dict(os.environ if base is None else base, NO_COLOR='1')


def plain(text: str) -> str:
    """Text a program printed, without the terminal's color escape codes."""
    return _ESCAPES.sub('', text or '')


_ESCAPES = re.compile(r'\x1b\[[0-9;?]*[A-Za-z]')


def _uv() -> str:
    uv = shutil.which('uv')
    if uv is None:
        raise PipelineEnvironmentError(
            'Pipeline environments are built with uv, which is not installed: '
            'https://docs.astral.sh/uv/ (the Mage Docker image has it).'
        )
    return uv


def _uv_version() -> str:
    """uv's version, asked once per uv binary: every worker's identity includes it."""
    try:
        uv = _uv()
        return _uv_version_of(uv, os.stat(uv).st_mtime_ns)
    except (OSError, PipelineEnvironmentError):
        return 'unknown'


@functools.lru_cache(maxsize=8)
def _uv_version_of(uv: str, modified: int) -> str:
    try:
        return subprocess.run(
            [uv, '--version'], capture_output=True, text=True, timeout=30,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return 'unknown'


def _requirement_name(line: str) -> Optional[str]:
    import re

    match = re.match(r'\s*([A-Za-z0-9][A-Za-z0-9._-]*)', line)
    return match.group(1).lower().replace('_', '-') if match else None


def pipeline_environment(pipeline) -> Optional[Environment]:
    """The environment the pipeline declares, or None when its blocks use Mage's."""
    config = getattr(pipeline, 'environment', None)
    if not config:
        return None
    if isinstance(config, dict) and config.get('worker'):
        if config.get('requirements'):
            raise PipelineEnvironmentError(
                f'The environment of pipeline {pipeline.uuid} names both a worker and '
                'requirements; a worker holds its packages in its own pyproject.toml.'
            )
        return None
    if not isinstance(config, dict):
        raise PipelineEnvironmentError(
            f'The environment of pipeline {pipeline.uuid} must be a mapping with '
            'requirements and, optionally, python.'
        )
    requirements = config.get('requirements') or []
    if isinstance(requirements, str):
        path = Path(pipeline.dir_path) / requirements
        if not path.is_file():
            raise PipelineEnvironmentError(
                f'Pipeline {pipeline.uuid} names the requirements file {requirements}, which '
                f'does not exist in {pipeline.dir_path}.'
            )
        requirements = path.read_text().splitlines()
    python = str(config.get('python') or f'{sys.version_info.major}.{sys.version_info.minor}')
    return environment_for(requirements, python)


def environment_for(requirements: List[str], python: Optional[str] = None) -> Environment:
    """An environment with these requirements, the exchange packages at Mage's versions."""
    python = python or f'{sys.version_info.major}.{sys.version_info.minor}'
    lines = []
    for line in requirements:
        line = str(line).split('#', 1)[0].strip()
        if line:
            lines.append(line)
    named = {_requirement_name(line) for line in lines}
    pins = []
    for package in EXCHANGE_PACKAGES:
        if package in named:
            continue
        try:
            pins.append(f'{package}=={importlib.metadata.version(package)}')
        except importlib.metadata.PackageNotFoundError:
            continue
    return Environment(python=python, requirements=tuple(lines), pins=tuple(pins))


def _process_lock(identity: str) -> threading.Lock:
    with _locks_guard:
        return _locks.setdefault(identity, threading.Lock())


def ensure(environment: Environment, log=print) -> Path:
    """The environment's interpreter, built first when it does not exist."""
    if (environment.directory / MARKER).is_file():
        return environment.python_executable
    try:
        import fcntl
    except ImportError:  # Windows: builds are serialized within the process only.
        fcntl = None

    root = environments_dir()
    root.mkdir(parents=True, exist_ok=True)
    with _process_lock(environment.identity):
        with open(root / f'{environment.identity[:24]}.lock', 'w') as lock_file:
            # Another process may be building the same environment; wait for it.
            if fcntl is not None:
                fcntl.flock(lock_file, fcntl.LOCK_EX)
            try:
                if not (environment.directory / MARKER).is_file():
                    if isinstance(environment, NamedWorker):
                        _build_worker(environment, log)
                    else:
                        _build(environment, log)
            finally:
                if fcntl is not None:
                    fcntl.flock(lock_file, fcntl.LOCK_UN)
    return environment.python_executable


def _run(command: List[str], log) -> None:
    _run_with_env(command, log, None)


def _run_with_env(command: List[str], log, env: Optional[Dict[str, str]]) -> None:
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        encoding='utf-8', errors='replace', start_new_session=HAS_PROCESS_GROUPS,
        env=uv_env(env),
    )
    timed_out = threading.Event()

    def stop():
        timed_out.set()
        stop_process_group(process)

    # A timer: uv can wait on the network without printing or closing its output.
    timer = threading.Timer(BUILD_TIMEOUT_SECONDS, stop)
    timer.daemon = True
    timer.start()
    lines = []
    try:
        for line in process.stdout:
            lines.append(line.rstrip())
            log(line.rstrip())
        code = process.wait()
    except BaseException:
        stop_process_group(process)
        raise
    finally:
        timer.cancel()
    if timed_out.is_set():
        raise PipelineEnvironmentError(
            f'Building the environment took longer than {BUILD_TIMEOUT_SECONDS} seconds.'
        )
    if code != 0:
        raise PipelineEnvironmentError(
            f'Building the environment failed ({" ".join(command[:3])}):\n'
            + '\n'.join(lines[-30:])
        )


def _build(environment: Environment, log) -> None:
    uv = _uv()
    final = environment.directory
    staging = final.parent / f'{final.name}.staging-{os.getpid()}'
    shutil.rmtree(staging, ignore_errors=True)
    log(f'Building the pipeline environment {final.name} (Python {environment.python}); '
        'later runs reuse it.')
    try:
        _run([uv, 'venv', '--relocatable', '--python', environment.python, str(staging)], log)
        python = staging / ('Scripts/python.exe' if sys.platform == 'win32' else 'bin/python')
        with tempfile.NamedTemporaryFile('w', suffix='.txt', delete=False) as file:
            file.write('\n'.join(environment.requirements + environment.pins) + '\n')
            requirements_path = file.name
        try:
            # Compiled now, while people wait for the build, not on the first block run
            # (pandas and pyarrow took 12 s there).
            _run([uv, 'pip', 'install', '--compile-bytecode', '--python', str(python), '-r',
                  requirements_path], log)
        finally:
            os.unlink(requirements_path)
        frozen = subprocess.run(
            [uv, 'pip', 'freeze', '--python', str(python)],
            capture_output=True, text=True, timeout=120,
        ).stdout.splitlines()
        (staging / MARKER).write_text(json.dumps(dict(
            identity=environment.identity,
            python=environment.python,
            requirements=list(environment.requirements),
            pins=list(environment.pins),
            packages=frozen,
        ), indent=2) + '\n')
        shutil.rmtree(final, ignore_errors=True)
        os.replace(staging, final)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _check_project_name(worker: NamedWorker) -> None:
    """
    A project named like one of its dependencies (a worker "pandas" that lists pandas): uv
    takes the project for the package, so the lock fails on a version or, unpinned, the
    package is never installed and the blocks fail on import.
    """
    import tomllib

    project = tomllib.loads(
        (Path(worker.definition) / 'pyproject.toml').read_text()).get('project') or {}
    name = (project.get('name') or '').lower().replace('_', '-').replace('.', '-')
    for dependency in project.get('dependencies') or []:
        required = (_requirement_name(dependency) or '').replace('.', '-')
        if name and required == name:
            raise PipelineEnvironmentError(
                f'Python worker "{worker.alias}": its project is named "{project["name"]}", '
                f'like the package it needs, so uv would take the project for the package. '
                f'Rename it in {worker.relative_definition()}/pyproject.toml, such as '
                f'name = "mageml-worker-{worker.alias}", and lock it again.'
            )


def _build_worker(worker: NamedWorker, log) -> None:
    _check_project_name(worker)
    uv = _uv()
    definition = Path(worker.definition)
    relative = worker.relative_definition()
    if not (definition / 'uv.lock').is_file():
        raise PipelineEnvironmentError(
            f'Python worker "{worker.alias}" has no uv.lock. Run "uv lock" in {relative} and '
            'commit the result.'
        )
    final = worker.directory
    staging = final.parent / f'{final.name}.staging-{os.getpid()}'
    shutil.rmtree(staging, ignore_errors=True)
    log(f'Preparing Python worker {worker.alias} (Python {worker.python}) in {final.name}.')
    try:
        _run([uv, 'venv', '--relocatable', '--python', worker.python, str(staging)], log)
        environment = dict(os.environ, UV_PROJECT_ENVIRONMENT=str(staging))
        environment.pop('VIRTUAL_ENV', None)
        try:
            _run_with_env(
                # --compile-bytecode: compiled while people wait for the prepare, not on
                # the first block run (pandas and pyarrow took 12 s there).
                [uv, 'sync', '--locked', '--no-dev', '--no-install-project', '--compile-bytecode',
                 '--project', str(definition)],
                log, environment,
            )
        except PipelineEnvironmentError as error:
            text = str(error)
            if 'lockfile' in text.lower() and 'update' in text.lower():
                raise PipelineEnvironmentError(
                    f'Python worker "{worker.alias}": uv.lock does not match pyproject.toml. '
                    f'Run "uv lock" in {relative} and commit the result.'
                ) from error
            raise PipelineEnvironmentError(
                f'Python worker "{worker.alias}" could not be prepared: {text}'
            ) from error
        _check_python(worker, staging)
        python = staging / ('Scripts/python.exe' if sys.platform == 'win32' else 'bin/python')
        _import_once(python, worker, log)
        frozen = subprocess.run(
            [uv, 'pip', 'freeze', '--python', str(python)],
            capture_output=True, text=True, timeout=120,
        ).stdout.splitlines()
        (staging / MARKER).write_text(json.dumps(dict(
            identity=worker.identity,
            worker=worker.alias,
            definition=relative,
            # The worker's folder, which _remove_superseded matches environments by.
            source=str(definition.resolve()),
            prepared_at=time.time(),
            digest=worker.digest,
            python=worker.python,
            packages=frozen,
        ), indent=2) + '\n')
        # An environment left without its marker (a cleanup stopped halfway) would make the
        # replace fail on a folder that is not empty, every time.
        shutil.rmtree(final, ignore_errors=True)
        os.replace(staging, final)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    _remove_superseded(worker, log)


# Environments of one worker folder kept: the current one and the one before, which a run
# that started before the change may still use.
KEPT_ENVIRONMENTS = 2


def _remove_superseded(worker: NamedWorker, log) -> None:
    """
    Removes the environments of the worker's folder older than the last KEPT_ENVIRONMENTS:
    each change of its lock prepares a new one, and a Feast worker's is about a gigabyte.
    Environments of other folders and projects are not touched.
    """
    source = str(Path(worker.definition).resolve())
    found = []
    for marker in environments_dir().glob(f'*/{MARKER}'):
        try:
            recorded = json.loads(marker.read_text())
        except (OSError, ValueError):
            continue
        if recorded.get('source') == source:
            found.append((recorded.get('prepared_at') or 0, marker.parent))
    for _, directory in sorted(found, reverse=True)[KEPT_ENVIRONMENTS:]:
        if directory == worker.directory:
            continue
        log(f'Removing the superseded environment {directory.name} of {worker.alias}.')
        # The marker first: a removal stopped halfway leaves an environment never used.
        (directory / MARKER).unlink(missing_ok=True)
        shutil.rmtree(directory, ignore_errors=True)


# Imports, in the new environment, the table libraries and the modules of the packages the
# worker lists; prints the ones that fail.
_IMPORT_ONCE = """
import importlib, importlib.metadata as metadata, json, sys
wanted = {name.lower().replace('_', '-') for name in json.loads(sys.argv[1])}
modules = ['pyarrow', 'pandas', 'polars', 'numpy']
for module, distributions in metadata.packages_distributions().items():
    if module.startswith('_') or module in modules:
        continue
    if any(d.lower().replace('_', '-') in wanted for d in distributions):
        modules.append(module)
for module in modules:
    try:
        importlib.import_module(module)
    except ModuleNotFoundError as error:
        if error.name != module:
            print(f'{module}: {error}')
    except Exception as error:
        print(f'{module}: {type(error).__name__}: {error}')
"""


def _import_once(python: Path, worker: NamedWorker, log) -> None:
    """
    Imports the worker's packages once, as the prepare's last step: the first load of
    freshly installed native libraries is slow (11.6 s for pandas and pyarrow on macOS,
    which checks new binaries; 0.2 s after), and it is paid here instead of in the first
    block run. A package that does not import is reported; the prepare still completes,
    as some import only with a GPU or credentials.
    """
    import tomllib

    try:
        project = tomllib.loads(
            (Path(worker.definition) / 'pyproject.toml').read_text()).get('project') or {}
    except (OSError, tomllib.TOMLDecodeError):
        project = {}
    names = [n for n in (_requirement_name(d) for d in project.get('dependencies') or []) if n]
    started = time.monotonic()
    try:
        result = subprocess.run(
            [str(python), '-c', _IMPORT_ONCE, json.dumps(names)],
            capture_output=True, text=True, timeout=600,
        )
    except subprocess.TimeoutExpired:
        log('Importing its packages took over 10 minutes; the first block run will finish it.')
        return
    for line in (result.stdout + result.stderr).strip().splitlines()[-20:]:
        log(f'Could not import {line}')
    log(f'Imported its packages once ({time.monotonic() - started:.1f} s), so the first block '
        'run starts at once.')


def _check_python(worker: NamedWorker, environment: Path) -> None:
    """uv sync replaces an environment whose Python its requires-python does not allow; a
    worker must run on the Python it says (the pages, the combine check and exports use it)."""
    try:
        config = (environment / 'pyvenv.cfg').read_text()
    except OSError:
        return
    found = next((line.split('=', 1)[1].strip() for line in config.splitlines()
                  if line.split('=', 1)[0].strip() in ('version_info', 'version')), None)
    if not found:
        return
    wanted = worker.python.removeprefix('cpython-').removeprefix('python')
    if found.split('.')[:2] != wanted.split('.')[:2]:
        raise PipelineEnvironmentError(
            f'Python worker "{worker.alias}" asks for Python {worker.python}, and its '
            f'requires-python made uv use Python {found}. Make .python-version and '
            f'requires-python in {worker.relative_definition()} agree, and lock it again.'
        )


def prepare(worker: NamedWorker, log=print) -> Path:
    """Builds the worker's environment from its lock when needed; returns its interpreter."""
    return ensure(worker, log)


# Prepare jobs the API started, by (project, alias): their state and every line uv printed.
_prepare_jobs: Dict[Tuple[str, str], Dict] = {}
_prepare_jobs_lock = threading.Lock()


def describe_workers(repo_path: str) -> List[Dict]:
    """Each worker as the Settings page shows it."""
    import tomllib

    described = []
    for alias, worker in named_workers(repo_path).items():
        folder = Path(worker.definition)
        project = tomllib.loads((folder / 'pyproject.toml').read_text()).get('project') or {}
        entry = dict(
            name=alias,
            label=worker.display_name,
            description=worker.description,
            combine=worker.combine,
            definition=worker.relative_definition(),
            python=worker.python,
            dependencies=list(project.get('dependencies') or []),
            locked=(folder / 'uv.lock').is_file(),
            prepared=worker.prepared,
            digest=worker.digest[:12],
            packages=[],
        )
        marker = worker.directory / MARKER
        if marker.is_file():
            entry['packages'] = json.loads(marker.read_text()).get('packages') or []
        job = prepare_job(repo_path, alias)
        if job:
            entry['job'] = job
        described.append(entry)
    return described


# Starting points for a new Python worker: a name for people, what it is for, and the
# packages it starts with. Tables cross between workers as Arrow, so each has pyarrow.
WORKER_TEMPLATES = {
    # No label: each worker from it is shown by its name, not all as "Python worker".
    'empty': dict(
        label=None, description='Your own packages', dependencies=['pyarrow'],
    ),
    'feast': dict(
        label='Feast worker',
        description='Feature retrieval with Feast, which needs pandas 2',
        dependencies=['feast>=0.50', 'pandas>=2.2,<3', 'pyarrow'],
    ),
    'mlflow': dict(
        label='MLflow worker',
        description='Models from an MLflow registry',
        dependencies=['mlflow-skinny>=3', 'pandas', 'pyarrow'],
    ),
    'ml-inference': dict(
        label='ML inference worker',
        description='Scoring with scikit-learn and ONNX Runtime models',
        dependencies=['scikit-learn', 'onnxruntime', 'joblib', 'pandas', 'pyarrow'],
    ),
    'polars': dict(
        label='Polars worker', description='Data work with Polars',
        dependencies=['polars', 'pyarrow'],
    ),
}


def new_worker(
    repo_path: str,
    name: str,
    template: str = 'empty',
    label: Optional[str] = None,
    description: Optional[str] = None,
) -> Path:
    """Writes python_workers/<name>/pyproject.toml from a template; returns its folder. The
    lock and the environment come after (start_prepare with lock=True, or uv lock)."""
    import re

    alias = normalized_alias(name or '')
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]*', alias):
        raise PipelineEnvironmentError(
            f'"{name}" cannot name a Python worker: use letters, digits and "-".'
        )
    if template not in WORKER_TEMPLATES:
        raise PipelineEnvironmentError(
            f'There is no worker template {template}; templates: '
            f'{", ".join(WORKER_TEMPLATES)}.'
        )
    if alias in named_workers(repo_path) or alias in BUILTIN_WORKERS:
        raise PipelineEnvironmentError(f'A Python worker named {alias} exists already.')
    chosen = WORKER_TEMPLATES[template]
    folder = Path(repo_path) / WORKERS_DIR / alias
    folder.mkdir(parents=True, exist_ok=False)
    dependencies = ', '.join(json.dumps(d) for d in chosen['dependencies'])
    (folder / 'pyproject.toml').write_text(
        '[project]\n'
        # Not the alias alone: a worker named "feast" would be a project "feast" that
        # depends on feast (_check_project_name).
        f'name = "mageml-worker-{alias}"\n'
        'version = "0"\n'
        f'requires-python = ">={sys.version_info.major}.{sys.version_info.minor}"\n'
        f'dependencies = [{dependencies}]\n'
        '\n'
        '[tool.mageml.worker]\n'
        + (f'label = {json.dumps(label or chosen["label"])}\n'
           if label or chosen['label'] else '')
        + f'description = {json.dumps(description or chosen["description"])}\n'
    )
    return folder


def assign_worker(pipeline, block_uuids: List[str], alias: Optional[str]) -> List[str]:
    """Sets the Python worker of the pipeline's blocks: a worker's alias, 'mage' for
    MageML's own Python, or None for the pipeline's. Every block is checked before any
    changes. Returns the blocks whose worker changed."""
    if alias not in (None, *BUILTIN_WORKERS):
        workers = named_workers(pipeline.repo_path)
        alias = normalized_alias(alias)
        if alias not in workers:
            raise PipelineEnvironmentError(
                f'There is no Python worker {alias}; workers: {", ".join(workers) or "none"}.'
            )
    elif alias in BUILTIN_WORKERS:
        alias = 'mage'
    blocks = []
    for uuid in block_uuids:
        block = pipeline.get_block(uuid)
        if block is None:
            raise PipelineEnvironmentError(f'Pipeline {pipeline.uuid} has no block {uuid}.')
        language = str(getattr(block.language, 'value', block.language))
        block_type = str(getattr(block.type, 'value', block.type))
        if language != 'python' or block_type not in SUPPORTED_BLOCK_TYPES:
            raise PipelineEnvironmentError(
                f'{uuid} is a {language} {block_type} block; Python workers run Python data '
                'loaders, transformers, data exporters and custom blocks.'
            )
        blocks.append(block)
    changed = []
    for block in blocks:
        configuration = dict(block.configuration or {})
        if configuration.get('python_worker') == alias or \
                (alias is None and 'python_worker' not in configuration):
            continue
        if alias is None:
            configuration.pop('python_worker', None)
        else:
            configuration['python_worker'] = alias
        block.update(dict(configuration=configuration))
        changed.append(block.uuid)
    return changed


# Answers of combine_check by the workers' identities: a resolution takes seconds and the
# answer holds as long as the locks do.
_combine_cache: Dict[Tuple, Dict] = {}
_combine_lock = threading.Lock()


def combine_check(repo_path: str, aliases: List[str]) -> Dict:
    """
    Whether the Python workers can run as one environment (WORKERS_V2.md, merging):

    - same: their packages resolve together at the versions their locks hold;
    - changes: they resolve together if some packages change version (changes, per worker);
    - conflict: they do not (reason, uv's explanation in a line);
    - kept_apart: a worker says combine = false;
    - unchecked: uv could not reach the package index (reason).

    Sizes are what each lock installs on Linux x86-64, and what the workers install together
    when their versions are the same.
    """
    import tempfile
    import tomllib

    import column_atlas_native

    workers = named_workers(repo_path)
    chosen = []
    for alias in dict.fromkeys(normalized_alias(a) for a in aliases):
        worker = workers.get(alias)
        if worker is None:
            raise PipelineEnvironmentError(f'There is no Python worker {alias}.')
        if not (Path(worker.definition) / 'uv.lock').is_file():
            raise PipelineEnvironmentError(
                f'Python worker {alias} has no uv.lock: run uv lock in '
                f'{worker.relative_definition()}.'
            )
        chosen.append(worker)
    if len(chosen) < 2:
        raise PipelineEnvironmentError('Name two Python workers or more to combine.')
    key = tuple(sorted((w.alias, w.digest, w.python) for w in chosen)) + (_uv_version(),)
    with _combine_lock:
        if key in _combine_cache:
            return dict(_combine_cache[key])

    names = {w.alias: w.display_name for w in chosen}
    locks = {w.alias: (Path(w.definition) / 'uv.lock').read_text() for w in chosen}
    sizes = {
        alias: json.loads(column_atlas_native.worker_lock_size(text, 'linux-x86_64',
                                                               chosen[0].python))['bytes']
        for alias, (text) in locks.items()
        for w in [next(x for x in chosen if x.alias == alias)]
    }
    result = dict(workers=[w.alias for w in chosen], names=names, sizes=sizes)
    kept = [w for w in chosen if not w.combine]
    if kept:
        result.update(state='kept_apart', reason=(
            f'{", ".join(w.display_name for w in kept)} '
            f'{"says" if len(kept) == 1 else "say"} combine = false.'
        ))
    elif len({w.python for w in chosen}) > 1:
        result.update(state='conflict', reason='They use different Python versions: ' + ', '.join(
            f'{w.display_name} {w.python}' for w in chosen) + '.')
    else:
        uv = _uv()
        with tempfile.TemporaryDirectory(prefix='mage_combine_') as directory:
            folder = Path(directory)
            # uv runs in the scratch folder: the server's working directory and its
            # pyproject.toml settings do not change the answer.
            def run(command: List[str], timeout: int):
                try:
                    return subprocess.run(command, capture_output=True, cwd=folder, text=True,
                                          env=uv_env(), timeout=timeout)
                except subprocess.TimeoutExpired:
                    # Answered as unchecked (_offline), never raised into an export.
                    return subprocess.CompletedProcess(
                        command, 124, '', f'uv timed out after {timeout} s')

            requirements, constraints, failed = [], [], None
            for worker in chosen:
                project = tomllib.loads(
                    (Path(worker.definition) / 'pyproject.toml').read_text()).get('project') or {}
                requirements += project.get('dependencies') or []
                exported = run(
                    [uv, 'export', '--frozen', '--no-hashes', '--no-emit-project', '--quiet',
                     '--project', worker.definition],
                    timeout=120,
                )
                if exported.returncode != 0:
                    # Without its versions, a check would say "same" for any lock.
                    failed = (worker, exported.stderr)
                    break
                path = folder / f'{worker.alias}.txt'
                path.write_text(exported.stdout)
                constraints += ['-c', str(path)]
            if failed:
                worker, stderr = failed
                lines = [line for line in stderr.strip().splitlines() if line.strip()]
                result.update(state='unchecked', reason=(
                    f'uv could not read the uv.lock of {worker.display_name}: '
                    f'{lines[-1] if lines else "no message"}'))
            else:
                (folder / 'combined.in').write_text('\n'.join(dict.fromkeys(requirements)) + '\n')
                compile_ = [uv, 'pip', 'compile', '--quiet', '--universal', '--python-version',
                            chosen[0].python, str(folder / 'combined.in'), '-o',
                            str(folder / 'combined.txt')]
                pinned = run(compile_ + constraints, timeout=300)
                if pinned.returncode == 0:
                    # The exact versions, which one environment for them all installs
                    # (an exported service builds it, export._merge_extra_workers).
                    result.update(state='same', changes={},
                                  resolved=(folder / 'combined.txt').read_text())
                else:
                    logger.info('Python workers %s do not resolve at their locked versions: %s',
                                ', '.join(w.alias for w in chosen), pinned.stderr.strip())
                    free = run(compile_, timeout=300)
                    if free.returncode == 0:
                        resolved = (folder / 'combined.txt').read_text()
                        changes = {
                            alias: json.loads(
                                column_atlas_native.worker_lock_changes(text, resolved))
                            for alias, text in locks.items()
                        }
                        # No lock's version moves: the versions are the same.
                        result.update(
                            state='changes' if any(changes.values()) else 'same',
                            changes=changes,
                        )
                        if result['state'] == 'same':
                            result['resolved'] = resolved
                    elif _offline(free.stderr):
                        result.update(state='unchecked', reason=(
                            'uv could not reach the package index to check them.'))
                    else:
                        result.update(state='conflict', reason=_resolver_reason(free.stderr))
    if result['state'] == 'same':
        combined = json.loads(column_atlas_native.worker_locks_size(
            list(locks.values()), 'linux-x86_64', chosen[0].python))['bytes']
        result.update(combined_size=combined, saved_bytes=max(0, sum(sizes.values()) - combined))
    if result['state'] != 'unchecked':
        with _combine_lock:
            _combine_cache[key] = dict(result)
    return result


def combine_into(
    pipeline, aliases: List[str], name: str, label: Optional[str] = None,
) -> Tuple[Path, List[str]]:
    """
    Combines Python workers for a pipeline in MageML: a new worker with the union of their
    dependencies (python_workers/<name>/, its label naming them), and the pipeline's blocks
    that selected one of them moved to it. The old workers stay. Lock and prepare it with
    start_prepare(..., lock=True). Returns its folder and the blocks moved.
    """
    import tomllib

    workers = named_workers(pipeline.repo_path)
    chosen = [workers[normalized_alias(a)] for a in aliases if normalized_alias(a) in workers]
    if len(chosen) < 2:
        raise PipelineEnvironmentError('Name two Python workers or more to combine.')
    dependencies = []
    for worker in chosen:
        project = tomllib.loads(
            (Path(worker.definition) / 'pyproject.toml').read_text()).get('project') or {}
        dependencies += project.get('dependencies') or []
    folder = new_worker(
        pipeline.repo_path, name, 'empty',
        label=label or ' + '.join(w.display_name for w in chosen),
        description=f'Combines {", ".join(w.alias for w in chosen)}',
    )
    pyproject = folder / 'pyproject.toml'
    text = pyproject.read_text()
    listed = ', '.join(json.dumps(d) for d in dict.fromkeys(dependencies + ['pyarrow']))
    pyproject.write_text(re.sub(r'dependencies = \[.*?\]', f'dependencies = [{listed}]', text,
                                count=1))
    combined = {w.alias for w in chosen}
    alias = normalized_alias(name)
    # Blocks that name one of them; blocks that inherit the pipeline's worker follow it.
    moving = [
        uuid for uuid, block in pipeline.blocks_by_uuid.items()
        if normalized_alias((block.configuration or {}).get('python_worker') or '') in combined
    ]
    moved = assign_worker(pipeline, moving, alias) if moving else []
    environment = getattr(pipeline, 'environment', None)
    if isinstance(environment, dict) and \
            normalized_alias(environment.get('worker') or '') in combined:
        pipeline.environment = dict(environment, worker=alias)
        pipeline.save()
        moved += [
            uuid for uuid, block in pipeline.blocks_by_uuid.items()
            if uuid not in moved and worker_selection(block) == alias
        ]
    return folder, moved


def _offline(stderr: str) -> bool:
    text = plain(stderr).lower()
    return any(s in text for s in ('failed to fetch', 'error sending request', 'dns error',
                                   'network is unreachable', 'offline', 'timed out'))


def _resolver_reason(stderr: str) -> str:
    """uv's explanation of a conflict, in one line: its first 'Because ...' sentence."""
    lines = [line.strip(' │╰─▶×') for line in plain(stderr).splitlines()]
    text = ' '.join(line for line in lines if line)
    start = text.find('Because')
    reason = text[start:] if start >= 0 else text
    end = reason.find('unsatisfiable')
    return (reason[:end + len('unsatisfiable')] + '.') if end >= 0 else reason[:400]


def start_prepare(repo_path: str, alias: str, lock: bool = False) -> Dict:
    """Prepares the worker in a thread; a prepare of it already running is returned. With
    lock, uv lock writes its uv.lock first (a new worker)."""
    worker = named_workers(repo_path).get(normalized_alias(alias))
    if worker is None:
        raise PipelineEnvironmentError(f'There is no Python worker {alias}.')
    key = (str(Path(repo_path).resolve()), worker.alias)
    with _prepare_jobs_lock:
        running = _prepare_jobs.get(key)
        if running and running['state'] == 'running':
            return dict(running, lines=list(running['lines']))
        job = dict(name=worker.alias, lines=[], problems=[], state='running',
                   started_at=time.time())
        _prepare_jobs[key] = job

    def log(line: str):
        with _prepare_jobs_lock:
            job['lines'].append(line)

    def run():
        try:
            if lock:
                _check_project_name(worker)
                log(f'Locking {worker.relative_definition()} with uv.')
                _run([_uv(), 'lock', '--project', worker.definition], log)
                prepared = named_workers(repo_path)[worker.alias]
            else:
                prepared = worker
            prepare(prepared, log=log)
            state, problems = 'completed', []
        except PipelineEnvironmentError as error:
            state, problems = 'failed', [str(error)]
        except Exception as error:
            state, problems = 'failed', [f'Preparing stopped on an error: {error}']
        with _prepare_jobs_lock:
            job.update(finished_at=time.time(), problems=problems, state=state)

    threading.Thread(target=run, daemon=True, name=f'prepare-{worker.alias}').start()
    with _prepare_jobs_lock:
        return dict(job, lines=list(job['lines']))


def prepare_job(repo_path: str, alias: str) -> Optional[Dict]:
    key = (str(Path(repo_path).resolve()), normalized_alias(alias))
    with _prepare_jobs_lock:
        job = _prepare_jobs.get(key)
        return dict(job, lines=list(job['lines'])) if job else None


def _json_variables(variables: Dict) -> Dict:
    """The variables a block can receive as JSON; other values are left out."""
    from datetime import date, datetime

    safe = {}
    for key, value in (variables or {}).items():
        if isinstance(value, (datetime, date)):
            safe[key] = value.isoformat()
            continue
        try:
            json.dumps(value)
        except (TypeError, ValueError):
            continue
        safe[key] = value
    return safe


def _forward(stream, sink) -> threading.Thread:
    def pump():
        for line in stream:
            sink(line)

    thread = threading.Thread(target=pump, daemon=True)
    thread.start()
    return thread


def dynamic_block_problem(block_uuid: str, environment) -> str:
    """Why a dynamic or replicated block cannot run where it is set to, and what to do."""
    if isinstance(environment, NamedWorker):
        return (f'Block {block_uuid} is dynamic or replicated, which cannot run in Python '
                f'worker "{environment.alias}" yet: choose MageML\'s Python for it in its '
                'settings, or make the block static.')
    return (f'Block {block_uuid} is dynamic or replicated, which cannot run in a pipeline '
            'environment yet; remove the environment from the pipeline settings or make the '
            'block static.')


def _missing_module_hint(environment, message: str, details: str) -> Optional[str]:
    """What to do about a module a worker does not have: the block's own import, or one of
    MageML's modules (a connector from a block template) that needs it."""
    import re

    match = re.search(r"No module named '([^']+)'", message)
    if not match or not isinstance(environment, NamedWorker):
        return None
    package = match.group(1).split('.')[0]
    where = f'{environment.relative_definition()}/pyproject.toml'
    files = re.findall(r'File "([^"]+)", line', details)
    mage_root = Path(__file__).resolve().parents[1]
    importer = next((f for f in reversed(files) if f.endswith('.py')), '')
    if importer and Path(importer).resolve().is_relative_to(mage_root):
        module = Path(importer).resolve().relative_to(mage_root.parent).with_suffix('')
        return (f'MageML\'s {".".join(module.parts)} needs {package}, which Python worker '
                f'"{environment.alias}" does not have: a worker has only its own packages. '
                f'Add {package} to {where} and lock it again, or run this block in MageML\'s '
                'Python.')
    return (f'Python worker "{environment.alias}" does not have {package}: add it to '
            f'{where} and lock it again.')


def _run_in_new_process(python, repo_path: str, request: Dict,
                        memory_limit: Optional[int] = None) -> Dict:
    """Runs one request in a worker process of its own (MAGE_WARM_WORKERS=0)."""
    from mage_ai.data_preparation import warm_workers

    read_fd, write_fd = os.pipe()
    process = subprocess.Popen(
        supervised_command([str(python), '-u', str(WORKER)]),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        errors='replace',
        env=warm_workers.worker_env(repo_path, write_fd),
        cwd=str(repo_path),
        pass_fds=(write_fd,),
        start_new_session=HAS_PROCESS_GROUPS,
    )
    os.close(write_fd)
    pumps = [
        _forward(process.stdout, lambda line: print(line, end='', flush=True)),
        _forward(process.stderr, lambda line: print(line, end='', file=sys.stderr, flush=True)),
    ]
    done, exceeded = threading.Event(), []
    warm_workers.watch_memory(process, memory_limit, done, exceeded)
    try:
        process.stdin.write(json.dumps(request) + '\n')
        process.stdin.write(json.dumps(dict(op='shutdown', id='bye')) + '\n')
        process.stdin.close()
        with os.fdopen(read_fd, encoding='utf-8') as replies:
            messages = [json.loads(line) for line in replies if line.strip()]
        process.wait()
    except BaseException:
        stop_process_group(process)
        raise
    finally:
        done.set()
        for pump in pumps:
            pump.join(timeout=5)
    result = next((m for m in messages if m.get('type') == 'result'), None)
    if result is None:
        if exceeded:
            raise warm_workers.MemoryLimitExceeded(exceeded[0], memory_limit)
        raise warm_workers.WorkerExited(process.returncode)
    return result


def _exit_message(environment, code: int) -> str:
    """Why a worker's process ended before the block's result, said for people."""
    import signal

    who = (f'Python worker "{environment.alias}"' if isinstance(environment, NamedWorker)
           else "The pipeline environment's Python")
    number = None
    if code is not None and code < 0:
        number = -code
    elif code is not None and HAS_PROCESS_GROUPS and 128 < code <= 128 + 64:
        # The supervisor (supervised_command) exits with 128 + the signal that ended it.
        number = code - 128
    if number:
        try:
            name = signal.Signals(number).name
        except ValueError:
            name = f'signal {number}'
        how = f'was stopped by {name} (exit code {code})'
        if number == getattr(signal, 'SIGKILL', 9):
            how += ', which the system sends when memory runs out'
    else:
        how = f'exited with code {code}'
    return (f'{who} {how} before the block returned its result. What the block printed last '
            'is in its log.')


def run_block(
    block, environment, code: Optional[str], input_vars: List, global_vars: Dict,
) -> Tuple[List[Any], List[Dict]]:
    """
    Runs a Python block in the environment (an Environment or a NamedWorker); returns its
    outputs and test results.
    """
    from mage_ai.pipeline_services.runtime import worker

    if block.type not in SUPPORTED_BLOCK_TYPES:
        raise PipelineEnvironmentError(
            f'{block.type} blocks cannot run in a pipeline environment yet; data loaders, '
            'transformers, data exporters and custom blocks can.'
        )
    python = ensure(environment)
    with tempfile.TemporaryDirectory(prefix='mage_environment_') as directory:
        directory = Path(directory)
        if code is not None:
            path = directory / f'{block.uuid}.py'
            path.write_text(code)
        else:
            path = Path(block.file_path)
        inputs = []
        for index, value in enumerate(input_vars or []):
            folder = directory / 'inputs' / str(index)
            folder.mkdir(parents=True)
            inputs.append(worker.write_output(value, str(folder), 0))
        request = dict(
            id='block',
            block=dict(
                uuid=block.uuid,
                type=str(getattr(block.type, 'value', block.type)),
                file=str(path),
                configuration=block.configuration or {},
            ),
            inputs=inputs,
            kwargs=_json_variables(global_vars),
            output_dir=str(directory / 'outputs'),
            run_tests=True,
        )
        # The block run's checkpoints folder, which the worker opens as the block's.
        checkpoint = (global_vars or {}).get('checkpoint')
        if getattr(checkpoint, 'directory', None):
            request['checkpoint_root'] = os.path.dirname(checkpoint.directory)
        repo_path = getattr(block, 'repo_path', None) or os.getcwd()
        from mage_ai.data_preparation import warm_workers

        limit = getattr(environment, 'memory_limit', None)
        try:
            if warm_workers.enabled():
                result = warm_workers.call(str(python), str(repo_path), str(WORKER), request,
                                           memory_limit=limit)
            else:
                result = _run_in_new_process(python, repo_path, request, memory_limit=limit)
        except warm_workers.MemoryLimitExceeded as exceeded:
            raise PipelineEnvironmentError(
                f'Python worker "{environment.alias}" used {format_bytes(exceeded.used)} of '
                f'memory, over its limit of {format_bytes(exceeded.limit)}, and was stopped. '
                f'Raise memory under [tool.mageml.worker] in '
                f'{environment.relative_definition()}/pyproject.toml, or give the block less '
                'data at once.'
            )
        except warm_workers.WorkerExited as exited:
            raise PipelineEnvironmentError(_exit_message(environment, exited.returncode))
        if not result.get('ok'):
            error = result.get('error') or {}
            details = error.get('details') or ''
            message = error.get('message') or 'The block failed.'
            hint = _missing_module_hint(environment, message, details)
            raise PipelineEnvironmentError(
                message + (f' {hint}' if hint else '') + (f'\n\n{details}' if details else '')
            )
        outputs = [worker.read_input(record) for record in result.get('outputs') or []]
        return outputs, list(result.get('tests') or [])
