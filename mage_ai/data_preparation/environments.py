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
    """The project's Python workers, by alias (the normalized [project].name)."""
    import tomllib

    root = Path(repo_path) / WORKERS_DIR
    workers: Dict[str, NamedWorker] = {}
    if not root.is_dir():
        return workers
    for folder in sorted(p for p in root.iterdir() if (p / 'pyproject.toml').is_file()):
        relative = f'{WORKERS_DIR}/{folder.name}'
        try:
            parsed = tomllib.loads((folder / 'pyproject.toml').read_text())
            project = parsed.get('project') or {}
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise PipelineEnvironmentError(
                f'{relative}/pyproject.toml cannot be read: {error}'
            )
        alias = normalized_alias(project.get('name') or folder.name)
        if alias in workers:
            raise PipelineEnvironmentError(
                f'Python workers {workers[alias].relative_definition()} and {relative} are '
                f'both named {alias}; names compare without case, "-", "_" or ".".'
            )
        lock = folder / 'uv.lock'
        digest = hashlib.sha256()
        digest.update(environment_bytes((folder / 'pyproject.toml').read_bytes()))
        digest.update(lock.read_bytes() if lock.is_file() else b'')
        version_file = folder / '.python-version'
        python = (version_file.read_text().strip() if version_file.is_file()
                  else f'{sys.version_info.major}.{sys.version_info.minor}')
        settings = (((parsed.get('tool') or {}).get('mageml') or {}).get('worker') or {})
        label = settings.get('label')
        description = settings.get('description')
        workers[alias] = NamedWorker(
            alias=alias, definition=str(folder), python=python, digest=digest.hexdigest(),
            label=str(label).strip() or None if label else None,
            description=str(description).strip() or None if description else None,
            combine=settings.get('combine', True) is not False,
        )
    return workers


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
    return 'mage' if alias in BUILTIN_WORKERS else alias


def block_worker(block) -> Optional[NamedWorker]:
    """The prepared worker the block runs in; None for MageML's own Python."""
    alias = worker_selection(block)
    if alias is None or alias == 'mage':
        return None
    repo_path = getattr(block, 'repo_path', None) or os.getcwd()
    workers = named_workers(repo_path)
    worker = workers.get(alias)
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
        encoding='utf-8', errors='replace', start_new_session=HAS_PROCESS_GROUPS, env=env,
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
            _run([uv, 'pip', 'install', '--python', str(python), '-r', requirements_path], log)
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
        os.replace(staging, final)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _build_worker(worker: NamedWorker, log) -> None:
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
                [uv, 'sync', '--locked', '--no-dev', '--no-install-project',
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
        python = staging / ('Scripts/python.exe' if sys.platform == 'win32' else 'bin/python')
        frozen = subprocess.run(
            [uv, 'pip', 'freeze', '--python', str(python)],
            capture_output=True, text=True, timeout=120,
        ).stdout.splitlines()
        (staging / MARKER).write_text(json.dumps(dict(
            identity=worker.identity,
            worker=worker.alias,
            definition=relative,
            digest=worker.digest,
            python=worker.python,
            packages=frozen,
        ), indent=2) + '\n')
        os.replace(staging, final)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


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
    'empty': dict(
        label='Python worker', description='Your own packages', dependencies=['pyarrow'],
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
        f'name = "{alias}"\n'
        'version = "0"\n'
        f'requires-python = ">={sys.version_info.major}.{sys.version_info.minor}"\n'
        f'dependencies = [{dependencies}]\n'
        '\n'
        '[tool.mageml.worker]\n'
        f'label = {json.dumps(label or chosen["label"])}\n'
        f'description = {json.dumps(description or chosen["description"])}\n'
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
            run = functools.partial(subprocess.run, capture_output=True, cwd=folder, text=True)
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
                    result.update(state='same', changes={})
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
    text = stderr.lower()
    return any(s in text for s in ('failed to fetch', 'error sending request', 'dns error',
                                   'network is unreachable', 'offline'))


def _resolver_reason(stderr: str) -> str:
    """uv's explanation of a conflict, in one line: its first 'Because ...' sentence."""
    lines = [line.strip(' │╰─▶×') for line in stderr.splitlines()]
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
        read_fd, write_fd = os.pipe()
        repo_path = getattr(block, 'repo_path', None) or os.getcwd()
        env = dict(
            os.environ,
            MAGE_REPO_PATH=str(repo_path),
            MAGE_WORKER_REPLY_FD=str(write_fd),
            MAGE_SUPERVISE_PASS_FDS=str(write_fd),
            PYTHONUNBUFFERED='1',
        )
        # The environment's packages first; Mage's own code stays importable for blocks
        # that use its helpers, as long as the environment has their dependencies.
        mage_root = str(Path(__file__).resolve().parents[2])
        env['PYTHONPATH'] = os.pathsep.join(
            p for p in (env.get('PYTHONPATH'), mage_root) if p
        )
        env.pop('VIRTUAL_ENV', None)
        # A run on the engine's job worker has its protocol descriptor named here; this
        # worker reads its request on stdin.
        env.pop('MAGE_WORKER_REQUEST_FD', None)
        process = subprocess.Popen(
            supervised_command([str(python), '-u', str(WORKER)]),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            env=env,
            cwd=str(repo_path),
            pass_fds=(write_fd,),
            start_new_session=HAS_PROCESS_GROUPS,
        )
        os.close(write_fd)
        pumps = [
            _forward(process.stdout, lambda line: print(line, end='', flush=True)),
            _forward(
                process.stderr, lambda line: print(line, end='', file=sys.stderr, flush=True),
            ),
        ]
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
            for pump in pumps:
                pump.join(timeout=5)
        result = next((m for m in messages if m.get('type') == 'result'), None)
        if result is None:
            raise PipelineEnvironmentError(
                f'The environment\'s Python exited with code {process.returncode} before it '
                'returned the block\'s result.'
            )
        if not result.get('ok'):
            error = result.get('error') or {}
            details = error.get('details') or ''
            raise PipelineEnvironmentError(
                f"{error.get('message') or 'The block failed.'}"
                + (f'\n\n{details}' if details else '')
            )
        outputs = [worker.read_input(record) for record in result.get('outputs') or []]
        return outputs, list(result.get('tests') or [])
