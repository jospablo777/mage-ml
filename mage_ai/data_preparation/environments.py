"""
Pipeline environments: the Python blocks of a pipeline that declares an environment run
in their own virtual environment, with the packages its requirements file lists, instead
of Mage's. Pipelines that need different versions of a library no longer share one set
of packages.

    # pipelines/<uuid>/metadata.yaml
    environment:
      requirements: requirements.txt   # relative to the pipeline's folder
      python: '3.12'                    # optional; the server's version by default

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
import hashlib
import importlib.metadata
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from mage_ai.shared.processes import (
    HAS_PROCESS_GROUPS,
    stop_process_group,
    supervised_command,
)

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
    try:
        return subprocess.run(
            [_uv(), '--version'], capture_output=True, text=True, timeout=30,
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
    python = str(config.get('python') or f'{sys.version_info.major}.{sys.version_info.minor}')
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
                    _build(environment, log)
            finally:
                if fcntl is not None:
                    fcntl.flock(lock_file, fcntl.LOCK_UN)
    return environment.python_executable


def _run(command: List[str], log) -> None:
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        encoding='utf-8', errors='replace',
    )
    lines = []
    for line in process.stdout:
        lines.append(line.rstrip())
        log(line.rstrip())
    try:
        code = process.wait(timeout=BUILD_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
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
    block, environment: Environment, code: Optional[str], input_vars: List, global_vars: Dict,
) -> Tuple[List[Any], List[Dict]]:
    """Runs a Python block in the environment; returns its outputs and test results."""
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
