"""
`mage service run`: a service on this machine's engine, without Docker (B2).

The box goes to .mage/run/<name>/box, leaving service.lock alone, so a local run changes no
committed file. Stream positions and run history stay in .mage/run/<name>/data across
restarts. The environment's values come from services/<name>/environments/<env>.yaml
(variables) and services/<name>/.env.<env> (git-ignored, for secrets). Blocks run with the
Python running MageML, from the box's copy of the project.
"""
import json
import os
import secrets
import subprocess
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional

import yaml

from mage_ai.pipeline_services import definitions, engine, export, lifecycle

RUN_DIR = Path('.mage') / 'run'
# Never watched for --reload.
SKIPPED = {'__pycache__', '.git', '.mage', 'node_modules'}
DEFAULT_PORT = 6790


class LocalRunError(RuntimeError):
    pass


@dataclass
class LocalRun:
    name: str
    box: Path
    data: Path
    token_file: Path
    port: int
    command: List[str]
    env: Dict[str, str] = field(default_factory=dict)
    missing: List[str] = field(default_factory=list)
    # The project paths the service reads, for --reload.
    inputs: List[str] = field(default_factory=list)
    # What the run does differently from a deploy (development positions).
    notes: List[str] = field(default_factory=list)


def run_dir(repo_path: str, name: str) -> Path:
    return Path(repo_path) / RUN_DIR / name


def read_env_file(path: Path) -> Dict[str, str]:
    """KEY=value lines; # comments, blank lines and quotes around values are allowed."""
    values = {}
    if not path.is_file():
        return values
    for number, line in enumerate(path.read_text().splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[len('export '):]
        key, separator, value = line.partition('=')
        if not separator or not key.strip():
            raise LocalRunError(f'{path}, line {number}: write KEY=value.')
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in '"\'':
            value = value[1:-1]
        values[key.strip()] = value
    return values


def environment_settings(repo_path: str, name: str, environment: str) -> Dict:
    """services/<name>/environments/<environment>.yaml: target, port, schedules, variables."""
    path = definitions.service_path(repo_path, name) / definitions.ENVIRONMENTS_DIR \
        / f'{environment}.yaml'
    if not path.is_file():
        if environment != 'dev':
            raise LocalRunError(f'Service {name} has no environment {environment} ({path}).')
        return {}
    settings = yaml.safe_load(path.read_text()) or {}
    # The service's platforms, unless the environment names its own.
    if not settings.get('platforms'):
        try:
            platforms = definitions.load_definition(repo_path, name).get('platforms')
        except definitions.DefinitionError:
            platforms = None
        if platforms:
            settings['platforms'] = platforms
    return settings


def environment_values(repo_path: str, name: str, environment: str) -> Dict[str, str]:
    """The engine's environment: the variables, `schedules` and .env.<environment>."""
    settings = environment_settings(repo_path, name, environment)
    values = {}
    for key, value in (settings.get('variables') or {}).items():
        values[str(key)] = value if isinstance(value, str) else json.dumps(value)
    if 'schedules' in settings:
        # YAML reads on and off as true and false.
        schedules = settings['schedules']
        on = schedules is True or str(schedules).lower() in ('on', 'true')
        values['MAGE_SERVICE_SCHEDULES'] = 'on' if on else 'off'
    folder = definitions.service_path(repo_path, name)
    values.update(read_env_file(folder / f'.env.{environment}'))
    return values


def _build_rust_blocks(box: Path) -> None:
    import shutil

    manifest = json.loads((box / 'service.json').read_text())
    binaries = sorted({
        Path(block['binary']).name
        for pipeline in manifest['pipelines'] for block in pipeline['blocks']
        if block.get('binary')
    })
    if not binaries:
        return
    if shutil.which('cargo') is None:
        raise LocalRunError(
            f'The service has Rust blocks ({", ".join(binaries)}), which need cargo to build '
            'for a local run: install Rust from https://rustup.rs, or run the service in Docker '
            'with `mage service build <name> --image`.'
        )
    from mage_ai.data_preparation.models.block.rust import workspace

    target = workspace.target_dir()
    subprocess.run(
        ['cargo', 'build', '--release', '--manifest-path', str(box / 'rust' / 'Cargo.toml'),
         *[a for binary in binaries for a in ('-p', binary)]],
        check=True, env=dict(os.environ, CARGO_TARGET_DIR=str(target)),
    )
    (box / 'bin').mkdir(exist_ok=True)
    for binary in binaries:
        shutil.copy2(target / 'release' / binary, box / 'bin' / binary)


def service_token(token_file: Path, token: Optional[str] = None) -> str:
    """
    The API token, kept in a file readable by its owner only: the one given, else the file's,
    else a new one.
    """
    if not token and token_file.is_file():
        token = token_file.read_text().strip()
        if token:
            return token
    token = token or secrets.token_urlsafe(32)
    if token_file.is_file() and token_file.read_text().strip() == token:
        return token
    token_file.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(token_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, 'w') as file:
        file.write(token)
    return token


def prepare(
    repo_path: str,
    name: str,
    environment: str = 'dev',
    port: int = DEFAULT_PORT,
    engine_binary: Optional[Path] = None,
) -> LocalRun:
    """Builds the box and the engine's environment; raises LocalRunError,
    engine.EngineNotFound, definitions.DefinitionError or export.ExportError."""
    binary = engine_binary or engine.engine_path()
    values = environment_values(repo_path, name, environment)
    folder = run_dir(repo_path, name)
    captured = lifecycle.capture(repo_path, name)
    box = Path(export.write(captured, str(folder / 'box'), force=True))
    # A local run is a development run: positions of its own (SAFETY.md section 3).
    from mage_ai.streaming.development import for_development_manifest

    manifest_path = box / 'service.json'
    manifest = json.loads(manifest_path.read_text())
    notes = for_development_manifest(manifest)
    if notes:
        manifest_path.write_text(json.dumps(manifest, indent=2))
    _build_rust_blocks(box)
    data = folder / 'data'
    data.mkdir(parents=True, exist_ok=True)
    token_file = folder / 'token'
    # For `mage service runs` and `logs`.
    (folder / 'port').write_text(str(port))
    env = engine_environment(box, data, port, service_token(token_file), values)
    _validate(binary, env)
    return LocalRun(
        name=name, box=box, data=data, token_file=token_file, port=port,
        command=[str(binary), 'serve'], env=env, missing=_missing(box, env), notes=notes,
        inputs=lifecycle.service_inputs(repo_path, name, captured)
        + [f'{definitions.SERVICES_DIR}/{name}/.env.{environment}'],
    )


def engine_environment(box: Path, data: Path, port: int, token: str,
                       values: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """The engine's environment for a box on this machine: blocks run with this Python."""
    python_dir = box / 'python'
    return dict(
        os.environ,
        **(values or {}),
        MAGE_SERVICE_DIR=str(box),
        MAGE_SERVICE_DATA=str(data),
        MAGE_SERVICE_PORT=str(port),
        MAGE_SERVICE_TOKEN=token,
        MAGE_SERVICE_PYTHON=sys.executable,
        MAGE_SERVICE_WORKER=str(python_dir / 'mage_ai' / 'pipeline_services' / 'runtime'
                                / 'worker.py'),
        PYTHONPATH=os.pathsep.join(
            [str(python_dir)] + ([os.environ['PYTHONPATH']] if os.environ.get('PYTHONPATH')
                                 else []),
        ),
    )


def _validate(binary: Path, env: Dict[str, str]) -> None:
    """The engine reads the box before it serves, so a stale engine fails here, clearly."""
    try:
        result = subprocess.run([str(binary), 'validate'], env=env, capture_output=True,
                                text=True, timeout=120)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise LocalRunError(f'The engine {binary} did not start: {error}') from None
    if result.returncode != 0:
        output = '\n'.join(
            line for line in (result.stdout + result.stderr).splitlines() if line.strip()
        )
        version = engine.engine_version(binary) or 'unknown version'
        raise LocalRunError(
            f'The engine {binary} ({version}) cannot run this service:\n{output}\n'
            'When the engine is older than this MageML, update it: uv pip install -U '
            'mageml-engine, or in a source checkout cargo build --release -p mage-service.'
        )


def _missing(box: Path, env: Dict[str, str]) -> List[str]:
    """The environment variables the blocks need that are not set (the manifest lists them)."""
    manifest = json.loads((box / 'service.json').read_text())
    return sorted(
        variable['name'] for variable in manifest.get('environment') or []
        if variable.get('required') and not env.get(variable['name'])
        # The engine fetches these at start-up, from the provider the reference names.
        and not variable.get('source')
    )


def supervise(command: List[str], env: Dict[str, str]) -> int:
    """
    Runs the engine until it exits and returns its code. SIGTERM and SIGHUP to this process
    reach the engine, which stops cleanly; without that, a process manager or an editor's
    stop button left the engine serving, with the port and the run history held.
    """
    import signal

    process = subprocess.Popen(command, env=env)

    def forward(signum, frame):
        process.send_signal(signal.SIGTERM)

    previous = {sig: signal.signal(sig, forward) for sig in (signal.SIGTERM, signal.SIGHUP)}
    try:
        while True:
            try:
                return process.wait()
            except KeyboardInterrupt:
                # The terminal sent SIGINT to the engine too; wait for it to stop.
                process.send_signal(signal.SIGTERM)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def snapshot(repo_path: str, inputs: List[str]) -> Dict[str, tuple]:
    """Modification time and size of every file under the inputs."""
    root = Path(repo_path)
    files: Dict[str, tuple] = {}
    for item in inputs:
        path = root / item
        found = [path] if path.is_file() else (path.rglob('*') if path.is_dir() else [])
        for file in found:
            if file.is_file() and not (SKIPPED & set(file.relative_to(root).parts)):
                stat = file.stat()
                files[file.relative_to(root).as_posix()] = (stat.st_mtime_ns, stat.st_size)
    return files


def _within(files: Dict[str, tuple], inputs: List[str]) -> Dict[str, tuple]:
    if '.' in inputs:
        return dict(files)
    return {k: v for k, v in files.items()
            if any(k == item or k.startswith(item.rstrip('/') + '/') for item in inputs)}


def changes(before: Dict[str, tuple], after: Dict[str, tuple]) -> List[str]:
    return sorted(k for k in before.keys() | after.keys() if before.get(k) != after.get(k))


def run_with_reload(repo_path: str, start: Callable[[], 'LocalRun'], echo: Callable[[str], None],
                    interval: float = 1.0, stop: Optional[threading.Event] = None) -> int:
    """
    Runs the service and starts it again, from a new box, whenever a file it reads changes.
    A build that fails waits for the next change. SIGTERM, SIGHUP and Ctrl+C stop it.
    """
    import signal

    stop = stop or threading.Event()
    in_main = threading.current_thread() is threading.main_thread()
    previous = {}
    if in_main:
        previous = {sig: signal.signal(sig, lambda *a: stop.set())
                    for sig in (signal.SIGTERM, signal.SIGHUP)}
    process = None
    try:
        while not stop.is_set():
            # Taken before the build, so an edit saved while it builds starts another.
            before = snapshot(repo_path, ['.'])
            try:
                run = start()
            except Exception as error:  # noqa: BLE001 - shown, then the next change retries
                echo(f'The service does not build: {error}')
                run = None
            # Without a build, any change in the project may fix it.
            inputs = run.inputs if run is not None else ['.']
            if run is not None:
                process = subprocess.Popen(run.command, env=run.env)
            watched = _within(before, inputs)
            while not stop.is_set():
                try:
                    stop.wait(interval)
                except KeyboardInterrupt:
                    stop.set()
                    break
                changed = changes(watched, snapshot(repo_path, inputs))
                if changed:
                    shown = ', '.join(changed[:5]) + (' and more' if len(changed) > 5 else '')
                    echo(f'Changed: {shown}. Starting the service again.')
                    break
                if process is not None and process.poll() is not None:
                    echo(f'The engine stopped with code {process.returncode}; waiting for a '
                         'change.')
                    process = None
            if process is not None:
                process.terminate()
                process.wait()
                process = None
        return 0
    except KeyboardInterrupt:
        return 0
    finally:
        if process is not None:
            process.terminate()
            process.wait()
        for sig, handler in previous.items():
            signal.signal(sig, handler)
