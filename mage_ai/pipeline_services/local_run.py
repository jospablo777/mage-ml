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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from mage_ai.pipeline_services import definitions, engine, export, lifecycle

RUN_DIR = Path('.mage') / 'run'
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
    return yaml.safe_load(path.read_text()) or {}


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
    _build_rust_blocks(box)
    data = folder / 'data'
    data.mkdir(parents=True, exist_ok=True)
    token_file = folder / 'token'
    # For `mage service runs` and `logs`.
    (folder / 'port').write_text(str(port))
    python_dir = box / 'python'
    env = dict(
        os.environ,
        **values,
        MAGE_SERVICE_DIR=str(box),
        MAGE_SERVICE_DATA=str(data),
        MAGE_SERVICE_PORT=str(port),
        MAGE_SERVICE_TOKEN=service_token(token_file),
        MAGE_SERVICE_PYTHON=sys.executable,
        MAGE_SERVICE_WORKER=str(python_dir / 'mage_ai' / 'pipeline_services' / 'runtime'
                                / 'worker.py'),
        PYTHONPATH=os.pathsep.join(
            [str(python_dir)] + ([os.environ['PYTHONPATH']] if os.environ.get('PYTHONPATH')
                                 else []),
        ),
    )
    return LocalRun(
        name=name, box=box, data=data, token_file=token_file, port=port,
        command=[str(binary), 'serve'], env=env, missing=_missing(box, env),
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
