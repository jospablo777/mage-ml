"""
Runs pipelines on the Rust engine of pipeline services (mage_ai/pipeline_services/service),
in place: the engine reads the project where it is, with a manifest written to the
project's variables directory and a link to the project beside it, so nothing is copied or
built into an image.

A pipeline the engine cannot run (sensors, dynamic blocks, callbacks, some SQL providers, Rust
blocks for now) returns a plan with the reasons, and runs on the Python path.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

SERVICE_SOURCE = Path(__file__).resolve().parents[1] / 'pipeline_services' / 'service'
WORKER = Path(__file__).resolve().parents[1] / 'pipeline_services' / 'runtime' / 'worker.py'


class EngineError(Exception):
    """The engine could not start or run, with the reason for the user."""


@dataclass
class EnginePlan:
    pipeline_uuid: str
    directory: Optional[Path] = None  # the engine's directory: service.json and the project link
    reasons: List[str] = field(default_factory=list)  # why the engine cannot run it

    @property
    def supported(self) -> bool:
        return self.directory is not None and not self.reasons


def engine_dir(repo_path: str) -> Path:
    from mage_ai.settings.repo import get_variables_dir

    return Path(get_variables_dir(repo_path=repo_path)) / 'engine'


def fingerprint(repo_path: str, pipeline_uuid: str) -> str:
    """What the engine's manifest depends on: the pipeline's folder, its blocks' files, the
    project's requirements and io_config.yaml, and MageML's version."""
    import hashlib

    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.server.constants import VERSION

    project = Path(repo_path).resolve()
    pipeline = Pipeline.get(pipeline_uuid, repo_path=str(project))
    paths = sorted(p for p in Path(pipeline.dir_path).rglob('*') if p.is_file())
    paths += sorted(Path(b.file_path) for b in pipeline.blocks_by_uuid.values()
                    if getattr(b, 'file_path', None))
    paths += [project / 'requirements.txt', project / 'io_config.yaml']
    digest = hashlib.sha256(VERSION.encode())
    for path in paths:
        digest.update(str(path).encode() + b'\0')
        if path.is_file():
            digest.update(path.read_bytes())
    return digest.hexdigest()


def prepare(repo_path: str, pipeline_uuid: str) -> EnginePlan:
    """
    The engine's manifest for one pipeline, or the reasons it runs on the Python path. The
    result is kept until the pipeline's fingerprint changes: reading a pipeline imports its
    modules to find its packages, which takes seconds.
    """
    from mage_ai.pipeline_services import export

    project = Path(repo_path).resolve()
    directory = engine_dir(str(project)) / pipeline_uuid
    current = fingerprint(str(project), pipeline_uuid)
    stamp = directory / 'fingerprint.json'
    try:
        cached = json.loads(stamp.read_text())
        if cached.get('fingerprint') == current:
            if cached.get('reasons'):
                return EnginePlan(pipeline_uuid, reasons=cached['reasons'])
            if (directory / 'service.json').is_file():
                return EnginePlan(pipeline_uuid, directory=directory)
    except (OSError, ValueError):
        pass

    def remember(reasons: List[str]) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        stamp.write_text(json.dumps(dict(fingerprint=current, reasons=reasons)))

    try:
        captured = export.capture(str(project), [pipeline_uuid], name='engine')
    except export.ExportError as error:
        remember(list(error.problems))
        return EnginePlan(pipeline_uuid, reasons=list(error.problems))
    blocks = [b for p in captured.manifest['pipelines'] for b in p['blocks']]
    if any(b.get('language') == 'rust' for b in blocks):
        reasons = ['Rust blocks run on the Python path until the engine builds them in place.']
        remember(reasons)
        return EnginePlan(pipeline_uuid, reasons=reasons)
    directory.mkdir(parents=True, exist_ok=True)
    manifest = directory / 'service.json'
    temporary = manifest.with_name('.service.json.tmp')
    temporary.write_text(json.dumps(captured.manifest, indent=2, default=str))
    os.replace(temporary, manifest)
    link = directory / project.name
    if link.is_symlink() and link.resolve() != project:
        link.unlink()
    if not link.exists():
        link.symlink_to(project, target_is_directory=True)
    remember([])
    return EnginePlan(pipeline_uuid, directory=directory)


def binary() -> Path:
    """The mage-service executable: MAGE_SERVICE_BINARY, a build in the source tree, or PATH."""
    configured = os.getenv('MAGE_SERVICE_BINARY')
    if configured:
        return Path(configured)
    for profile in ('release', 'debug'):
        built = SERVICE_SOURCE / 'target' / profile / 'mage-service'
        if built.is_file():
            return built
    found = shutil.which('mage-service')
    if found:
        return Path(found)
    raise EngineError(
        'The Rust engine (mage-service) is not built. Build it with '
        f'`cargo build --release --manifest-path {SERVICE_SOURCE / "Cargo.toml"} -p mage-service`'
        ' or set MAGE_SERVICE_BINARY.',
    )


def environment(plan: EnginePlan, data_dir: Path) -> Dict[str, str]:
    return {
        **os.environ,
        'MAGE_SERVICE_DIR': str(plan.directory),
        'MAGE_SERVICE_DATA': str(data_dir),
        'MAGE_SERVICE_WORKER': str(WORKER),
        'MAGE_SERVICE_PYTHON': sys.executable,
        # The project's own modules import as <project>.module, as in MageML.
        'PYTHONPATH': os.pathsep.join(
            p for p in (str(plan.directory), os.getenv('PYTHONPATH', '')) if p
        ),
    }


def run(plan: EnginePlan, variables: Optional[Dict] = None) -> Dict:
    """Runs the pipeline once on the engine and returns the run as JSON (status and blocks)."""
    if not plan.supported:
        raise EngineError('; '.join(plan.reasons) or 'The pipeline is not prepared.')
    data_dir = plan.directory / 'data'
    data_dir.mkdir(exist_ok=True)
    command = [str(binary()), 'run', plan.pipeline_uuid, '--json']
    for key, value in (variables or {}).items():
        command += ['--var', f'{key}={value if isinstance(value, str) else json.dumps(value)}']
    result = subprocess.run(
        command, capture_output=True, text=True, env=environment(plan, data_dir),
    )
    # The engine prints the blocks' logs on stderr and the run as JSON on stdout.
    sys.stderr.write(result.stderr)
    start = result.stdout.find('{')
    if start == -1:
        raise EngineError(f'The engine returned no run (exit {result.returncode}).')
    return json.loads(result.stdout[start:])
