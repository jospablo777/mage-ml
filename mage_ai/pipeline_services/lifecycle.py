"""
The lifecycle of a service saved in the project (services/<name>/), shared by the
`mage service` commands and the Services page: new, check, build, export.

`build` writes the generated box to .mage/build/<name>/ (git-ignored) and the service's lock,
services/<name>/service.lock (committed). The lock pins what the build used, without
timestamps, so it changes only when something the service uses changed, and its diff tells
a reviewer what moved: the project files, the requirements, a model version, the engine.
"""
import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from mage_ai.pipeline_services import definitions, export

BUILD_DIR = Path('.mage') / 'build'
LOCK_HEADER = (
    '# Written by `mage service build`; commit it. It pins what the build used, so the same\n'
    '# commit and lock build the same service.\n'
)


@dataclass
class CheckResult:
    name: str
    problems: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    report: str = ''
    capture: Optional[export.Capture] = None

    @property
    def ok(self) -> bool:
        return not self.problems


def new(repo_path: str, name: str, pipelines: List[str], description: str = '') -> List[Path]:
    """Creates services/<name>/ with its service.yaml and a dev environment."""
    if name in definitions.load_definitions(repo_path):
        raise definitions.DefinitionError(f'A service named {name} already exists.')
    known = set(_pipeline_uuids(repo_path))
    unknown = [p for p in pipelines if p not in known]
    if unknown:
        raise definitions.DefinitionError(
            f'The project has no pipeline {", ".join(unknown)}; its pipelines are: '
            f'{", ".join(sorted(known)) or "none"}.'
        )
    definitions.save_definition(repo_path, name, dict(pipelines=pipelines,
                                                      description=description))
    return [definitions.service_file(repo_path, name), *definitions.scaffold(repo_path, name)]


def capture(repo_path: str, name: str) -> export.Capture:
    """The service's export as it would be built now (raises ExportError)."""
    from mage_ai.pipeline_services.exports import EXPORT_LIMITS, model_registry

    saved = definitions.load_definition(repo_path, name)
    registry = None if os.environ.get('MLFLOW_TRACKING_URI') else saved['model_registry']
    with model_registry(registry, EXPORT_LIMITS):
        return export.capture(
            repo_path,
            saved['pipelines'],
            name=name,
            max_concurrent_runs=saved['max_runs'],
            models=[f"{m['name']}={m['uri']}" for m in saved['models']],
            includes=saved['include'],
        )


def check(repo_path: str, name: str) -> CheckResult:
    """Everything that stops the service from building, and what to know about it."""
    result = CheckResult(name=name)
    try:
        captured = capture(repo_path, name)
    except definitions.DefinitionError as error:
        result.problems.append(str(error))
        return result
    except export.ExportError as error:
        result.problems.extend(error.problems)
        return result
    result.capture = captured
    result.notes = list(captured.notes)
    result.report = export.report(captured)
    return result


def build_dir(repo_path: str, name: str) -> Path:
    return Path(repo_path) / BUILD_DIR / name


def build(repo_path: str, name: str) -> Path:
    """Writes the box to .mage/build/<name>/ and the lock; returns the box folder."""
    captured = capture(repo_path, name)
    box = export.write(captured, str(build_dir(repo_path, name)), force=True)
    write_lock(repo_path, name, lock(captured, box))
    return box


def export_to(repo_path: str, name: str, out: str, force: bool = False) -> Path:
    """Writes the box to a folder of the user's choice, for a team that builds elsewhere."""
    return export.write(capture(repo_path, name), out, force=force)


def lock(captured: export.Capture, box: Path) -> Dict:
    """What a build used, without timestamps."""
    manifest = json.loads(json.dumps(captured.manifest))
    manifest['service'].pop('exported_at', None)
    requirements = box / 'requirements.txt'
    return dict(
        service=captured.name,
        mage_version=manifest['service'].get('mage_version'),
        manifest_sha256=_sha256(json.dumps(manifest, sort_keys=True).encode()),
        source_sha256=manifest['service'].get('source_sha256'),
        requirements_sha256=(
            _sha256(requirements.read_bytes()) if requirements.is_file() else None
        ),
        engine_sha256=_tree_sha256(export.SERVICE_SOURCE, skip={'target', '.git'}),
        models=[
            {k: m[k] for k in ('name', 'uri', 'version', 'run_id') if m.get(k) is not None}
            for m in manifest.get('models') or []
        ],
    )


def lock_path(repo_path: str, name: str) -> Path:
    return definitions.service_path(repo_path, name) / definitions.LOCK_FILE


def write_lock(repo_path: str, name: str, value: Dict) -> None:
    body = yaml.safe_dump(value, sort_keys=False, default_flow_style=False)
    path = lock_path(repo_path, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(LOCK_HEADER + body)


def read_lock(repo_path: str, name: str) -> Optional[Dict]:
    path = lock_path(repo_path, name)
    if not path.is_file():
        return None
    return yaml.safe_load(path.read_text())


def services_using(repo_path: str, pipeline_uuid: str) -> List[str]:
    """The services that run a pipeline, for the pipeline page."""
    return sorted(
        name for name, value in definitions.load_definitions(repo_path).items()
        if pipeline_uuid in value['pipelines']
    )


def _pipeline_uuids(repo_path: str) -> List[str]:
    root = Path(repo_path) / 'pipelines'
    return [p.parent.name for p in root.glob('*/metadata.yaml')] if root.is_dir() else []


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _tree_sha256(root: Path, skip: set) -> Optional[str]:
    if not root.is_dir():
        return None
    digest = hashlib.sha256()
    for path in sorted(root.rglob('*')):
        relative = path.relative_to(root)
        if path.is_file() and not (set(relative.parts) & skip):
            digest.update(str(relative).encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()


# Project files every service reads.
SHARED_INPUTS = ('io_config.yaml', 'io_config.yml', 'requirements.txt', 'metadata.yaml')


def service_inputs(repo_path: str, name: str, captured: export.Capture) -> List[str]:
    """The project paths (files and folders) whose change can change the service's build."""
    root = Path(repo_path).resolve()

    def relative(path) -> Optional[str]:
        path = Path(path)
        path = (path if path.is_absolute() else root / path).resolve()
        try:
            return path.relative_to(root).as_posix()
        except ValueError:
            return None

    inputs = {relative(definitions.service_path(repo_path, name))}
    for pipeline in captured.manifest.get('pipelines') or []:
        inputs.add(f"pipelines/{pipeline.get('uuid')}")
    for path in set(captured.files) | set(captured.stream_files):
        inputs.add(relative(path))
    if captured.r_project:
        inputs.add(relative(captured.r_project))
    inputs.update(SHARED_INPUTS)
    return sorted(p for p in inputs if p)


def changed_paths(repo_path: str, base: str) -> List[str]:
    """The project paths changed since base (a Git ref), committed or not."""
    import subprocess

    def git(*args) -> str:
        return subprocess.run(
            ['git', *args], cwd=repo_path, capture_output=True, text=True, check=True,
        ).stdout

    top = Path(git('rev-parse', '--show-toplevel').strip()).resolve()
    names = set(git('diff', '--name-only', f'{base}...HEAD').splitlines())
    for line in git('status', '--porcelain').splitlines():
        names.add(line[3:].split(' -> ')[-1].strip('"'))
    root = Path(repo_path).resolve()
    paths = set()
    for name in names:
        if not name:
            continue
        try:
            paths.add((top / name).resolve().relative_to(root).as_posix())
        except ValueError:
            continue  # outside the project
    return sorted(paths)


def affected(repo_path: str, paths: List[str]) -> List[str]:
    """
    The services that the changed project paths touch: their definitions, pipelines, block
    code, shared modules and io_config.yaml. A service that cannot be captured counts, so
    its check says why.
    """
    found = []
    for name in definitions.names(repo_path):
        try:
            inputs = service_inputs(repo_path, name, capture(repo_path, name))
        except (definitions.DefinitionError, export.ExportError):
            found.append(name)
            continue
        if any(
            path == item or path.startswith(item.rstrip('/') + '/')
            for path in paths for item in inputs
        ):
            found.append(name)
    return found
