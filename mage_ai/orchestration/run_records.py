"""
Run records: what each pipeline run ran with, so two runs can be compared and a run can be
reproduced.

When a pipeline run starts, the scheduler records a manifest:

- code: the SHA-256 of every source file the run can read: the pipeline's folder, its
  block files, and the project's shared code and settings (Python, SQL, R and Rust files,
  YAML, TOML, lock files). The files themselves are saved once per distinct set, so a
  run's exact code can be restored later.
- environment: Python, Mage and every installed package with its version, the platform,
  and the pipeline environment (data_preparation/environments.py) when it has one.
- variables: a digest and the names of the run's variables (the values stay in the
  database, with the run).
- git: the commit, branch and whether the project had uncommitted changes.

Each completed block run records the SHA-256 and size of its stored outputs
(block_run.metrics['outputs']).

Records live with the pipeline variables:

    <variables dir>/.run_records/runs/<pipeline uuid>/<run id>/<n>.json   manifests
    <variables dir>/.run_records/code/<digest>.json                      file contents
    <variables dir>/.run_records/environments/<digest>.json              packages

A run started again (a retry of the whole run) gets a new manifest, numbered after the
first, so a retry that ran other code shows it.
"""
import base64
import hashlib
import importlib.metadata
import json
import os
import platform
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

RECORDS_FOLDER = '.run_records'
METRIC = 'run_record'
MANIFEST_VERSION = 1
CODE_SUFFIXES = {
    '.py', '.sql', '.r', '.rs', '.yaml', '.yml', '.toml', '.lock', '.txt', '.cfg', '.ini',
    '.jinja', '.json', '.md', '.ipynb',
}
SKIPPED_FOLDERS = {
    '__pycache__', 'node_modules', 'venv', 'target', 'mage_data', 'site-packages', 'dist',
    'build', 'pipelines',
}
MAX_FILE_BYTES = 1024 * 1024
MAX_FILES = 5000
MAX_TOTAL_BYTES = 64 * 1024 * 1024
# Files of a stored output Mage derives from its data; the data files identify it.
DERIVED_OUTPUT_FILES = {
    'sample_data.parquet', 'sample_data.json', 'statistics.json', 'insights.json',
    'suggestions.json', 'metadata.json', 'resource_usage.json',
}
_ENVIRONMENT_TTL_SECONDS = 60
# Variables Mage sets on every run to place its outputs; never the run's inputs.
STORAGE_VARIABLES = {'execution_partition'}

_environment_cache: Dict[str, Any] = {}
_environment_lock = threading.Lock()


class RunRecordError(Exception):
    pass


def enabled() -> bool:
    return os.getenv('MAGE_RUN_RECORDS', '1').lower() not in ('0', 'false', 'off')


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str).encode()


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _file_digest(path: str) -> str:
    with open(path, 'rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()


# ---- code -------------------------------------------------------------------------


def _pipeline_files(pipeline) -> Iterable[str]:
    for root, folders, files in os.walk(pipeline.dir_path):
        folders[:] = [f for f in folders if not f.startswith('.') and f != '__pycache__']
        for name in files:
            yield os.path.join(root, name)
    blocks = list((pipeline.blocks_by_uuid or {}).values())
    for attribute in ('callbacks_by_uuid', 'conditionals_by_uuid', 'widgets_by_uuid'):
        blocks += list((getattr(pipeline, attribute, None) or {}).values())
    for block in blocks:
        path = getattr(block, 'file_path', None)
        if path and os.path.isfile(path):
            yield path


def _project_files(repo_path: str) -> Iterable[str]:
    for root, folders, files in os.walk(repo_path):
        folders[:] = [
            f for f in folders if not f.startswith('.') and f not in SKIPPED_FOLDERS
        ]
        for name in files:
            if os.path.splitext(name)[1].lower() in CODE_SUFFIXES:
                yield os.path.join(root, name)


def source_files(pipeline) -> Tuple[Dict[str, str], bool]:
    """{path relative to the project: absolute path}, and whether the caps cut it short."""
    repo_path = os.path.realpath(pipeline.repo_path)
    found: Dict[str, str] = {}
    total = 0
    truncated = False
    for path in list(_pipeline_files(pipeline)) + list(_project_files(repo_path)):
        real = os.path.realpath(path)
        if os.path.commonpath([repo_path, real]) != repo_path:
            continue
        relative = os.path.relpath(real, repo_path)
        if relative in found:
            continue
        try:
            size = os.path.getsize(real)
        except OSError:
            continue
        if size > MAX_FILE_BYTES or len(found) >= MAX_FILES or total + size > MAX_TOTAL_BYTES:
            truncated = True
            continue
        found[relative] = real
        total += size
    return dict(sorted(found.items())), truncated


def _snapshot(files: Dict[str, str]) -> Dict[str, Dict[str, str]]:
    snapshot = {}
    for relative, path in files.items():
        with open(path, 'rb') as file:
            content = file.read()
        try:
            snapshot[relative] = dict(encoding='utf-8', content=content.decode('utf-8'))
        except UnicodeDecodeError:
            snapshot[relative] = dict(
                encoding='base64', content=base64.b64encode(content).decode(),
            )
    return snapshot


def _content(entry: Dict[str, str]) -> bytes:
    if entry.get('encoding') == 'base64':
        return base64.b64decode(entry['content'])
    return entry['content'].encode('utf-8')


# ---- environment ------------------------------------------------------------------


def _packages() -> Dict[str, str]:
    packages = {}
    for distribution in importlib.metadata.distributions():
        name = (distribution.metadata['Name'] or '').lower().replace('_', '-')
        if name:
            packages[name] = distribution.version
    return dict(sorted(packages.items()))


def _mage_version() -> str:
    for name in ('mage-ml', 'mage-ai'):
        try:
            return importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            continue
    return 'unknown'


def environment(pipeline=None) -> Dict[str, Any]:
    """The interpreter, packages and platform this process runs blocks with."""
    with _environment_lock:
        cached = _environment_cache.get('value')
        if cached is None or time.monotonic() - _environment_cache['at'] > \
                _ENVIRONMENT_TTL_SECONDS:
            cached = dict(
                python=platform.python_version(),
                implementation=platform.python_implementation(),
                platform=f'{sys.platform}-{platform.machine()}',
                mage=_mage_version(),
                packages=_packages(),
            )
            _environment_cache.update(value=cached, at=time.monotonic())
    document = dict(cached)
    if pipeline is not None and getattr(pipeline, 'environment', None):
        document['pipeline_environment'] = _pipeline_environment(pipeline)
    return document


def _pipeline_environment(pipeline) -> Dict[str, Any]:
    from mage_ai.data_preparation import environments

    try:
        env = environments.pipeline_environment(pipeline)
    except environments.PipelineEnvironmentError as error:
        return dict(error=str(error))
    if env is None:
        return {}
    described = dict(
        identity=env.identity,
        python=env.python,
        requirements=list(env.requirements),
        pins=list(env.pins),
    )
    marker = env.directory / environments.MARKER
    if marker.is_file():
        described['packages'] = json.loads(marker.read_text()).get('packages') or []
    return described


# ---- git --------------------------------------------------------------------------


def _git(repo_path: str) -> Optional[Dict[str, Any]]:
    def run(*args: str) -> Optional[str]:
        try:
            result = subprocess.run(
                ['git', '-C', repo_path, *args], capture_output=True, text=True, timeout=5,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        return result.stdout.strip() if result.returncode == 0 else None

    commit = run('rev-parse', 'HEAD')
    if not commit:
        return None
    status = run('status', '--porcelain', '--untracked-files=no', '--', '.')
    return dict(
        commit=commit,
        branch=run('rev-parse', '--abbrev-ref', 'HEAD'),
        dirty=bool(status),
    )


# ---- storage ----------------------------------------------------------------------


def _root(pipeline) -> str:
    return os.path.join(pipeline.variable_manager.variables_dir, RECORDS_FOLDER)


def _storage(pipeline):
    return pipeline.variable_manager.storage


def _manifest_dir(pipeline, run_id: int) -> str:
    return os.path.join(_root(pipeline), 'runs', pipeline.uuid, str(run_id))


def _write_once(pipeline, path: str, data: Any) -> None:
    storage = _storage(pipeline)
    if storage.path_exists(path):
        return
    storage.makedirs(os.path.dirname(path), exist_ok=True)
    storage.write_json_file(path, data)


def capture(pipeline, pipeline_run) -> Dict[str, Any]:
    """Records the manifest of a run as it starts; returns it."""
    files, truncated = source_files(pipeline)
    code = {relative: _file_digest(path) for relative, path in files.items()}
    code_digest = _digest(code)
    env = environment(pipeline)
    env_digest = _digest(env)
    root = _root(pipeline)
    if not truncated:
        _write_once(pipeline, os.path.join(root, 'code', f'{code_digest}.json'), _snapshot(files))
    _write_once(pipeline, os.path.join(root, 'environments', f'{env_digest}.json'), env)

    variables = run_variables(pipeline_run)
    manifest = dict(
        version=MANIFEST_VERSION,
        pipeline_uuid=pipeline.uuid,
        pipeline_run_id=pipeline_run.id,
        execution_date=(
            pipeline_run.execution_date.isoformat() if pipeline_run.execution_date else None
        ),
        captured_at=datetime.now(timezone.utc).isoformat(timespec='seconds'),
        code=dict(digest=code_digest, files=code, truncated=truncated),
        environment=dict(
            digest=env_digest,
            python=env['python'],
            mage=env['mage'],
            platform=env['platform'],
            pipeline_environment=(env.get('pipeline_environment') or {}).get('identity'),
        ),
        variables=dict(digest=_digest(variables), names=sorted(variables)),
        git=_git(pipeline.repo_path),
    )
    storage = _storage(pipeline)
    directory = _manifest_dir(pipeline, pipeline_run.id)
    storage.makedirs(directory, exist_ok=True)
    number = len([n for n in storage.listdir(directory) if n.endswith('.json')]) + 1
    storage.write_json_file(os.path.join(directory, f'{number}.json'), manifest)
    pipeline_run.update(metrics=dict(
        pipeline_run.metrics or {},
        **{METRIC: dict(manifest=number, code=code_digest, environment=env_digest)},
    ))
    return manifest


def manifests(pipeline, run_id: int) -> List[Dict[str, Any]]:
    storage = _storage(pipeline)
    directory = _manifest_dir(pipeline, run_id)
    if not storage.path_exists(directory):
        return []
    numbers = sorted(
        int(name[:-5]) for name in storage.listdir(directory)
        if name.endswith('.json') and name[:-5].isdigit()
    )
    return [storage.read_json_file(os.path.join(directory, f'{n}.json')) for n in numbers]


def manifest(pipeline, run_id: int) -> Dict[str, Any]:
    """The latest manifest of the run."""
    found = manifests(pipeline, run_id)
    if not found:
        raise RunRecordError(
            f'Pipeline run {run_id} has no run record; runs started before run records '
            'existed have none.'
        )
    return found[-1]


def code_snapshot(pipeline, digest: str) -> Dict[str, Dict[str, str]]:
    path = os.path.join(_root(pipeline), 'code', f'{digest}.json')
    if not _storage(pipeline).path_exists(path):
        raise RunRecordError(
            'The code of this run was not saved (the project had more files than run '
            'records keep, or the record was deleted).'
        )
    return _storage(pipeline).read_json_file(path)


def environment_document(pipeline, digest: str) -> Dict[str, Any]:
    path = os.path.join(_root(pipeline), 'environments', f'{digest}.json')
    if not _storage(pipeline).path_exists(path):
        return {}
    return _storage(pipeline).read_json_file(path)


# ---- outputs ----------------------------------------------------------------------


def output_digests(pipeline, block_uuid: str, partition: Optional[str]) -> Dict[str, Dict]:
    """SHA-256 and bytes of each stored output of a block run, from its data files."""
    from mage_ai.data_preparation.storage.local_storage import LocalStorage

    manager = pipeline.variable_manager
    if not isinstance(manager.storage, LocalStorage):
        return {}
    digests = {}
    for name in manager.get_variables_by_block(
        pipeline.uuid, block_uuid, partition=partition, output_variable_only=True,
    ):
        variable = manager.get_variable_object(
            pipeline.uuid, block_uuid, name, partition=partition,
        )
        folder = variable.variable_path
        if not os.path.isdir(folder):
            continue
        hasher = hashlib.sha256()
        size = 0
        for root, folders, files in os.walk(folder):
            folders.sort()
            for file_name in sorted(files):
                if file_name in DERIVED_OUTPUT_FILES or file_name.startswith('.'):
                    continue
                path = os.path.join(root, file_name)
                hasher.update(os.path.relpath(path, folder).encode() + b'\0')
                hasher.update(_file_digest(path).encode())
                size += os.path.getsize(path)
        digests[name] = dict(sha256=hasher.hexdigest(), bytes=size)
    return digests


# ---- comparing runs ---------------------------------------------------------------


def _changes(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, List]:
    return dict(
        added=sorted(set(after) - set(before)),
        removed=sorted(set(before) - set(after)),
        changed=sorted(k for k in set(before) & set(after) if before[k] != after[k]),
    )


def _unified(pipeline, first: Dict, second: Dict, paths: List[str], limit: int) -> Dict:
    import difflib

    try:
        snapshots = [code_snapshot(pipeline, m['code']['digest']) for m in (first, second)]
    except RunRecordError:
        return {}
    diffs = {}
    for path in paths[:limit]:
        texts = []
        for snapshot in snapshots:
            entry = snapshot.get(path)
            if entry is None or entry.get('encoding') != 'utf-8':
                texts.append(None)
            else:
                texts.append(entry['content'].splitlines(keepends=True))
        if None in texts:
            continue
        diffs[path] = ''.join(difflib.unified_diff(
            texts[0], texts[1], fromfile=f'a/{path}', tofile=f'b/{path}', n=2,
        ))[:20000]
    return diffs


def compare(pipeline, first_run, second_run, diff_limit: int = 20) -> Dict[str, Any]:
    """What differs between two runs of the pipeline: code, packages, variables, outputs."""
    first = manifest(pipeline, first_run.id)
    second = manifest(pipeline, second_run.id)
    code = _changes(first['code']['files'], second['code']['files'])
    code['diffs'] = _unified(pipeline, first, second, code['changed'], diff_limit)

    environments = [
        environment_document(pipeline, m['environment']['digest']) for m in (first, second)
    ]
    packages = _changes(*(e.get('packages') or {} for e in environments))
    package_versions = {
        name: [environments[0]['packages'][name], environments[1]['packages'][name]]
        for name in packages['changed']
    }
    runtime = {
        key: [first['environment'].get(key), second['environment'].get(key)]
        for key in ('python', 'mage', 'platform', 'pipeline_environment')
        if first['environment'].get(key) != second['environment'].get(key)
    }

    variables = _changes(run_variables(first_run), run_variables(second_run))

    outputs = []
    block_runs = [
        {b.block_uuid: b for b in run.block_runs} for run in (first_run, second_run)
    ]
    for uuid in sorted(set(block_runs[0]) | set(block_runs[1])):
        pair = [runs.get(uuid) for runs in block_runs]
        digests = [((b.metrics or {}).get('outputs') if b else None) for b in pair]
        statuses = [str(b.status) if b else None for b in pair]
        if None in digests:
            result = 'not recorded'
        elif digests[0] == digests[1]:
            result = 'same'
        else:
            result = 'differs'
        entry = dict(block_uuid=uuid, result=result, statuses=statuses)
        tracked = [((b.metrics or {}).get('mlflow') if b else None) for b in pair]
        if any(tracked):
            from mage_ai.orchestration.experiments import metric_changes

            entry['metrics'] = metric_changes(*tracked)
        outputs.append(entry)

    return dict(
        runs=[first_run.id, second_run.id],
        code=code,
        same_code=first['code']['digest'] == second['code']['digest'],
        environment=dict(
            runtime=runtime,
            added=packages['added'],
            removed=packages['removed'],
            changed=package_versions,
        ),
        same_environment=first['environment']['digest'] == second['environment']['digest'],
        variables=variables,
        outputs=outputs,
        git=[first.get('git'), second.get('git')],
    )


def format_comparison(comparison: Dict[str, Any]) -> str:
    first, second = comparison['runs']
    lines = [f'Pipeline runs {first} and {second}:']
    code = comparison['code']
    if comparison['same_code']:
        lines.append('- Code: the same.')
    else:
        for kind in ('changed', 'added', 'removed'):
            if code[kind]:
                lines.append(f'- Code {kind}: {", ".join(code[kind])}')
    env = comparison['environment']
    if comparison['same_environment']:
        lines.append('- Environment: the same.')
    else:
        for key, (before, after) in env['runtime'].items():
            lines.append(f'- {key}: {before} -> {after}')
        for name, (before, after) in env['changed'].items():
            lines.append(f'- Package {name}: {before} -> {after}')
        if env['added']:
            lines.append(f'- Packages added: {", ".join(env["added"])}')
        if env['removed']:
            lines.append(f'- Packages removed: {", ".join(env["removed"])}')
    variables = comparison['variables']
    changed = variables['changed'] + variables['added'] + variables['removed']
    lines.append(
        f'- Variables changed: {", ".join(changed)}' if changed else '- Variables: the same.'
    )
    for output in comparison['outputs']:
        lines.append(f'- Output of {output["block_uuid"]}: {output["result"]}')
        for metric, (before, after) in (output.get('metrics') or {}).items():
            lines.append(f'  - MLflow metric {metric}: {before} -> {after}')
    for diff in code.get('diffs', {}).values():
        lines.append('')
        lines.append(diff.rstrip())
    return '\n'.join(lines)


# ---- restoring code ---------------------------------------------------------------


def restore_code(pipeline, run_id: int, destination: str) -> Dict[str, Any]:
    """
    Writes the code a run ran with into destination (a folder named like the project);
    returns the run's manifest.
    """
    recorded = manifest(pipeline, run_id)
    if recorded['code'].get('truncated'):
        raise RunRecordError(
            f'Pipeline run {run_id} read more files than run records keep '
            f'({MAX_FILES} files, {MAX_FILE_BYTES // 1024} KiB each), so its code was not '
            'saved.'
        )
    snapshot = code_snapshot(pipeline, recorded['code']['digest'])
    root = os.path.realpath(destination)
    for relative, entry in snapshot.items():
        path = os.path.realpath(os.path.join(root, relative))
        if os.path.commonpath([root, path]) != root:
            raise RunRecordError(f'The saved code has a path outside the project: {relative}')
        content = _content(entry)
        if hashlib.sha256(content).hexdigest() != recorded['code']['files'].get(relative):
            raise RunRecordError(f'The saved copy of {relative} does not match its digest.')
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as file:
            file.write(content)
    return recorded


# ---- reproducing a run ------------------------------------------------------------


def reproduction_trigger(pipeline, run_id: int):
    """An inactive trigger for the reproductions of a run: schedulers start none of it."""
    from mage_ai.data_preparation.models.triggers import ScheduleStatus, ScheduleType
    from mage_ai.orchestration.db.models.schedules import PipelineSchedule

    name = f'Reproduce run {run_id}'
    schedule = PipelineSchedule.repo_query.filter(
        PipelineSchedule.name == name,
        PipelineSchedule.pipeline_uuid == pipeline.uuid,
    ).first()
    if schedule is None:
        schedule = PipelineSchedule.create(
            name=name,
            pipeline_uuid=pipeline.uuid,
            repo_path=pipeline.repo_path,
            schedule_type=ScheduleType.API,
            status=ScheduleStatus.INACTIVE,
        )
    return schedule


def run_reproduction(pipeline, original, log=print):
    """
    Runs the pipeline (loaded from the restored code) in this process with the original
    run's variables and execution date; returns the new pipeline run.
    """
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.orchestration.fusion_verify import run_in_process
    from mage_ai.orchestration.pipeline_scheduler import (
        configure_pipeline_run_payload,
    )

    payload, _ = configure_pipeline_run_payload(
        reproduction_trigger(pipeline, original.id),
        pipeline.type,
        # Without the original's execution partition: the run stores its own outputs.
        dict(execution_date=original.execution_date, variables=run_variables(original)),
    )
    run = PipelineRun.create(**payload)
    log(f'Reproducing pipeline run {original.id} as pipeline run {run.id}.')
    return run_in_process(run, log=log)


def run_variables(pipeline_run) -> Dict[str, Any]:
    """The run's variables without the ones Mage sets to place its outputs."""
    return {
        key: value for key, value in (pipeline_run.variables or {}).items()
        if key not in STORAGE_VARIABLES
    }


def _absolute_database_url(url: str) -> str:
    """A SQLite URL with a relative path, made absolute for a process in another folder."""
    prefix = 'sqlite:///'
    if url and url.startswith(prefix) and not url.startswith(prefix + '/'):
        return prefix + os.path.abspath(url[len(prefix):])
    return url


def _variables_base(pipeline) -> str:
    """MAGE_DATA_DIR for a copy of the project to share this project's variables."""
    variables_dir = os.path.realpath(pipeline.variable_manager.variables_dir)
    repo_name = os.path.basename(os.path.realpath(pipeline.repo_path))
    if os.path.basename(variables_dir) != repo_name or variables_dir.startswith(('s3', 'gs')):
        raise RunRecordError(
            'Reproducing a run needs the variables in a local folder named like the project '
            f'({variables_dir}).'
        )
    return os.path.dirname(variables_dir)


def reproduce(pipeline, original, log=print, keep: bool = False) -> Dict[str, Any]:
    """
    Restores the code the run ran with into a copy of the project, runs the pipeline there
    with the run's variables and execution date (in its own process, against this project's
    database and variables), and compares each block's outputs with the original run's.
    """
    import shutil
    import tempfile

    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.orchestration.db import db_connection_url
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.orchestration.fusion_verify import compare_runs

    recorded = manifest(pipeline, original.id)
    base = _variables_base(pipeline)
    current = environment(pipeline)
    recorded_environment = environment_document(pipeline, recorded['environment']['digest'])
    environment_changes = _changes(
        recorded_environment.get('packages') or {}, current.get('packages') or {},
    )
    runtime_changes = {
        key: [recorded['environment'].get(key), current.get(key)]
        for key in ('python', 'mage', 'platform')
        if recorded['environment'].get(key) != current.get(key)
    }

    workspace = tempfile.mkdtemp(prefix='mage_reproduce_')
    project = os.path.join(workspace, os.path.basename(os.path.realpath(pipeline.repo_path)))
    try:
        restore_code(pipeline, original.id, project)
        report_path = os.path.join(workspace, 'reproduction.json')
        command = [
            sys.executable, '-m', 'mage_ai.cli.main', 'reproduce-run',
            project, pipeline.uuid, str(original.id), '--report', report_path,
        ]
        env = {
            **os.environ,
            'MAGE_DATA_DIR': base,
            'MAGE_DATABASE_CONNECTION_URL': _absolute_database_url(db_connection_url),
            'PYTHONUNBUFFERED': '1',
        }
        env.pop('MAGE_REPO_PATH', None)
        # The same working folder, so relative paths (a SQLite file) mean the same; the
        # restored project comes first on the child's import path.
        process = subprocess.Popen(
            command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding='utf-8', errors='replace',
        )
        for line in process.stdout:
            log(line.rstrip())
        code = process.wait()
        if not os.path.exists(report_path):
            raise RunRecordError(f'The reproduction stopped (exit code {code}).')
        with open(report_path, encoding='utf-8') as file:
            reproduction = json.load(file)
        if reproduction.get('error'):
            raise RunRecordError(reproduction['error'])

        reproduced = PipelineRun.get(reproduction['pipeline_run_id'])
        restored_pipeline = Pipeline.get(pipeline.uuid, repo_path=project)
        blocks = compare_runs(
            restored_pipeline, original, reproduced, labels=('originally', 'reproduced'),
        )
        reproduced_record = (reproduced.metrics or {}).get(METRIC) or {}
    finally:
        if not keep:
            shutil.rmtree(workspace, ignore_errors=True)
    return dict(
        original_run_id=original.id,
        reproduced_run_id=reproduced.id,
        status=str(reproduced.status),
        code_restored=reproduced_record.get('code') == recorded['code']['digest'],
        git=recorded.get('git'),
        environment=dict(runtime=runtime_changes, **environment_changes),
        blocks=[dict(
            block_uuid=b.block_uuid, result=b.result, detail=b.detail,
        ) for b in blocks],
        passed=str(reproduced.status) == 'completed' and all(
            b.result in ('same', 'close') for b in blocks
        ),
        workspace=workspace if keep else None,
    )


def format_reproduction(report: Dict[str, Any]) -> str:
    lines = [
        f'Pipeline run {report["original_run_id"]} reproduced as pipeline run '
        f'{report["reproduced_run_id"]} ({report["status"]}).'
    ]
    if not report['code_restored']:
        lines.append('- The restored code does not have the recorded digest.')
    env = report['environment']
    for key, (before, after) in env['runtime'].items():
        lines.append(f'- {key} was {before}, is {after}.')
    if env['changed'] or env['added'] or env['removed']:
        lines.append(
            f'- Packages differ from the original run: {len(env["changed"])} changed, '
            f'{len(env["added"])} added, {len(env["removed"])} removed'
            + (f' ({", ".join(env["changed"][:10])})' if env['changed'] else '') + '.'
        )
    for block in report['blocks']:
        line = f'- {block["block_uuid"]}: {block["result"]}'
        if block.get('detail'):
            line += f'. {block["detail"]}'
        lines.append(line)
    lines.append('Reproduced.' if report['passed'] else 'Not reproduced.')
    return '\n'.join(lines)
