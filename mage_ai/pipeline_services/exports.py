"""
Service exports started from the app (Export as service). They are written to the
project's MageML data folder, not to the project: an export holds about 1,500 files, the
service's source among them, which would crowd the file browser and the git history.
The app downloads an export as a zip.
"""
import contextlib
import datetime
import json
import os
import re
import tempfile
import threading
import time
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

EXPORTS_FOLDER = 'service_exports'
_NAME = re.compile(r'^[a-z0-9][a-z0-9-]*$')


def exports_root(repo_path: str) -> Path:
    from mage_ai.settings.repo import get_variables_dir

    return Path(get_variables_dir(repo_path=repo_path)) / EXPORTS_FOLDER


def export_directory(repo_path: str, name: str) -> Path:
    """The folder of an export, refusing names that would leave the exports folder."""
    if not isinstance(name, str) or not _NAME.match(name):
        raise ValueError(f'{name!r} is not a service name: use lowercase letters, digits and -.')
    return exports_root(repo_path) / name


# Exports running or finished in this process, by project and service name, so the app can
# show an export's progress.
_jobs: Dict[tuple, Dict] = {}
_jobs_lock = threading.Lock()

STAGE_READING = 'Reading the pipelines and the code they import'
STAGE_WRITING = 'Writing the Docker build context'
STAGE_WRITING_MODELS = 'Downloading the models and writing the Docker build context'
STAGE_DONE = 'Ready'


# Feast's MLflow integration tags a model with the feature service it was trained on.
FEATURE_SERVICE_TAG = 'feast.feature_service'

# MLflow reads its server from the process environment; exports that name a registry set it
# one at a time.
_registry_lock = threading.Lock()


# How long MLflow may take: the check waits for the registry while someone edits, so it
# gives up fast; an export retries a little. Without limits MLflow retries an unreachable
# server for about four minutes. Values set in the environment win.
CHECK_LIMITS = dict(MLFLOW_HTTP_REQUEST_MAX_RETRIES='0', MLFLOW_HTTP_REQUEST_TIMEOUT='10')
EXPORT_LIMITS = dict(MLFLOW_HTTP_REQUEST_MAX_RETRIES='2', MLFLOW_HTTP_REQUEST_TIMEOUT='60')


@contextlib.contextmanager
def model_registry(url: Optional[str], limits: Optional[Dict[str, str]] = None):
    """
    MLFLOW_TRACKING_URI set to url while the block runs, when url is given, and MLflow's
    request limits, unless the environment sets them.
    """
    limits = {k: v for k, v in (limits or {}).items() if k not in os.environ}
    if not url and not limits:
        yield
        return
    with _registry_lock:
        previous = os.environ.get('MLFLOW_TRACKING_URI')
        os.environ.update(limits)
        if url:
            os.environ['MLFLOW_TRACKING_URI'] = url
            try:
                import mlflow

                mlflow.set_tracking_uri(url)
            except ImportError:
                pass
        try:
            yield
        finally:
            for key in limits:
                os.environ.pop(key, None)
            if url:
                try:
                    import mlflow

                    # None also removes MLFLOW_TRACKING_URI from the environment; the
                    # earlier value comes back below.
                    mlflow.set_tracking_uri(previous)
                except ImportError:
                    pass
                if previous is None:
                    os.environ.pop('MLFLOW_TRACKING_URI', None)
                else:
                    os.environ['MLFLOW_TRACKING_URI'] = previous


def model_options(models: Optional[List[Dict]]) -> List[str]:
    """--model options, NAME=URI, for the models a service embeds."""
    return [f"{m['name']}={m['uri']}" for m in models or []]


def challenger_settings(models: Optional[List[Dict]]) -> Dict[str, Dict]:
    """Each model's challengers and shadow sample, from a service definition."""
    return {
        m['name']: dict(sample=m['shadow']['sample'], challengers=m['challengers'])
        for m in models or [] if m.get('challengers')
    }


def resolve_models(
    models: List[Tuple[str, str]],
    registry: Optional[str] = None,
) -> Tuple[List[Dict], List[str]]:
    """
    The registry version each (name, uri) points to (an alias resolves to its version), with
    the version's run and tags, and the problems reaching the registry.
    """
    from mage_ai.pipeline_services import model_export

    resolved, problems = [], []
    for name, uri in models:
        model = dict(name=name, uri=uri)
        try:
            details = model_export._registry_details(uri)
        except ImportError:
            problems.append(
                'Embedding a model from a registry needs mlflow: uv pip install mlflow.'
            )
            break
        except Exception as error:
            problems.append(
                f"Model {model['name']} ({model['uri']}) could not be read from the registry: "
                f'{model_export.explain(error, uri, registry)}'
            )
            continue
        tags = details.get('tags') or {}
        service = tags.get(FEATURE_SERVICE_TAG)
        if not service and details.get('run_id'):
            try:
                service = model_export._run_details(details['run_id'])['run_tags'].get(
                    FEATURE_SERVICE_TAG)
            except Exception:
                service = None
        resolved.append(dict(
            name=model['name'],
            uri=model['uri'],
            feature_service=service,
            registered_name=details.get('registered_name'),
            version=details.get('version'),
            run_id=details.get('run_id'),
            aliases=details.get('aliases') or [],
            tags=details.get('tags') or {},
        ))
    return resolved, problems


def _key(repo_path: str, name: str) -> tuple:
    # The export resolves the project's path (macOS /var is /private/var) and sets the repo
    # path to it; keys use the resolved path so a job is found from either.
    return (os.path.realpath(repo_path), name)


def start_export(
    repo_path: str,
    pipeline_uuids: List[str],
    name: Optional[str] = None,
    max_runs: Optional[int] = None,
    includes: Optional[List[str]] = None,
    models: Optional[List[Dict]] = None,
    registry: Optional[str] = None,
) -> Dict:
    """
    Starts exporting in a thread and returns its job: state (running, completed, failed),
    stage, progress from 0 to 1, and when it ends the export's summary or its problems. An
    export of the same name already running is returned instead of starting another.
    """
    from mage_ai.pipeline_services import export

    service = export._service_name(name, list(pipeline_uuids))
    export_directory(repo_path, service)
    key = _key(repo_path, service)
    with _jobs_lock:
        running = _jobs.get(key)
        if running and running['state'] == 'running':
            return dict(running)
        job = dict(
            name=service,
            problems=[],
            progress=0.05,
            stage=STAGE_READING,
            started_at=time.time(),
            state='running',
        )
        _jobs[key] = job

    def update(**values):
        with _jobs_lock:
            job.update(values)

    def run():
        try:
            result = export_pipelines(
                repo_path, pipeline_uuids, name=service, max_runs=max_runs,
                includes=includes, models=models, registry=registry,
                on_stage=lambda stage, progress: update(stage=stage, progress=progress),
            )
            update(progress=1.0, result=result, stage=STAGE_DONE, state='completed')
        except export.ExportError as error:
            update(problems=list(error.problems), state='failed')
        except Exception as error:
            import traceback

            traceback.print_exc()
            update(
                problems=[f'The export stopped on an error: {type(error).__name__}: {error}'],
                state='failed',
            )
        finally:
            update(finished_at=time.time())

    threading.Thread(target=run, daemon=True, name=f'export-{service}').start()
    return dict(job)


def export_job(repo_path: str, name: str) -> Optional[Dict]:
    with _jobs_lock:
        job = _jobs.get(_key(repo_path, name))
        return dict(job) if job else None


def export_pipelines(
    repo_path: str,
    pipeline_uuids: List[str],
    name: Optional[str] = None,
    max_runs: Optional[int] = None,
    on_stage=None,
    includes: Optional[List[str]] = None,
    models: Optional[List[Dict]] = None,
    registry: Optional[str] = None,
) -> Dict:
    """
    Exports the pipelines as one service, as `mage export service` does, replacing an earlier
    export of the same name, and returns what the app shows. Raises export.ExportError with
    every problem. on_stage(stage, progress) hears when each stage starts.
    """
    from mage_ai.pipeline_services import export

    if on_stage:
        on_stage(STAGE_READING, 0.05)
    with model_registry(registry, EXPORT_LIMITS):
        captured = export.capture(
            repo_path, list(pipeline_uuids), name=name or None, max_concurrent_runs=max_runs,
            includes=includes, models=model_options(models),
            challengers=challenger_settings(models),
        )
        out_dir = export_directory(repo_path, captured.name)
        out_dir.parent.mkdir(parents=True, exist_ok=True)
        if on_stage:
            on_stage(STAGE_WRITING_MODELS if captured.models else STAGE_WRITING, 0.6)
        # Downloads the models from the registry into the build context.
        export.write(captured, str(out_dir), force=True)
    (out_dir / 'report.txt').write_text(export.report(captured))
    return summary(captured, out_dir)


def preview_pipelines(
    repo_path: str,
    pipeline_uuids: List[str],
    name: Optional[str] = None,
    max_runs: Optional[int] = None,
    includes: Optional[List[str]] = None,
    models: Optional[List[Dict]] = None,
    registry: Optional[str] = None,
) -> Dict:
    """
    What exporting the pipelines would give, without writing anything: the app shows it while
    the pipelines are chosen. Problems come back in the result instead of as an error.
    """
    from mage_ai.pipeline_services import export

    with model_registry(registry, CHECK_LIMITS):
        try:
            captured = export.capture(
                repo_path, list(pipeline_uuids), name=name or None,
                max_concurrent_runs=max_runs, includes=includes, models=model_options(models),
                challengers=challenger_settings(models),
            )
        except export.ExportError as error:
            return dict(name=export._service_name(name, list(pipeline_uuids)),
                        problems=list(error.problems))
        resolved, problems = resolve_models(captured.models, registry)
    result = summary(captured, None)
    result['models'] = resolved
    result['notes'] = result['notes'] + unused_models(captured, models)
    result['problems'] = problems
    return result


# Calls that name a model: load_model('churn', ...), model_path, model_metadata,
# feature_service.
_MODEL_CALL = re.compile(
    r'\b(?:load_model|model_path|model_metadata|feature_service)\(\s*[\'"]([^\'"]+)[\'"]'
)


def unused_models(captured, models: Optional[List[Dict]]) -> List[str]:
    """Notes for chosen models no block names, which usually means a typo in one of them."""
    if not models:
        return []
    named = set()
    for path in captured.files:
        if path.suffix == '.py':
            try:
                named.update(_MODEL_CALL.findall(path.read_text()))
            except (OSError, UnicodeDecodeError):
                continue
    return [
        f"No block loads the model {m['name']} by that name; the image holds it, but blocks "
        f"reach it with load_model('{m['name']}', ...). Check the name in the code."
        for m in models if m['name'] not in named
    ]


def _embedded_model(model: Dict, out_dir: Optional[Path]) -> Dict:
    """An embedded model as the app shows it, with the registry details of its metadata."""
    metadata = {}
    path = out_dir / model['path'] / 'mage-model.json' if out_dir and model.get('path') else None
    if path and path.is_file():
        try:
            metadata = json.loads(path.read_text())
        except ValueError:
            pass
    return dict(
        name=model.get('name'),
        uri=model.get('uri'),
        feature_service=(metadata.get('tags') or {}).get(FEATURE_SERVICE_TAG)
        or (metadata.get('run_tags') or {}).get(FEATURE_SERVICE_TAG),
        version=model.get('version'),
        run_id=model.get('run_id'),
        registered_name=metadata.get('registered_name'),
        aliases=metadata.get('aliases') or [],
        tags=metadata.get('tags') or {},
        flavors=model.get('flavors') or [],
        size_bytes=model.get('size_bytes'),
    )


def summary(captured, out_dir: Optional[Path]) -> Dict:
    pipelines = []
    for pipeline in captured.manifest['pipelines']:
        pipelines.append(dict(
            blocks=[
                dict(conditions=b.get('conditions') or [], language=b['language'],
                     type=b['type'], uuid=b['uuid'])
                for b in pipeline['blocks']
            ],
            max_concurrent_runs=pipeline['max_concurrent_runs'],
            triggers=[
                dict(
                    active=t['active'],
                    after_pipeline=t.get('after_pipeline'),
                    after_status=t.get('after_status'),
                    kind=t['kind'],
                    name=t['name'],
                    schedule=t.get('schedule'),
                )
                for t in pipeline['triggers']
            ],
            uuid=pipeline['uuid'],
        ))
        if pipeline.get('stream'):
            stream = pipeline['stream']
            pipelines[-1]['stream'] = dict(
                sinks=[_stream_connector(c) for c in stream['sinks']],
                source=_stream_connector(stream['source']),
            )
    return dict(
        environment=[
            dict(name=v['name'], required=v['required'], secret=v['secret'])
            for v in captured.manifest.get('environment') or []
        ],
        files=sorted(str(p.relative_to(captured.project)) for p in captured.files),
        name=captured.name,
        notes=list(captured.notes),
        packages=dict(captured.requirements),
        models=[_embedded_model(m, out_dir) for m in captured.manifest.get('models') or []],
        path=str(out_dir) if out_dir else None,
        pipelines=pipelines,
    )


def _stream_connector(connector: Dict) -> Dict:
    """What the app shows of a stream connector: its kind, block and topics, no settings."""
    config = connector.get('config') or {}
    topics = config.get('topics') or ([config['topic']] if config.get('topic') else [])
    return dict(
        after=connector.get('after'),
        kind=connector['kind'],
        name=connector['name'],
        topics=[t for t in topics if isinstance(t, str)],
    )


def list_exports(repo_path: str) -> List[Dict]:
    """The exports made from the app, newest first, read from each one's service.json."""
    root = exports_root(repo_path)
    if not root.is_dir():
        return []
    found = []
    for directory in root.iterdir():
        manifest_path = directory / 'service.json'
        if not _NAME.match(directory.name) or not manifest_path.is_file():
            continue
        try:
            manifest = json.loads(manifest_path.read_text())
        except (OSError, ValueError):
            continue
        exported_at = (manifest.get('service') or {}).get('exported_at') or (
            datetime.datetime.fromtimestamp(
                manifest_path.stat().st_mtime, tz=datetime.timezone.utc,
            ).isoformat()
        )
        pipelines = manifest.get('pipelines') or []
        found.append(dict(
            blocks=sum(len(p.get('blocks') or []) for p in pipelines),
            environment=[v.get('name') for v in manifest.get('environment') or []],
            exported_at=exported_at,
            models=[m.get('name') for m in manifest.get('models') or []],
            name=directory.name,
            path=str(directory),
            pipelines=[p.get('uuid') for p in pipelines],
            schedules=sum(
                1 for p in pipelines for t in p.get('triggers') or []
                if t.get('kind') == 'time'
            ),
        ))
    return sorted(found, key=lambda e: e['exported_at'], reverse=True)


def zip_export(repo_path: str, name: str):
    """
    A temporary zip of an export, its files under a folder named like the service. Returns
    the open file; it is deleted when closed.
    """
    directory = export_directory(repo_path, name)
    if not (directory / 'service.json').is_file():
        raise FileNotFoundError(f'There is no export named {name}; export it again.')
    archive = tempfile.NamedTemporaryFile(suffix='.zip', mode='w+b')
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, folders, files in os.walk(directory):
            folders.sort()
            for file in sorted(files):
                path = Path(root) / file
                if path.is_symlink():
                    continue
                # Streamed: an export holds its models, which can be large files.
                zipf.write(path, str(Path(name) / path.relative_to(directory)))
    archive.seek(0)
    return archive


def docker_status() -> Dict:
    """Whether this server can build images: docker installed and its daemon answering."""
    import shutil
    import subprocess

    docker = shutil.which('docker')
    if not docker:
        return dict(docker=False, buildx=False, reason='docker is not installed on the server.')

    def answers(*args) -> bool:
        try:
            return subprocess.run(
                [docker, *args], capture_output=True, timeout=10,
            ).returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            return False

    if not answers('info', '--format', '{{.ServerVersion}}'):
        return dict(docker=False, buildx=False,
                    reason='docker is installed, but its daemon does not answer.')
    return dict(docker=True, buildx=answers('buildx', 'version'), reason=None)


_builds: Dict[tuple, Dict] = {}


def start_build(
    repo_path: str,
    name: str,
    image: str,
    platforms: Optional[List[str]] = None,
    push: bool = False,
) -> Dict:
    """
    Builds an export's image in a thread, and pushes it when asked; the job keeps every line
    docker prints. A build of the same export already running is returned instead.
    """
    from mage_ai.pipeline_services import export

    directory = export_directory(repo_path, name)
    if not (directory / 'service.json').is_file():
        raise FileNotFoundError(f'There is no export named {name}; export it first.')
    commands = export.build_commands(directory, image, platforms=platforms, push=push)
    key = _key(repo_path, name)
    with _jobs_lock:
        running = _builds.get(key)
        if running and running['state'] == 'running':
            return dict(running, lines=list(running['lines']))
        job = dict(
            command=' && '.join(' '.join(['docker'] + c[1:]) for c in commands),
            image=image,
            lines=[],
            name=name,
            problems=[],
            push=push,
            started_at=time.time(),
            state='running',
        )
        _builds[key] = job

    def on_line(line: str):
        with _jobs_lock:
            job['lines'].append(line)

    def run():
        try:
            export.build_image(directory, image, platforms=platforms, push=push, on_line=on_line)
            state, problems = 'completed', []
        except export.ExportError as error:
            state, problems = 'failed', list(error.problems)
        except Exception as error:
            state, problems = 'failed', [f'The build stopped on an error: {error}']
        with _jobs_lock:
            job.update(finished_at=time.time(), problems=problems, state=state)

    threading.Thread(target=run, daemon=True, name=f'build-{name}').start()
    return dict(job, lines=list(job['lines']))


def build_job(repo_path: str, name: str) -> Optional[Dict]:
    with _jobs_lock:
        job = _builds.get(_key(repo_path, name))
        return dict(job, lines=list(job['lines'])) if job else None
