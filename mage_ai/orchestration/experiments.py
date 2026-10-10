"""
Experiments: MLflow runs that blocks create, linked to the Mage runs that created them.

While a block runs in a pipeline run, every MLflow run its code starts gets tags that
name the Mage run: mage.project, mage.pipeline_uuid, mage.pipeline_run_id,
mage.block_uuid, mage.block_run_id, mage.execution_partition and mage.code_digest (the
run record's, run_records.py). MlflowRunContext adds them; MLflow loads it through the
`mlflow.run_context_provider` entry point of the mage-ml package, and Mage registers it
too when MLflow is already imported.

When the block run completes, Mage looks up the MLflow runs tagged with its block run
and records them in block_run.metrics['mlflow']: each run's experiment, name, status,
latest metrics, number of parameters and the model versions registered from it, with
the tracking URI. Blocks change nothing: they use MLflow as they would anyway.
"""
import importlib.abc
import importlib.machinery
import json
import os
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Dict, Iterator, List, Optional

TAG_PREFIX = 'mage.'
ENV_TAGS = 'MAGE_TRACKING_TAGS'
MAX_RUNS = 50
MAX_METRICS = 100

_tags: ContextVar[Optional[Dict[str, str]]] = ContextVar('mage_tracking_tags', default=None)


def enabled() -> bool:
    return os.getenv('MAGE_TRACK_EXPERIMENTS', '1').lower() not in ('0', 'false', 'off')


def current_tags() -> Dict[str, str]:
    """The Mage tags of the block running in this context, empty outside a block."""
    tags = _tags.get()
    if tags is not None:
        return tags
    try:
        return json.loads(os.environ.get(ENV_TAGS) or '{}')
    except ValueError:
        return {}


@contextmanager
def block_context(tags: Dict[str, Any]) -> Iterator[None]:
    """
    Tags MLflow runs started inside as runs of this block. The tags also go to the
    environment, for processes the block starts.
    """
    values = {
        f'{TAG_PREFIX}{key}': str(value) for key, value in tags.items() if value is not None
    }
    token = _tags.set(values)
    previous = os.environ.get(ENV_TAGS)
    os.environ[ENV_TAGS] = json.dumps(values)
    _register_if_imported()
    try:
        yield
    finally:
        _tags.reset(token)
        if previous is None:
            os.environ.pop(ENV_TAGS, None)
        else:
            os.environ[ENV_TAGS] = previous


REGISTRY_MODULE = 'mlflow.tracking.context.registry'


def _register(registry_module) -> None:
    try:
        from mage_ai.orchestration.mlflow_context import MlflowRunContext

        registry = registry_module._run_context_provider_registry
        if not any(type(p).__name__ == MlflowRunContext.__name__ for p in registry):
            registry.register(MlflowRunContext)
    except Exception:
        pass


class _RegisterOnImport(importlib.abc.MetaPathFinder):
    """Registers the context provider as soon as MLflow loads its provider registry."""

    def find_spec(self, name, path, target=None):
        if name != REGISTRY_MODULE:
            return None
        spec = importlib.machinery.PathFinder.find_spec(name, path)
        if spec is None or spec.loader is None:
            return None
        execute = spec.loader.exec_module

        def exec_module(module):
            execute(module)
            _register(module)

        spec.loader.exec_module = exec_module
        return spec


_hook = _RegisterOnImport()


def _register_if_imported() -> None:
    """
    Registers the context provider in MLflow: now if MLflow is imported, otherwise when
    the block imports it. The package entry point does the same where it is installed.
    """
    module = sys.modules.get(REGISTRY_MODULE)
    if module is not None:
        _register(module)
    elif _hook not in sys.meta_path:
        sys.meta_path.insert(0, _hook)


def _ui_url(tracking_uri: str, experiment_id: str, run_id: str) -> Optional[str]:
    if not tracking_uri.startswith(('http://', 'https://')):
        return None
    return f'{tracking_uri.rstrip("/")}/#/experiments/{experiment_id}/runs/{run_id}'


def runs_of_block_run(block_run_id: int) -> Optional[Dict[str, Any]]:
    """
    The MLflow runs tagged with the block run, as recorded in its metrics; None when the
    block did not import MLflow in this process.
    """
    if 'mlflow' not in sys.modules:
        return None
    import mlflow
    from mlflow.tracking import MlflowClient

    client = MlflowClient()
    tracking_uri = mlflow.get_tracking_uri()
    found = mlflow.search_runs(
        search_all_experiments=True,
        filter_string=f"tags.`{TAG_PREFIX}block_run_id` = '{int(block_run_id)}'",
        output_format='list',
        max_results=MAX_RUNS,
    )
    runs = []
    for run in found:
        info, data = run.info, run.data
        metrics = dict(sorted(data.metrics.items())[:MAX_METRICS])
        try:
            versions = [
                dict(name=v.name, version=str(v.version))
                for v in client.search_model_versions(f"run_id='{info.run_id}'")
            ]
        except Exception:
            versions = []
        runs.append(dict(
            run_id=info.run_id,
            run_name=info.run_name,
            experiment_id=info.experiment_id,
            status=info.status,
            start_time=info.start_time,
            end_time=info.end_time,
            metrics=metrics,
            params=len(data.params),
            artifact_uri=info.artifact_uri,
            model_versions=versions,
            url=_ui_url(tracking_uri, info.experiment_id, info.run_id),
        ))
    if not runs:
        return None
    return dict(tracking_uri=_safe_uri(tracking_uri), runs=runs)


def _safe_uri(uri: str) -> str:
    """The tracking URI without a password."""
    from urllib.parse import urlsplit, urlunsplit

    try:
        parts = urlsplit(uri)
    except ValueError:
        return uri
    if parts.password:
        netloc = parts.hostname or ''
        if parts.port:
            netloc += f':{parts.port}'
        if parts.username:
            netloc = f'{parts.username}@{netloc}'
        return urlunsplit(parts._replace(netloc=netloc))
    return uri


def metric_changes(first: Optional[Dict], second: Optional[Dict]) -> Dict[str, List]:
    """{metric: [first value, second value]} for metrics that differ between two records."""
    def flatten(record: Optional[Dict]) -> Dict[str, float]:
        values = {}
        runs = (record or {}).get('runs') or []
        for run in runs:
            prefix = f'{run.get("run_name")}/' if len(runs) > 1 else ''
            for key, value in (run.get('metrics') or {}).items():
                values[f'{prefix}{key}'] = value
        return values

    before, after = flatten(first), flatten(second)
    return {
        key: [before.get(key), after.get(key)]
        for key in sorted(set(before) | set(after))
        if before.get(key) != after.get(key)
    }
