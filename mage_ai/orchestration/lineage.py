"""
OpenLineage events of MageML's pipeline runs and block runs, as exported services send them
(pipeline_services/service/crates/service/src/lineage.rs), so catalogs (Marquez, DataHub,
OpenMetadata) show the project's pipelines and the data between their blocks.

- A pipeline run is a job named by its pipeline; each attempt of a block run is a job
  `<pipeline>.<block>` whose run has the pipeline run as its parent facet. Each gets START,
  then COMPLETE, FAIL or ABORT.
- A block's output is the dataset `<pipeline>.<block>`, with its columns (a schema facet)
  from its stored Parquet output; its inputs are its upstream blocks'.
- Run ids are UUIDs derived from the project and the run's id, so every process names a run
  the same way.

`OPENLINEAGE_URL` (with `OPENLINEAGE_ENDPOINT`, `api/v1/lineage` by default),
`OPENLINEAGE_API_KEY`, `OPENLINEAGE_NAMESPACE` (the project's name by default) and
`OPENLINEAGE_DISABLED` configure it; without a URL nothing starts. Events go out in order from
one thread; a slow or absent backend drops them. A block run's process waits at most 5 s at
its end for its events to go out.
"""
import atexit
import json
import os
import queue
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

SCHEMA_URL = 'https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent'
PARENT_URL = 'https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json#/$defs/ParentRunFacet'
SCHEMA_FACET_URL = \
    'https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet'
ERROR_FACET_URL = 'https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json' \
    '#/$defs/ErrorMessageRunFacet'
PRODUCER = 'https://github.com/jospablo777/mage-ml'
QUEUE = 4096
FLUSH_SECONDS = 5
REQUEST_SECONDS = 10


def config() -> Optional[Dict]:
    """The backend the environment names, or None when lineage is off."""
    if (os.getenv('OPENLINEAGE_DISABLED') or '').strip().lower() == 'true':
        return None
    url = (os.getenv('OPENLINEAGE_URL') or '').strip()
    if not url:
        return None
    endpoint = (os.getenv('OPENLINEAGE_ENDPOINT') or 'api/v1/lineage').strip()
    return dict(
        url=f"{url.rstrip('/')}/{endpoint.lstrip('/')}",
        api_key=(os.getenv('OPENLINEAGE_API_KEY') or '').strip() or None,
        namespace=(os.getenv('OPENLINEAGE_NAMESPACE') or '').strip() or None,
    )


class Sender:
    """One thread posting events in order from a bounded queue."""

    def __init__(self, settings: Dict):
        self.settings = settings
        self.queue: 'queue.Queue' = queue.Queue(maxsize=QUEUE)
        self.sent = self.failed = self.dropped = 0
        self._warned_at = 0.0
        threading.Thread(target=self._loop, name='mage-lineage', daemon=True).start()

    def emit(self, event: Dict) -> None:
        try:
            self.queue.put_nowait(event)
        except queue.Full:
            self.dropped += 1

    def flush(self, seconds: float = FLUSH_SECONDS) -> None:
        deadline = time.monotonic() + seconds
        while self.queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.02)

    def _loop(self) -> None:
        while True:
            event = self.queue.get()
            try:
                self._post(event)
                self.sent += 1
            except Exception as error:
                self.failed += 1
                # One line a minute: a backend that is down does not flood the log.
                if time.monotonic() - self._warned_at > 60:
                    self._warned_at = time.monotonic()
                    print(f'[Lineage] An event was not sent to {self.settings["url"]}: {error}')
            finally:
                self.queue.task_done()

    def _post(self, event: Dict) -> None:
        headers = {'Content-Type': 'application/json'}
        if self.settings.get('api_key'):
            headers['Authorization'] = f"Bearer {self.settings['api_key']}"
        request = urllib.request.Request(
            self.settings['url'], data=json.dumps(event).encode(), headers=headers,
            method='POST',
        )
        with urllib.request.urlopen(request, timeout=REQUEST_SECONDS) as response:
            response.read()


_sender: Optional[Sender] = None
_lock = threading.Lock()


def sender() -> Optional[Sender]:
    """The process's sender; started at the first event (a forked process starts its own)."""
    global _sender
    settings = config()
    if settings is None:
        return None
    with _lock:
        if _sender is None or _sender.settings != settings or _sender.pid != os.getpid():
            _sender = Sender(settings)
            _sender.pid = os.getpid()
            atexit.register(_sender.flush)
            # Block runs run in multiprocessing children, which end without atexit.
            from multiprocessing import util

            util.Finalize(None, _sender.flush, exitpriority=10)
    return _sender


def run_id(project: str, kind: str, *parts) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f'mageml:{project}:{kind}:' + ':'.join(
        str(p) for p in parts)))


def namespace(project: str) -> str:
    settings = config() or {}
    return settings.get('namespace') or project


def block_dataset(project: str, pipeline: str, block: str) -> Dict:
    return dict(namespace=namespace(project), name=f'{pipeline}.{block}')


def with_schema(dataset: Dict, parquet: Optional[str]) -> Dict:
    """`dataset` with the columns of its stored output, when it is a Parquet file."""
    if not parquet or not os.path.isfile(parquet):
        return dataset
    try:
        import pyarrow.parquet as pq

        schema = pq.read_schema(parquet)
    except Exception:
        return dataset
    fields = [dict(name=f.name, type=str(f.type)) for f in schema
              if not f.name.startswith('__index_level_')]
    return dict(dataset, facets=dict(schema=dict(
        _producer=PRODUCER, _schemaURL=SCHEMA_FACET_URL, fields=fields,
    )))


def event(
    project: str,
    job: str,
    run: str,
    outcome: Optional[str] = None,
    parent: Optional[tuple] = None,
    error: Optional[str] = None,
    inputs: List[Dict] = None,
    outputs: List[Dict] = None,
) -> Dict:
    """A run event: START when `outcome` is None, else COMPLETE, FAIL or ABORT."""
    facets = {}
    if parent:
        parent_run, parent_job = parent
        facets['parent'] = dict(
            _producer=PRODUCER, _schemaURL=PARENT_URL,
            run=dict(runId=parent_run), job=dict(namespace=namespace(project), name=parent_job),
        )
    if error:
        facets['errorMessage'] = dict(
            _producer=PRODUCER, _schemaURL=ERROR_FACET_URL, message=str(error)[:4000],
            programmingLanguage='python',
        )
    return dict(
        eventType=outcome or 'START',
        eventTime=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        producer=PRODUCER,
        schemaURL=SCHEMA_URL,
        run=dict(runId=run, facets=facets),
        job=dict(namespace=namespace(project), name=job),
        inputs=inputs or [],
        outputs=outputs or [],
    )


def emit(event_: Dict) -> None:
    target = sender()
    if target is not None:
        target.emit(event_)


def flush(seconds: float = FLUSH_SECONDS) -> None:
    if _sender is not None:
        _sender.flush(seconds)


# What a pipeline run's status says in an event.
OUTCOMES = dict(completed='COMPLETE', failed='FAIL', cancelled='ABORT')


def pipeline_run_status(pipeline_run, status) -> None:
    """A pipeline run started or ended: its START, or its COMPLETE, FAIL or ABORT."""
    if config() is None:
        return
    status = str(getattr(status, 'value', status))
    outcome = 'START' if status == 'running' else OUTCOMES.get(status)
    if outcome is None:
        return
    project = _project_name()
    emit(event(
        project, pipeline_run.pipeline_uuid, run_id(project, 'pipeline_run', pipeline_run.id),
        outcome=None if outcome == 'START' else outcome,
    ))


def _project_name() -> str:
    from mage_ai.settings.repo import get_repo_path

    return os.path.basename(os.path.normpath(get_repo_path()))
