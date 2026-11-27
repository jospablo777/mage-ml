"""
OpenTelemetry traces of MageML's pipeline runs and block runs, as exported services send
theirs (pipeline_services/service/crates/service/src/traces.rs): a span per pipeline run
and, under it, a span per block run attempt, sent over OTLP/HTTP as JSON.

A pipeline run's blocks run in other processes, at other times. So a run's trace id and span
id are derived from the project and the run's id, and every process names them the same way:
an attempt's span is sent when the attempt ends, with the run's span as its parent, and the
run's span when the run ends, with its recorded start and end. Block code gets its attempt's
span as TRACEPARENT, so OpenTelemetry in it continues the trace.

`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, or `OTEL_EXPORTER_OTLP_ENDPOINT` with `/v1/traces`
when `OTEL_EXPORTER_OTLP_PROTOCOL` is http/protobuf or http/json (MageML's server exports
its own spans over gRPC to OTEL_EXPORTER_OTLP_ENDPOINT), with `OTEL_EXPORTER_OTLP_HEADERS`,
`OTEL_SERVICE_NAME` (`mageml-runs`), `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_SDK_DISABLED`, and
`OTEL_TRACES_SAMPLER=traceidratio` with `OTEL_TRACES_SAMPLER_ARG`. Without an endpoint nothing
starts. Spans go out from one thread; a block run's process waits at most 5 s at its end.
"""
import atexit
import hashlib
import json
import os
import queue
import random
import threading
import time
import urllib.parse
import urllib.request
from datetime import timezone
from typing import Dict, List, Optional, Tuple

BATCH = 512
FLUSH_SECONDS = 5
QUEUE = 8192


def config() -> Optional[Dict]:
    if (os.getenv('OTEL_SDK_DISABLED') or '').strip().lower() == 'true':
        return None
    endpoint = (os.getenv('OTEL_EXPORTER_OTLP_TRACES_ENDPOINT') or '').strip()
    if not endpoint:
        shared = (os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT') or '').strip()
        protocol = (os.getenv('OTEL_EXPORTER_OTLP_TRACES_PROTOCOL')
                    or os.getenv('OTEL_EXPORTER_OTLP_PROTOCOL') or '').strip()
        if not shared or not protocol.startswith('http'):
            return None
        endpoint = f"{shared.rstrip('/')}/v1/traces"
    headers = {}
    for variable in ('OTEL_EXPORTER_OTLP_HEADERS', 'OTEL_EXPORTER_OTLP_TRACES_HEADERS'):
        headers.update(_pairs(os.getenv(variable) or ''))
    resource = dict(_pairs(os.getenv('OTEL_RESOURCE_ATTRIBUTES') or ''))
    resource['service.name'] = (os.getenv('OTEL_SERVICE_NAME') or '').strip() or 'mageml-runs'
    ratio = 1.0
    if (os.getenv('OTEL_TRACES_SAMPLER') or '').strip() in (
            'traceidratio', 'parentbased_traceidratio'):
        try:
            ratio = float(os.getenv('OTEL_TRACES_SAMPLER_ARG') or 1)
        except ValueError:
            ratio = 1.0
    return dict(endpoint=endpoint, headers=headers, resource=resource, ratio=ratio)


def _pairs(text: str) -> List[Tuple[str, str]]:
    pairs = []
    for pair in text.split(','):
        key, _, value = pair.partition('=')
        if key.strip() and _:
            pairs.append((key.strip(), urllib.parse.unquote(value.strip())))
    return pairs


def sampled(trace_id: str, settings: Dict) -> bool:
    # The trace id's last 8 bytes against the ratio, as the engine samples.
    return int(trace_id[16:], 16) < settings['ratio'] * 0xFFFFFFFFFFFFFFFF


def run_ids(project: str, pipeline_run_id) -> Tuple[str, str]:
    """
    A pipeline run's trace id and span id, hex, the same in every process. From SHA-256, so
    every bit varies: a UUID's fixed variant bits fall where the ratio sampler reads.
    """
    digest = hashlib.sha256(f'mageml:{project}:run:{pipeline_run_id}'.encode()).hexdigest()
    return digest[:32], digest[32:48]


def new_span_id() -> str:
    return f'{random.getrandbits(64) or 1:016x}'


def traceparent(trace_id: str, span_id: str) -> str:
    return f'00-{trace_id}-{span_id}-01'


def span(
    name: str,
    trace_id: str,
    span_id: str,
    parent_id: Optional[str],
    start_ns: int,
    end_ns: int,
    attributes: Dict,
    error: Optional[str] = None,
) -> Dict:
    """A span in OTLP/JSON: ids in hex, times as strings of nanoseconds."""
    value = dict(
        traceId=trace_id, spanId=span_id, name=name, kind=1,
        startTimeUnixNano=str(start_ns), endTimeUnixNano=str(max(end_ns, start_ns)),
        attributes=[_attribute(k, v) for k, v in attributes.items() if v is not None],
    )
    if parent_id:
        value['parentSpanId'] = parent_id
    if error:
        value['status'] = dict(code=2, message=str(error)[:1000])
    return value


def _attribute(key: str, value) -> Dict:
    if isinstance(value, bool):
        return dict(key=key, value=dict(boolValue=value))
    if isinstance(value, int):
        return dict(key=key, value=dict(intValue=str(value)))
    return dict(key=key, value=dict(stringValue=str(value)))


class Exporter:
    def __init__(self, settings: Dict):
        self.settings = settings
        self.queue: 'queue.Queue' = queue.Queue(maxsize=QUEUE)
        self.sent = self.failed = self.dropped = 0
        self._warned_at = 0.0
        self.pid = os.getpid()
        threading.Thread(target=self._loop, name='mage-traces', daemon=True).start()

    def emit(self, value: Dict) -> None:
        try:
            self.queue.put_nowait(value)
        except queue.Full:
            self.dropped += 1

    def flush(self, seconds: float = FLUSH_SECONDS) -> None:
        deadline = time.monotonic() + seconds
        while self.queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.02)

    def _loop(self) -> None:
        while True:
            batch = [self.queue.get()]
            deadline = time.monotonic() + 1
            while len(batch) < BATCH:
                try:
                    batch.append(self.queue.get(timeout=max(0.0, deadline - time.monotonic())))
                except queue.Empty:
                    break
            try:
                self._post(batch)
                self.sent += len(batch)
            except Exception as error:
                self.failed += len(batch)
                if time.monotonic() - self._warned_at > 60:
                    self._warned_at = time.monotonic()
                    print(f'[Traces] {len(batch)} span(s) not sent to '
                          f'{self.settings["endpoint"]}: {error}')
            finally:
                for _ in batch:
                    self.queue.task_done()

    def _post(self, spans: List[Dict]) -> None:
        body = dict(resourceSpans=[dict(
            resource=dict(attributes=[
                _attribute(k, v) for k, v in self.settings['resource'].items()
            ]),
            scopeSpans=[dict(scope=dict(name='mageml'), spans=spans)],
        )])
        request = urllib.request.Request(
            self.settings['endpoint'], data=json.dumps(body).encode(),
            headers={'Content-Type': 'application/json', **self.settings['headers']},
            method='POST',
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()


_exporter: Optional[Exporter] = None
_lock = threading.Lock()


def exporter() -> Optional[Exporter]:
    """The process's exporter; started at the first span (a forked process starts its own)."""
    global _exporter
    settings = config()
    if settings is None:
        return None
    with _lock:
        if _exporter is None or _exporter.settings != settings or _exporter.pid != os.getpid():
            _exporter = Exporter(settings)
            atexit.register(_exporter.flush)
            # Block runs run in multiprocessing children, which end without atexit.
            from multiprocessing import util

            util.Finalize(None, _exporter.flush, exitpriority=10)
    return _exporter


def emit(value: Dict) -> None:
    target = exporter()
    if target is not None and sampled(value['traceId'], target.settings):
        target.emit(value)


def flush(seconds: float = FLUSH_SECONDS) -> None:
    if _exporter is not None:
        _exporter.flush(seconds)


def pipeline_run_ended(pipeline_run, status) -> None:
    """The pipeline run's span, from its recorded start to now, when it ends."""
    status = str(getattr(status, 'value', status))
    if status not in ('completed', 'failed', 'cancelled') or config() is None:
        return
    from mage_ai.orchestration.lineage import _project_name

    project = _project_name()
    trace_id, span_id = run_ids(project, pipeline_run.id)
    started = pipeline_run.started_at or pipeline_run.created_at
    if started is not None and started.tzinfo is None:
        # The run history keeps UTC without a zone.
        started = started.replace(tzinfo=timezone.utc)
    start_ns = int(started.timestamp() * 1e9) if started else time.time_ns()
    emit(span(
        f'run {pipeline_run.pipeline_uuid}', trace_id, span_id, None, start_ns, time.time_ns(),
        {'mage.pipeline': pipeline_run.pipeline_uuid, 'mage.run_id': str(pipeline_run.id),
         'mage.run.status': status},
        error=f'The run {status}.' if status == 'failed' else None,
    ))


def trace_url(pipeline_run) -> Optional[str]:
    """
    The pipeline run's trace in the tracing UI: MAGE_TRACE_URL with {trace_id} in it, such as
    https://jaeger.example/trace/{trace_id}; None when runs are not traced or it is not set.
    """
    template = (os.getenv('MAGE_TRACE_URL') or '').strip()
    if '{trace_id}' not in template or config() is None:
        return None
    from mage_ai.orchestration.lineage import _project_name

    trace_id, _ = run_ids(_project_name(), pipeline_run.id)
    return template.replace('{trace_id}', trace_id)
