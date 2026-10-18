"""
A streaming pipeline running on the local engine from the editor (C2): start, pause,
resume, stop, and its status (state, rates, lag, totals, the engine's log).

The run is a development run (SAFETY.md section 3): sources read with positions of their own
(mage_ai/streaming/development.py), and it starts only when every sink it writes was named
by the user for this run. The engine runs the pipeline as an exported service would, from a
box in .mage/dev/<pipeline>/, with the server's environment, as editor runs have.
"""
import atexit
import json
import secrets
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

DEV_DIR = Path('.mage') / 'dev'
LOG_LINES = 300


class DevRunError(RuntimeError):
    pass


@dataclass
class DevRun:
    pipeline: str
    port: int
    token: str
    process: subprocess.Popen
    log_path: Path
    notes: List[str] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    # Stopped by the user, not exited on its own.
    stopped: bool = False

    @property
    def alive(self) -> bool:
        return self.process.poll() is None

    def call(self, method: str, path: str, body: Optional[Dict] = None) -> Dict:
        request = urllib.request.Request(
            f'http://127.0.0.1:{self.port}{path}', method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={'Authorization': f'Bearer {self.token}',
                     'Content-Type': 'application/json'},
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return json.loads(response.read() or b'{}')
        except urllib.error.HTTPError as error:
            try:
                message = json.loads(error.read()).get('error')
            except ValueError:
                message = None
            raise DevRunError(message or f'The engine answered {error.code}.') from None
        except OSError as error:
            raise DevRunError(f'The engine does not answer: {error}') from None


_runs: Dict[Tuple[str, str], DevRun] = {}
_lock = threading.Lock()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def _log_tail(path: Path, lines: int = LOG_LINES) -> List[str]:
    if not path.is_file():
        return []
    return path.read_text(errors='replace').splitlines()[-lines:]


def start(repo_path: str, pipeline_uuid: str, confirmed_sinks: List[str],
          user: Optional[str] = None, timeout: float = 90) -> Dict:
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.pipeline_services import engine, export, local_run
    from mage_ai.streaming.development import for_development_manifest, sinks_written

    key = (str(Path(repo_path).resolve()), pipeline_uuid)
    with _lock:
        current = _runs.get(key)
        if current is not None and current.alive:
            raise DevRunError(f'{pipeline_uuid} already runs on the local engine; stop it first.')
    pipeline = Pipeline.get(pipeline_uuid, repo_path=repo_path)
    unconfirmed = [(uuid, where) for uuid, where in sinks_written(pipeline)
                   if uuid not in set(confirmed_sinks or [])]
    if unconfirmed:
        raise DevRunError(
            'This run writes to sinks: '
            + ', '.join(f'{uuid} ({where})' for uuid, where in unconfirmed)
            + '. Name them to start it.'
        )
    binary = engine.engine_path()
    folder = Path(repo_path) / DEV_DIR / pipeline_uuid
    try:
        captured = export.capture(repo_path, [pipeline_uuid], name=f'dev-{pipeline_uuid}')
    except export.ExportError as error:
        raise DevRunError('The pipeline cannot run on the engine:\n'
                          + '\n'.join(f'- {p}' for p in error.problems)) from None
    box = Path(export.write(captured, str(folder / 'box'), force=True))
    manifest_path = box / 'service.json'
    manifest = json.loads(manifest_path.read_text())
    notes = for_development_manifest(manifest, user)
    manifest_path.write_text(json.dumps(manifest, indent=2))
    local_run._build_rust_blocks(box)
    data = folder / 'data'
    data.mkdir(parents=True, exist_ok=True)
    port, token = _free_port(), secrets.token_urlsafe(24)
    # A webhook source's senders use a token that can only send messages; the port and that
    # token are kept for `mage examples generate`, readable by their owner only.
    ingest_token = local_run.service_token(folder / 'ingest_token')
    (folder / 'port').write_text(str(port))
    # A variable the blocks need that is not set stops the start, with its name.
    # MAGE_SERVICE_TAP keeps the last messages at each block for the editor's tap.
    env = dict(local_run.engine_environment(box, data, port, token),
               MAGE_SERVICE_STRICT_ENV='on', MAGE_SERVICE_TAP='on',
               MAGE_SERVICE_INGEST_TOKEN=ingest_token)
    local_run._validate(binary, env)
    log_path = folder / 'engine.log'
    log = open(log_path, 'w')
    process = subprocess.Popen([str(binary), 'serve'], env=env, stdout=log,
                               stderr=subprocess.STDOUT)
    log.close()
    run = DevRun(pipeline=pipeline_uuid, port=port, token=token, process=process,
                 log_path=log_path, notes=notes)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not run.alive:
            raise DevRunError('The engine stopped while starting:\n'
                              + '\n'.join(_log_tail(log_path, 40)))
        try:
            run.call('GET', '/healthz')
            break
        except DevRunError:
            time.sleep(0.25)
    else:
        process.terminate()
        raise DevRunError(f'The engine did not start within {int(timeout)} seconds.')
    with _lock:
        _runs[key] = run
    return status(repo_path, pipeline_uuid)


def _run(repo_path: str, pipeline_uuid: str) -> Optional[DevRun]:
    with _lock:
        return _runs.get((str(Path(repo_path).resolve()), pipeline_uuid))


def status(repo_path: str, pipeline_uuid: str) -> Dict:
    run = _run(repo_path, pipeline_uuid)
    if run is None:
        return dict(pipeline_uuid=pipeline_uuid, running=False, state='not_started', logs=[])
    result = dict(pipeline_uuid=pipeline_uuid, running=run.alive, notes=run.notes,
                  started_at=run.started_at, logs=_log_tail(run.log_path))
    if not run.alive:
        result.update(state='stopped' if run.stopped else 'exited',
                      exit_code=run.process.returncode)
        return result
    try:
        result.update(run.call('GET', f'/v1/streams/{pipeline_uuid}'))
        snapshot = run.call('GET', '/v1/snapshot')
        found = next((p for p in snapshot.get('pipelines') or []
                      if p.get('id') == pipeline_uuid), {})
        result['paused'] = found.get('status') == 'paused'
    except DevRunError as error:
        result['error'] = str(error)
    result['pipeline_uuid'] = pipeline_uuid
    return result


def tap(repo_path: str, pipeline_uuid: str, point: Optional[str] = None,
        limit: int = 50) -> Dict:
    """
    The last messages at a tap point of the running stream (`source`, or a block whose output
    the stream sees), with the source's secret fields redacted, and the points there are.
    """
    import urllib.parse

    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.streaming.samples import redact

    run = _run(repo_path, pipeline_uuid)
    if run is None or not run.alive:
        raise DevRunError(f'{pipeline_uuid} is not running on the local engine.')
    query = urllib.parse.urlencode({k: v for k, v in dict(point=point, limit=limit).items()
                                    if v is not None})
    found = run.call('GET', f'/v1/streams/{pipeline_uuid}/tap?{query}')
    pipeline = Pipeline.get(pipeline_uuid, repo_path=repo_path)
    secret_fields = []
    for block in pipeline.blocks_by_uuid.values():
        if str(block.type) == 'data_loader' and \
                str(getattr(block.language, 'value', block.language)) == 'yaml':
            from mage_ai.data_preparation.models.block.streaming import streaming_config

            config = streaming_config(block.content or '', pipeline.variables or {})
            if isinstance(config, dict):
                secret_fields = config.get('secret_fields') or []
    values, redacted = redact([m.get('value') for m in found.get('messages') or []],
                              secret_fields)
    for message, value in zip(found.get('messages') or [], values):
        message['value'] = value
    found['redacted'] = redacted
    return found


def control(repo_path: str, pipeline_uuid: str, action: str) -> Dict:
    if action not in ('pause', 'resume'):
        raise DevRunError('action is pause or resume.')
    run = _run(repo_path, pipeline_uuid)
    if run is None or not run.alive:
        raise DevRunError(f'{pipeline_uuid} is not running on the local engine.')
    snapshot = run.call('GET', '/v1/snapshot')
    found = next((p for p in snapshot.get('pipelines') or []
                  if p.get('id') == pipeline_uuid), None)
    if found is None:
        raise DevRunError(f'The engine has no pipeline {pipeline_uuid}.')
    run.call('POST', '/v1/control', dict(
        pipeline_id=pipeline_uuid, request_id=secrets.token_hex(8),
        expected_config_revision=found['config_revision'], command=dict(kind=action),
    ))
    return status(repo_path, pipeline_uuid)


def stop(repo_path: str, pipeline_uuid: str, timeout: float = 30) -> Dict:
    run = _run(repo_path, pipeline_uuid)
    if run is not None and run.alive:
        run.stopped = True
        run.process.terminate()
        try:
            run.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            run.process.kill()
            run.process.wait()
    return status(repo_path, pipeline_uuid)


@atexit.register
def _stop_all() -> None:
    """The server's dev engines end with it."""
    with _lock:
        runs = list(_runs.values())
    for run in runs:
        if run.alive:
            run.process.terminate()
