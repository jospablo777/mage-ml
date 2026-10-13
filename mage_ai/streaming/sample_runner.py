"""
Runs a streaming pipeline's blocks on sample messages for the editor, with the same Python
worker and `stream` request the service engine uses, so the editor runs what production runs.

The worker is a child process of the server (a block cannot crash or block the server) kept
warm per project: a block's top-level code, such as loading a model, runs again only when its
file changes. Requests run one at a time per worker.
"""
import atexit
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

WORKER = Path(__file__).resolve().parents[1] / 'pipeline_services' / 'runtime' / 'worker.py'
BLOCK_MARKER = '\x1emage-block '
RUN_TIMEOUT_SECONDS = 120
# The first message that fails is looked for among this many, one at a time.
FIND_FAILING_LIMIT = 500


class SampleRunError(Exception):
    pass


class _Worker:
    def __init__(self, repo_path: str):
        self.repo_path = repo_path
        reply_read, reply_write = os.pipe()
        env = dict(os.environ)
        env['MAGE_REPO_PATH'] = repo_path
        env['MAGE_WORKER_REPLY_FD'] = str(reply_write)
        env.setdefault('PYTHONUNBUFFERED', '1')
        self.process = subprocess.Popen(
            [sys.executable, str(WORKER)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            pass_fds=(reply_write,),
            env=env,
            cwd=repo_path,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        os.close(reply_write)
        self.replies = os.fdopen(reply_read, 'r', encoding='utf-8')
        self.lock = threading.Lock()
        self.lines: List[str] = []
        self.lines_lock = threading.Lock()
        threading.Thread(target=self._read_output, daemon=True).start()
        ready = self._read_reply(30)
        if ready.get('type') != 'ready':
            raise SampleRunError(f'The Python worker did not start: {ready}')

    def _read_output(self) -> None:
        for line in self.process.stdout:
            with self.lines_lock:
                self.lines.append(line.rstrip('\n'))

    def _read_reply(self, timeout: float) -> Dict:
        result: Dict = {}

        def read():
            line = self.replies.readline()
            if line:
                result.update(json.loads(line))

        reader = threading.Thread(target=read, daemon=True)
        reader.start()
        reader.join(timeout)
        if reader.is_alive():
            self.close()
            raise SampleRunError(
                f'The blocks ran past {int(timeout)} seconds; the worker was stopped.'
            )
        if not result:
            output = '\n'.join(self.take_lines()[-30:])
            self.close()
            raise SampleRunError(f'The Python worker stopped.\n{output}'.rstrip())
        return result

    def take_lines(self) -> List[str]:
        with self.lines_lock:
            lines, self.lines = self.lines, []
        return lines

    def request(self, payload: Dict, timeout: float) -> Dict:
        with self.lock:
            self.take_lines()
            payload = dict(payload, id=str(uuid.uuid4()))
            self.process.stdin.write(json.dumps(payload) + '\n')
            self.process.stdin.flush()
            reply = self._read_reply(timeout)
            # What the blocks printed arrives on another pipe; let the last lines land.
            time.sleep(0.05)
            reply['output'] = self.take_lines()
            return reply

    def alive(self) -> bool:
        return self.process.poll() is None

    def close(self) -> None:
        if self.process.poll() is None:
            try:
                os.killpg(self.process.pid, 9)
            except OSError:
                self.process.kill()
        self.process.wait(timeout=10)


_workers: Dict[str, _Worker] = {}
_workers_lock = threading.Lock()


def _worker(repo_path: str) -> _Worker:
    with _workers_lock:
        worker = _workers.get(repo_path)
        if worker is None or not worker.alive():
            worker = _Worker(repo_path)
            _workers[repo_path] = worker
        return worker


@atexit.register
def stop_workers() -> None:
    with _workers_lock:
        for worker in _workers.values():
            worker.close()
        _workers.clear()


def _chain(pipeline, block_uuid: str) -> List[Dict]:
    """The stream's transformers from the source down to block_uuid, in run order."""
    blocks = pipeline.blocks_by_uuid
    if block_uuid not in blocks:
        raise SampleRunError(f'The pipeline has no block {block_uuid}.')
    sources = {u for u, b in blocks.items() if str(getattr(b.type, 'value', b.type)) ==
               'data_loader'}
    wanted: List[str] = []

    def visit(uuid_: str) -> None:
        if uuid_ in wanted or uuid_ in sources:
            return
        block = blocks[uuid_]
        for parent in block.upstream_block_uuids or []:
            visit(parent)
        wanted.append(uuid_)

    visit(block_uuid)
    chain = []
    for uuid_ in wanted:
        block = blocks[uuid_]
        kind = str(getattr(block.type, 'value', block.type))
        language = str(getattr(block.language, 'value', block.language))
        if kind != 'transformer':
            raise SampleRunError(f'{uuid_} is a {kind} block; samples run through transformers.')
        if language != 'python':
            raise SampleRunError(
                f'{uuid_} is written in {language}; stream blocks run in Python now, and Rust '
                'and R come next.'
            )
        path = Path(block.file_path)
        chain.append(dict(
            uuid=uuid_,
            type='transformer',
            language='python',
            file=str(path),
            sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            configuration=block.configuration or {},
            upstream=[u for u in block.upstream_block_uuids or [] if u not in sources],
            pipeline_uuid=pipeline.uuid,
        ))
    return chain


def _logs(lines: List[str]) -> List[Dict]:
    """What the blocks printed, by block (the worker marks where each block starts)."""
    logs: List[Dict] = []
    block = None
    for line in lines:
        if line.startswith(BLOCK_MARKER):
            block = line[len(BLOCK_MARKER):].strip()
            continue
        logs.append(dict(block_uuid=block, line=line))
    return logs


def run_on_sample(
    repo_path: str,
    pipeline_uuid: str,
    block_uuid: str,
    messages: List[Any],
    variables: Optional[Dict] = None,
) -> Dict:
    """
    Runs the blocks from the source down to block_uuid on the messages. Returns each block's
    output messages (`outputs`), its time (`blocks`), what the blocks printed (`logs`), and
    on failure the error with the first message that fails on its own (`failing_message`).
    """
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.get(pipeline_uuid, repo_path=repo_path)
    chain = _chain(pipeline, block_uuid)
    kwargs = dict(pipeline.variables or {})
    kwargs.update(variables or {})
    request = dict(
        op='stream', chain=chain, messages=messages,
        outputs=[b['uuid'] for b in chain], kwargs=kwargs,
    )
    started = time.monotonic()
    worker = _worker(repo_path)
    reply = worker.request(request, RUN_TIMEOUT_SECONDS)
    result = dict(
        block_uuid=block_uuid,
        input_messages=len(messages),
        logs=_logs(reply.get('output') or []),
        seconds=round(time.monotonic() - started, 3),
    )
    if reply.get('ok'):
        result.update(outputs=reply.get('outputs') or {}, blocks=reply.get('blocks') or [])
        return result
    error = reply.get('error') or {}
    result['error'] = dict(
        block_uuid=error.get('block_uuid'),
        message=error.get('message'),
        details=error.get('details'),
    )
    result['failing_message'] = _first_failing(worker, request, error.get('block_uuid'))
    return result


def _first_failing(worker: _Worker, request: Dict, block_uuid: Optional[str]) -> Optional[Dict]:
    """The first message that fails in the same block on its own, with its index."""
    for index, message in enumerate(request['messages'][:FIND_FAILING_LIMIT]):
        reply = worker.request(dict(request, messages=[message]), RUN_TIMEOUT_SECONDS)
        if not reply.get('ok') and (reply.get('error') or {}).get('block_uuid') == block_uuid:
            return dict(index=index, message=message)
    return None
