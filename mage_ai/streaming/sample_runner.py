"""
Runs a streaming pipeline's blocks on sample messages for the editor, with the same Python
worker and `stream` request the service engine uses, so the editor runs what production runs.
Rust blocks run in their own programs over the engine's protocol (rust_sample).

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


def _language_runs(pipeline, block_uuid: str) -> Optional[List[List]]:
    """
    The blocks from the source down to block_uuid in runs of one language, when any is Rust
    (they form one chain, as services run them); None when they are all Python.
    """
    blocks = pipeline.blocks_by_uuid
    if block_uuid not in blocks:
        raise SampleRunError(f'The pipeline has no block {block_uuid}.')
    chain = []
    block = blocks[block_uuid]
    while block is not None and str(getattr(block.type, 'value', block.type)) == 'transformer':
        chain.insert(0, block)
        parents = block.upstream_block_uuids or []
        if len(parents) > 1:
            break
        block = blocks.get(parents[0]) if parents else None
    languages = [str(getattr(b.language, 'value', b.language)) for b in chain]
    if 'rust' not in languages:
        return None
    unknown = sorted(set(languages) - {'python', 'rust'})
    if unknown:
        raise SampleRunError(f'Stream blocks run in Python or Rust, not {unknown[0]}.')
    runs: List[List] = []
    for block, language in zip(chain, languages):
        if runs and str(getattr(runs[-1][0].language, 'value', runs[-1][0].language)) == language:
            runs[-1].append(block)
        else:
            runs.append([block])
    return runs


def _chain_entry(pipeline, block, upstream: List[str]) -> Dict:
    path = Path(block.file_path)
    return dict(
        uuid=block.uuid,
        type='transformer',
        language='python',
        file=str(path),
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        configuration=block.configuration or {},
        upstream=upstream,
        pipeline_uuid=pipeline.uuid,
    )


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
                f'{uuid_} is written in {language}; stream blocks run in Python or Rust.'
            )
        chain.append(_chain_entry(
            pipeline, block, [u for u in block.upstream_block_uuids or [] if u not in sources],
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
    runs = _language_runs(pipeline, block_uuid)
    if runs is not None:
        return _run_language_runs(repo_path, pipeline, block_uuid, runs, messages, variables)
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


def _run_rust(repo_path: str, pipeline, block_uuid: str, chain: List, messages: List[Any],
              variables: Optional[Dict]) -> Dict:
    """Each block's program runs on the messages of the block before it."""
    from mage_ai.streaming import rust_sample

    kwargs = dict(pipeline.variables or {})
    kwargs.update(variables or {})
    started = time.monotonic()
    result = dict(block_uuid=block_uuid, input_messages=len(messages), logs=[])
    outputs: Dict[str, List] = {}
    timings = []
    current = [rust_sample.encode(m) for m in messages]
    for block in chain:
        try:
            binary = rust_sample.build_stream_program(block, repo_path)
            program = rust_sample.RustStreamProgram(
                binary, block.uuid, pipeline.uuid, kwargs, repo_path,
                rust_sample.program_env(block), RUN_TIMEOUT_SECONDS,
            )
        except Exception as error:
            result['error'] = dict(block_uuid=block.uuid, message=str(error), details=None)
            break
        try:
            reply = program.run(current)
            result['logs'] += _logs(program.take_lines())
            if not reply.get('ok'):
                error = reply.get('error') or {}
                result['error'] = dict(block_uuid=block.uuid, message=error.get('message'),
                                       details=None)
                result['failing_message'] = _first_failing_rust(program, current)
                break
            timings += reply.get('blocks') or []
            current = reply['messages']
            outputs[block.uuid] = [rust_sample.decode(m) for m in current]
        except rust_sample.RustSampleError as error:
            result['logs'] += _logs(program.take_lines())
            result['error'] = dict(block_uuid=block.uuid, message=str(error), details=None)
            break
        finally:
            program.close()
    result['seconds'] = round(time.monotonic() - started, 3)
    if 'error' not in result:
        result.update(outputs=outputs, blocks=timings)
    return result


def _first_failing_rust(program, batch: List[bytes]) -> Optional[Dict]:
    """
    The first message of the batch that fails in the block on its own, with its index; the
    batch is the block's input, which for the first block is the sample's messages.
    """
    from mage_ai.streaming import rust_sample

    for index, message in enumerate(batch[:FIND_FAILING_LIMIT]):
        try:
            reply = program.run([message])
        except rust_sample.RustSampleError:
            return None
        program.take_lines()
        if not reply.get('ok'):
            return dict(index=index, message=rust_sample.decode(message))
    return None


def _run_language_runs(repo_path: str, pipeline, block_uuid: str, runs: List[List],
                       messages: List[Any], variables: Optional[Dict]) -> Dict:
    """Each run of Python or Rust blocks takes the messages of the run before it."""
    started = time.monotonic()
    kwargs = dict(pipeline.variables or {})
    kwargs.update(variables or {})
    result: Dict = dict(block_uuid=block_uuid, input_messages=len(messages), logs=[],
                        outputs={}, blocks=[])
    current = messages
    for run in runs:
        language = str(getattr(run[0].language, 'value', run[0].language))
        if language == 'rust':
            part = _run_rust(repo_path, pipeline, run[-1].uuid, run, current, variables)
        else:
            chain = [_chain_entry(pipeline, block, [run[i - 1].uuid] if i else [])
                     for i, block in enumerate(run)]
            request = dict(op='stream', chain=chain, messages=current,
                           outputs=[b['uuid'] for b in chain], kwargs=kwargs)
            worker = _worker(repo_path)
            reply = worker.request(request, RUN_TIMEOUT_SECONDS)
            part = dict(logs=_logs(reply.get('output') or []))
            if reply.get('ok'):
                part.update(outputs=reply.get('outputs') or {}, blocks=reply.get('blocks') or [])
            else:
                error = reply.get('error') or {}
                part['error'] = dict(block_uuid=error.get('block_uuid'),
                                     message=error.get('message'), details=error.get('details'))
                part['failing_message'] = _first_failing(worker, request,
                                                         error.get('block_uuid'))
        result['logs'] += part.get('logs') or []
        if 'error' in part:
            result['error'] = part['error']
            if part.get('failing_message'):
                result['failing_message'] = part['failing_message']
            break
        result['outputs'].update(part['outputs'])
        result['blocks'] += part.get('blocks') or []
        current = part['outputs'].get(run[-1].uuid, [])
    result['seconds'] = round(time.monotonic() - started, 3)
    if 'error' in result:
        result.pop('outputs')
        result.pop('blocks')
    return result
