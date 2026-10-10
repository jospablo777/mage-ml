"""
Rust blocks: data loaders, transformers, data exporters, custom and conditional blocks
written in Rust.

A block is compiled into its own binary (see build.py) with Mage's `mage` crate, which
calls its function (see sdk/). The binary runs in its own process: the upstream outputs
and variables go to a job directory (see exchange.py), and its outputs and test results
come back from it.
"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from mage_ai.data_preparation.models.block import Block, ConditionalBlock
from mage_ai.data_preparation.models.block.rust import build as rust_build
from mage_ai.data_preparation.models.block.rust import exchange
from mage_ai.data_preparation.models.block.rust import inputs as rust_inputs
from mage_ai.data_preparation.models.block.rust.source import RustSourceError
from mage_ai.data_preparation.models.constants import BlockType
from mage_ai.shared.processes import HAS_PROCESS_GROUPS, stop_process_group

BLOCK_TYPES = (
    BlockType.CUSTOM, BlockType.DATA_EXPORTER, BlockType.DATA_LOADER, BlockType.TRANSFORMER,
)
# Conditional blocks run through RustConditionalBlock.execute_conditional.
EXECUTABLE_BLOCK_TYPES = BLOCK_TYPES + (BlockType.CONDITIONAL,)
JOB_API_VERSION = 1


class RustBlockError(Exception):
    """A Rust block did not compile, failed or panicked; the message says where."""


@dataclass
class RustRun:
    outputs: List[Any]
    tests: List[Dict] = field(default_factory=list)
    build_seconds: float = 0.0
    run_seconds: float = 0.0
    cached_build: bool = False


def _settings_int(name: str) -> Optional[int]:
    value = os.getenv(name)
    return int(value) if value else None


def _process_env() -> Dict[str, str]:
    env = dict(os.environ)
    threads = _settings_int('MAGE_RUST_THREADS')
    if threads:
        # Polars and Rayon size their thread pools from these.
        env['POLARS_MAX_THREADS'] = str(threads)
        env['RAYON_NUM_THREADS'] = str(threads)
    env.setdefault('RUST_BACKTRACE', '0')
    return env


def _run_binary(
    binary: str,
    job_dir: str,
    cwd: str,
    timeout: Optional[int],
    relabel: Callable[[str], str] = lambda line: line,
) -> int:
    """Runs the block, printing its output as it comes; stops it with its children."""
    process = subprocess.Popen(
        [binary, job_dir],
        cwd=cwd,
        env=_process_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace',
        start_new_session=HAS_PROCESS_GROUPS,
    )
    timed_out = threading.Event()
    timer = None
    if timeout:
        def stop():
            timed_out.set()
            stop_process_group(process)

        # A timer, not a check between lines: a block can compute without printing.
        timer = threading.Timer(timeout, stop)
        timer.daemon = True
        timer.start()
    try:
        for line in process.stdout:
            sys.stdout.write(relabel(line))
        code = process.wait()
    except BaseException:
        # An interrupted or failed run leaves no Rust process computing.
        stop_process_group(process)
        raise
    finally:
        if timer is not None:
            timer.cancel()
    if timed_out.is_set():
        raise RustBlockError(f'The Rust block ran longer than {timeout} seconds and was stopped.')
    return code


def _read_json(path: str) -> Optional[Dict]:
    if not os.path.exists(path):
        return None
    with open(path, encoding='utf-8') as file:
        return json.load(file)


def _relabel(text: str, prepared: rust_build.Prepared, job_dir: Optional[str] = None) -> str:
    text = rust_build.relabel(text, prepared)
    if job_dir:
        # Input files are the upstream outputs; their temporary paths mean nothing.
        text = re.sub(
            re.escape(os.path.join(job_dir, 'input_')) + r'(\d+)\.(?:arrow|json)',
            lambda match: f'<upstream output {int(match.group(1)) + 1}>',
            text,
        )
        text = text.replace(job_dir, '<job directory>')
    return text


def execute_rust_code(
    block_type: BlockType,
    code: str,
    input_vars: List = None,
    global_vars: Dict = None,
    repo_path: str = None,
    block_uuid: str = None,
    pipeline_uuid: str = None,
    execution_partition: str = None,
    label: str = None,
) -> RustRun:
    """Builds the code as a block of block_type when it changed, then runs it."""
    if block_type not in EXECUTABLE_BLOCK_TYPES:
        raise RustBlockError(f'Rust blocks cannot be {block_type} blocks.')
    try:
        prepared = rust_build.prepare(code, block_type, block_uuid, repo_path, label=label)
        built = rust_build.build(prepared)
    except (RustSourceError, rust_build.RustBuildError) as error:
        raise RustBlockError(str(error)) from error
    timings = os.getenv('MAGE_RUST_TIMINGS') == '1'
    if not built.cached:
        print(f'Compiled {prepared.label} in {built.seconds:.1f} s.', flush=True)
    for warning in built.warnings:
        print(warning.rendered.rstrip(), flush=True)

    job_dir = tempfile.mkdtemp(prefix='mage_rust_')
    try:
        phase = time.monotonic()
        entries = exchange.write_inputs(input_vars or [], job_dir)
        input_seconds = time.monotonic() - phase
        variables, skipped = exchange.variables_for_rust(global_vars)
        if skipped:
            print(
                f'[Rust block {block_uuid}] Variables {", ".join(skipped)} are not JSON '
                'values and are not passed to the block.',
            )
        with open(os.path.join(job_dir, 'job.json'), 'w', encoding='utf-8') as file:
            json.dump(dict(
                api_version=JOB_API_VERSION,
                block_type=str(block_type),
                block_uuid=block_uuid or '',
                execution_partition=execution_partition,
                inputs=entries,
                output_dir='out',
                pipeline_uuid=pipeline_uuid,
                variables=variables,
            ), file)

        started = time.monotonic()
        cwd = repo_path if repo_path and os.path.isdir(repo_path) else job_dir
        exit_code = _run_binary(
            str(built.binary),
            job_dir,
            cwd,
            _settings_int('MAGE_RUST_TIMEOUT_SECONDS'),
            relabel=lambda line: _relabel(line, prepared, job_dir),
        )
        seconds = time.monotonic() - started
        if exit_code != 0:
            error = _read_json(os.path.join(job_dir, 'error.json')) or {}
            message = _relabel(error.get('message') or '', prepared, job_dir)
            if error.get('kind') == 'panic':
                location = _relabel(error.get('location') or '', prepared)
                where = f' at {location}' if location else ''
                raise RustBlockError(f'Rust block {block_uuid} panicked{where}: {message}')
            raise RustBlockError(
                f'Rust block {block_uuid} failed: '
                + (message or f'the process exited with {exit_code}'),
            )
        result = _read_json(os.path.join(job_dir, 'result.json'))
        if result is None:
            raise RustBlockError(f'Rust block {block_uuid} exited without a result.')
        phase = time.monotonic()
        outputs, tests = exchange.read_outputs(result, job_dir)
        if timings:
            print(
                f'[Rust block {block_uuid}] inputs {input_seconds:.3f} s, process '
                f'{seconds:.3f} s, outputs {time.monotonic() - phase:.3f} s '
                f'({", ".join(entry.get("format", entry["kind"]) for entry in entries)})',
                flush=True,
            )
        if block_type == BlockType.DATA_EXPORTER:
            outputs = []
        return RustRun(
            build_seconds=built.seconds,
            cached_build=built.cached,
            outputs=outputs,
            run_seconds=seconds,
            tests=tests,
        )
    finally:
        shutil.rmtree(job_dir, ignore_errors=True)


def _replayed_test(result: Dict) -> Callable:
    """A test function that passes or fails as the Rust test did."""

    def test(*args, **kwargs):
        if not result.get('passed'):
            raise AssertionError(result.get('message') or 'The Rust test failed.')

    test.__name__ = str(result.get('name') or 'test')
    return test


class RustBlock(Block):
    def _execute_block(
        self,
        outputs_from_input_vars,
        custom_code: str = None,
        execution_partition: str = None,
        global_vars: Dict = None,
        input_vars: List = None,
        **kwargs,
    ) -> List:
        code = custom_code if custom_code is not None and custom_code.strip() else None
        if code is None:
            code = self.content
        if code is None and os.path.exists(self.file_path):
            with open(self.file_path, encoding='utf-8') as file:
                code = file.read()
        label = None
        if self.file_path and self.repo_path:
            label = os.path.relpath(self.file_path, self.repo_path)
        run = execute_rust_code(
            self.type,
            code or '',
            block_uuid=self.uuid,
            execution_partition=execution_partition,
            global_vars=global_vars,
            input_vars=input_vars,
            label=label,
            pipeline_uuid=self.pipeline_uuid,
            repo_path=self.repo_path,
        )
        # The tests ran in Rust with the output; run_tests reports them as Mage reports
        # the tests of Python blocks, after the output is stored.
        self.test_functions = [_replayed_test(result) for result in run.tests]
        return run.outputs

    def fetch_input_variables(
        self,
        input_args,
        block_run_outputs_cache: Dict[str, List] = None,
        dynamic_block_index: int = None,
        dynamic_upstream_block_uuids: List[str] = None,
        execution_partition: str = None,
        upstream_block_uuids: List[str] = None,
        upstream_block_uuids_override: List[str] = None,
        **kwargs,
    ):
        # Stored upstream tables go to the block by path; inputs given in memory (fused
        # stages, outputs cached in memory, explicit input_args) are passed as given.
        uuids = upstream_block_uuids or self.upstream_block_uuids
        if (
            not input_args
            and not block_run_outputs_cache
            and dynamic_block_index is None
            and not dynamic_upstream_block_uuids
            and not upstream_block_uuids_override
        ):
            tables = rust_inputs.stored_tables(self, uuids, execution_partition)
            if tables is not None:
                return tables, [], uuids
        return super().fetch_input_variables(
            input_args,
            block_run_outputs_cache=block_run_outputs_cache,
            dynamic_block_index=dynamic_block_index,
            dynamic_upstream_block_uuids=dynamic_upstream_block_uuids,
            execution_partition=execution_partition,
            upstream_block_uuids=upstream_block_uuids,
            upstream_block_uuids_override=upstream_block_uuids_override,
            **kwargs,
        )

    def run_tests(self, *args, **kwargs) -> None:
        # The tests are Rust functions; updating them would run the block's code as Python.
        kwargs['update_tests'] = False
        return super().run_tests(*args, **kwargs)


class RustConditionalBlock(ConditionalBlock):
    """A condition written in Rust: `fn condition(...) -> Result<bool>`."""

    def execute_conditional(
        self,
        parent_block: Block,
        dynamic_block_index: Optional[int] = None,
        dynamic_upstream_block_uuids: Optional[List[str]] = None,
        execution_partition: Optional[str] = None,
        global_vars: Optional[Dict] = None,
        logger=None,
        logging_tags: Optional[Dict] = None,
        **kwargs,
    ) -> bool:
        with self._redirect_streams(logger=logger, logging_tags=logging_tags):
            global_vars = self._create_global_vars(
                global_vars,
                parent_block,
                dynamic_block_index=dynamic_block_index,
                **kwargs,
            )
            variables = global_vars.copy()
            input_vars = []
            if parent_block is not None:
                input_vars, kwargs_vars, _ = parent_block.fetch_input_variables(
                    None,
                    execution_partition=execution_partition,
                    global_vars=global_vars,
                    dynamic_block_index=dynamic_block_index,
                    dynamic_upstream_block_uuids=dynamic_upstream_block_uuids,
                )
                for kwargs_var in kwargs_vars:
                    variables.update(kwargs_var)
            label = None
            if self.file_path and self.repo_path:
                label = os.path.relpath(self.file_path, self.repo_path)
            run = execute_rust_code(
                BlockType.CONDITIONAL,
                self.content or '',
                block_uuid=self.uuid,
                execution_partition=execution_partition,
                global_vars=variables,
                input_vars=input_vars,
                label=label,
                pipeline_uuid=self.pipeline_uuid,
                repo_path=self.repo_path,
            )
            decisions = [value for value in run.outputs if isinstance(value, bool)]
            if not decisions:
                raise RustBlockError(
                    f'The condition {self.uuid} returned no decision; it must return bool.',
                )
            return all(decisions)
