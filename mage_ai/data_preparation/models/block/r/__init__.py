"""
R blocks: data loaders, transformers and data exporters written in R.

A block runs in its own Rscript process, with the library of the project's rv
environment (see runtime.py) and the mageml R package, which runs it (see mageml/). The
upstream outputs and the pipeline's variables are written to a job directory, as
described in exchange.py, and the block's return value and test results are read back
from it.
"""
import json
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.block.r import exchange, runtime
from mage_ai.data_preparation.models.constants import BlockType

RUNNER = Path(__file__).parent / 'runner.R'
BLOCK_TYPES = (
    BlockType.DATA_LOADER, BlockType.TRANSFORMER, BlockType.DATA_EXPORTER, BlockType.CUSTOM,
)


class RBlockError(Exception):
    """An R block failed. The message holds R's error and the block's calls."""


@dataclass
class RRun:
    # The block's return value, in a list, or an empty list when it returned NULL.
    outputs: List[Any]
    # The name, whether it passed and the error message of each of the block's tests.
    tests: List[Dict] = field(default_factory=list)


def _read_text(path: str) -> str:
    if not os.path.exists(path):
        return ''
    with open(path, encoding='utf-8', errors='replace') as file:
        return file.read().strip()


def _write_json(job_dir: str, name: str, value: Any, private: bool = False) -> None:
    path = os.path.join(job_dir, name)
    # Settings with passwords are readable by the owner only; the job directory is
    # removed after the run.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600 if private else 0o644)
    with os.fdopen(descriptor, 'w', encoding='utf-8') as file:
        json.dump(value, file, ensure_ascii=False, default=str)


def execute_r_code(
    block_type: BlockType,
    code: str,
    input_vars: List = None,
    global_vars: Dict = None,
    repo_path: str = None,
    block_uuid: str = None,
    pipeline_uuid: str = None,
    execution_partition: str = None,
) -> RRun:
    """Run R code as a block of block_type."""
    if block_type not in BLOCK_TYPES:
        raise RBlockError(f'R blocks cannot be {block_type} blocks.')
    config = runtime.r_config(repo_path)
    libraries = runtime.prepare(config)

    job_dir = tempfile.mkdtemp(prefix='mage_r_')
    try:
        entries, warnings = exchange.write_inputs(input_vars or [], job_dir)
        values, skipped = exchange.globals_for_r(global_vars)
        if skipped:
            warnings.append(
                f'Variables {", ".join(skipped)} cannot be written as JSON and are not in '
                'global_vars.',
            )
        for warning in warnings:
            print(f'[R block {block_uuid}] {warning}')
        # An empty dict, not a list, when there are no variables.
        _write_json(job_dir, 'globals.json', values or {})
        _write_json(job_dir, 'manifest.json', dict(
            block_type=str(block_type),
            block_uuid=block_uuid,
            pipeline_uuid=pipeline_uuid,
            execution_partition=execution_partition,
            inputs=entries,
        ))
        with open(os.path.join(job_dir, 'block.R'), 'w', encoding='utf-8') as file:
            file.write(code)
        databases = exchange.database_settings(code, repo_path)
        if databases:
            _write_json(job_dir, 'io_config.json', databases, private=True)

        env = dict(os.environ)
        env.pop('MAGE_R_LIBRARY', None)
        if libraries.rv is not None:
            env['MAGE_R_LIBRARY'] = str(libraries.rv)
        env['MAGE_R_MAGEML_LIBRARY'] = str(libraries.mageml)
        # R reads naive timestamps as UTC and shows them in the session's time zone.
        env['TZ'] = os.getenv('MAGE_R_TZ') or 'UTC'
        cwd = repo_path if repo_path and os.path.isdir(repo_path) else job_dir
        exit_code = runtime.run_rscript(config, RUNNER, [job_dir], env=env, cwd=cwd)
        if exit_code != 0:
            message = _read_text(os.path.join(job_dir, 'error.txt'))
            calls = _read_text(os.path.join(job_dir, 'traceback.txt'))
            text = f'R block {block_uuid} failed: {message or f"Rscript exited with {exit_code}"}'
            if calls:
                text += f'\n\nR calls:\n{calls}'
            raise RBlockError(text)

        tests_path = os.path.join(job_dir, 'tests.json')
        tests = []
        if os.path.exists(tests_path):
            with open(tests_path, encoding='utf-8') as file:
                tests = json.load(file) or []
        if block_type == BlockType.DATA_EXPORTER:
            return RRun(outputs=[], tests=tests)
        returned, value = exchange.read_output(job_dir)
        return RRun(outputs=[value] if returned else [], tests=tests)
    finally:
        shutil.rmtree(job_dir, ignore_errors=True)


def _replayed_test(result: Dict) -> Callable:
    """A test function that passes or fails as the R test did."""

    def test(*args, **kwargs):
        if not result.get('passed'):
            raise AssertionError(result.get('message') or 'The R test failed.')

    test.__name__ = str(result.get('name') or 'test')
    return test


class RBlock(Block):
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
        run = execute_r_code(
            self.type,
            code or '',
            input_vars=input_vars,
            global_vars=global_vars,
            repo_path=self.repo_path,
            block_uuid=self.uuid,
            pipeline_uuid=self.pipeline_uuid,
            execution_partition=execution_partition,
        )
        # The tests ran in R with the output; run_tests reports them as Mage reports the
        # tests of Python blocks, after the output is stored.
        self.test_functions = [_replayed_test(result) for result in run.tests]
        return run.outputs

    def run_tests(self, *args, **kwargs) -> None:
        # The tests are R functions; updating them would run the block's code as Python.
        kwargs['update_tests'] = False
        return super().run_tests(*args, **kwargs)
