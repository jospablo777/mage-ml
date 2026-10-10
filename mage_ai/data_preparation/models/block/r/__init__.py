"""
R blocks: data loaders, transformers and data exporters written in R.

A block runs in its own Rscript process, with the library of the project's rv
environment (see runtime.py) and the mageml R package, which runs it (see mageml/). The
upstream outputs and the pipeline's variables are written to a job directory, as
described in exchange.py, and the block's return value and test results are read back
from it.
"""
import os
from typing import Callable, Dict, List

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.block.r import exchange, runtime  # noqa: F401
from mage_ai.data_preparation.models.block.r.execution import (  # noqa: F401
    BLOCK_TYPES,
    RUNNER,
    RBlockError,
    RRun,
    execute_r_code,
)


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
