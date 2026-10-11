"""
R blocks: data loaders, transformers and data exporters written in R.

A block runs in its own Rscript process, with the library of the project's rv
environment (see runtime.py) and the mageml R package, which runs it (see mageml/). The
upstream outputs and the pipeline's variables are written to a job directory, as
described in exchange.py, and the block's return value and test results are read back
from it.
"""
import os
from typing import Callable, Dict, List, Optional

from mage_ai.data_preparation.models.block import Block, ConditionalBlock
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
        from mage_ai.data_preparation.models.block import plans

        step = dict(block_uuid=self.uuid, block_type=str(self.type), code=code or '',
                    tests=['test'] if '@test' in (code or '') else [])
        upstream = plans.upstream_plan(input_vars, 'r')
        if upstream is not None:
            plan = upstream.extend(step)
        else:
            plan = plans.Plan('r', [step], [plans.resolve(v, self, execution_partition, global_vars)
                                            for v in (input_vars or [])],
                              list(self.upstream_block_uuids or []), global_vars)
        if plans.may_plan(self, step):
            self.test_functions = []
            return [plan]
        if len(plan.steps) > 1:
            run = plans.run(plan, self, execution_partition)
            prefix = f'{self.uuid}: '
            self.test_functions = [
                _replayed_test(dict(r, name=str(r.get('name', '')).removeprefix(prefix)))
                for r in run.tests if str(r.get('name', '')).startswith(prefix)
            ]
            return run.outputs
        input_vars = plan.inputs
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


class RConditionalBlock(ConditionalBlock):
    """
    A condition written in R: a function annotated `#* @condition` that receives the
    inputs of the block it guards and returns TRUE (run it) or FALSE (skip it).
    """

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
        from mage_ai.data_preparation.models.constants import BlockType

        with self._redirect_streams(logger=logger, logging_tags=logging_tags):
            variables = self._create_global_vars(
                global_vars,
                parent_block,
                dynamic_block_index=dynamic_block_index,
                **kwargs,
            ).copy()
            input_vars = []
            if parent_block is not None:
                input_vars, kwargs_vars, _ = parent_block.fetch_input_variables(
                    None,
                    execution_partition=execution_partition,
                    global_vars=variables,
                    dynamic_block_index=dynamic_block_index,
                    dynamic_upstream_block_uuids=dynamic_upstream_block_uuids,
                )
                for kwargs_var in kwargs_vars:
                    variables.update(kwargs_var)
            run = execute_r_code(
                BlockType.CONDITIONAL,
                self.content or '',
                input_vars=input_vars,
                global_vars=variables,
                repo_path=self.repo_path,
                block_uuid=self.uuid,
                pipeline_uuid=self.pipeline_uuid,
                execution_partition=execution_partition,
            )
            decisions = [value for value in run.outputs if isinstance(value, bool)]
            if not decisions:
                raise RBlockError(
                    f'The condition {self.uuid} returned no decision; it must return '
                    'TRUE or FALSE.',
                )
            return all(decisions)
