"""
Compiled chains of Rust and R blocks in a pipeline run.

In a fused stage, a Rust or R block whose next block in the stage is a block of the same
language returns a Plan instead of running: its code and the plan of the blocks before
it. The next block extends the plan, and the first block whose output is needed runs the
whole chain once: Rust as one process whose blocks' lazy queries Polars optimizes as one
(mage::run_chain), R in one R session (mageml::run_chain).

A block's output is needed, so it runs the chain, when it is the last block of the
chain, an exporter, or has tests, monitors or a data contract, or when the pipeline keeps
every block's output (keep_block_outputs). A planned block keeps its block run, logs and
status; its stored output is the plan, as JSON, which any later reader can run.
"""
import contextvars
import os
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional

PLAN_KEY = '__mage_plan__'

# The next block of the stage when it is a block of the same language, set by the stage.
_next_in_stage: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    'mage_next_in_stage', default=None,
)
# The pipeline run the stage runs, to mark a planned block run that failed in the chain.
_pipeline_run_id: contextvars.ContextVar[Optional[int]] = contextvars.ContextVar(
    'mage_plan_pipeline_run', default=None,
)


@contextmanager
def stage_context(next_block_uuid: Optional[str], pipeline_run_id: Optional[int]) -> Iterator:
    """While a stage runs a block: the next block of the stage of the same language."""
    tokens = (_next_in_stage.set(next_block_uuid), _pipeline_run_id.set(pipeline_run_id))
    try:
        yield
    finally:
        _next_in_stage.reset(tokens[0])
        _pipeline_run_id.reset(tokens[1])


class Plan(dict):
    """
    Blocks of one language whose run waits for the next one. The dict holds what is
    stored (JSON); `inputs` holds the first block's inputs in memory.
    """

    def __init__(
        self,
        language: str,
        steps: List[Dict],
        inputs: List[Any],
        upstream_uuids: List[str],
        global_vars: Optional[Dict] = None,
    ):
        super().__init__({PLAN_KEY: dict(
            language=language,
            steps=steps,
            upstream_uuids=upstream_uuids,
            note=(
                f'Planned in a compiled chain of {language} blocks: the block that needs '
                'this result computes it with the whole chain. Run the block in the '
                'editor to see its output.'
            ),
        )})
        self.inputs = list(inputs or [])
        self.global_vars = global_vars or {}

    @property
    def language(self) -> str:
        return self[PLAN_KEY]['language']

    @property
    def steps(self) -> List[Dict]:
        return self[PLAN_KEY]['steps']

    def extend(self, step: Dict) -> 'Plan':
        plan = Plan(self.language, self.steps + [step], self.inputs,
                    self[PLAN_KEY]['upstream_uuids'], self.global_vars)
        return plan


def is_plan(value: Any) -> bool:
    return isinstance(value, dict) and PLAN_KEY in value and len(value) == 1


def upstream_plan(input_vars: List, language: str) -> Optional[Plan]:
    """The plan a block extends: its only input, a plan of its language, in memory."""
    if len(input_vars or []) == 1 and isinstance(input_vars[0], Plan) and \
            input_vars[0].language == language:
        return input_vars[0]
    return None


def may_plan(block, step: Dict) -> bool:
    """
    Whether the block returns a plan instead of running: in a stage whose next block is
    of the same language, when nothing needs this block's own output.
    """
    from mage_ai.data_preparation.models.constants import BlockType

    following = _next_in_stage.get()
    if not following or os.getenv('MAGE_COMPILED_CHAINS') == 'off':
        return False
    downstream = block.downstream_blocks or []
    if len(downstream) != 1 or downstream[0].uuid != following:
        return False
    if block.type in (BlockType.DATA_EXPORTER, BlockType.CONDITIONAL):
        return False
    pipeline = block.pipeline
    if pipeline is not None and getattr(pipeline, 'keep_block_outputs', False):
        return False
    if step.get('tests'):
        return False
    try:
        from mage_ai.data_preparation import contracts

        if contracts.binding_of(block) is not None:
            return False
    except Exception:
        return False
    try:
        from mage_ai.orchestration.monitors import monitors_for

        if pipeline is not None and monitors_for(block.repo_path, pipeline.uuid, block.uuid):
            return False
    except Exception:
        return False
    return True


def mark_failed(block_uuid: str, message: str) -> None:
    """Marks the block run of a planned block that failed when a later block ran the
    chain; the block that ran it fails with an error that names this one."""
    pipeline_run_id = _pipeline_run_id.get()
    if pipeline_run_id is None:
        return
    try:
        from mage_ai.orchestration.db.models.schedules import BlockRun

        for block_run in BlockRun.query.filter(
            BlockRun.pipeline_run_id == pipeline_run_id,
            BlockRun.block_uuid == block_uuid,
        ).all():
            # The shape the scheduler stores for a failed block run (on_block_failure).
            metrics = dict(block_run.metrics or {}, error=dict(
                error=message[:4000], errors=[message[:4000]], message=message[:4000],
            ))
            block_run.update(status=BlockRun.BlockRunStatus.FAILED, metrics=metrics)
    except Exception:
        pass


def resolve(value: Any, block, execution_partition: Optional[str] = None) -> Any:
    """
    A stored plan (read from storage, not passed in memory) run to its value, for a reader
    outside the chain: a retry, a later stage, the editor.
    """
    if not is_plan(value) or isinstance(value, Plan):
        return value
    stored = value[PLAN_KEY]
    pipeline = block.pipeline
    inputs = []
    for uuid in stored.get('upstream_uuids') or []:
        upstream = pipeline.get_block(uuid)
        variable = pipeline.variable_manager.get_variable(
            pipeline.uuid, uuid, 'output_0', partition=execution_partition,
        )
        inputs.append(resolve(variable, upstream, execution_partition))
    plan = Plan(stored['language'], stored['steps'], inputs, stored.get('upstream_uuids') or [])
    outputs = run(plan, block, execution_partition).outputs
    return outputs[0] if outputs else None


def run(plan: Plan, block, execution_partition: Optional[str] = None):
    """Runs a plan's chain; returns the run: the last block's outputs and the tests."""
    from mage_ai.data_preparation.models.constants import BlockType

    steps = plan.steps
    if plan.language == 'rust':
        from mage_ai.data_preparation.models.block import rust
        from mage_ai.data_preparation.models.block.rust import build as rust_build

        members = [
            rust_build.ChainMember(code=s['code'], block_type=BlockType(s['block_type']),
                                   block_uuid=s['block_uuid'])
            for s in steps
        ]
        try:
            run_ = rust.execute_rust_chain(
                members,
                input_vars=plan.inputs,
                global_vars=plan.global_vars,
                repo_path=block.repo_path,
                pipeline_uuid=block.pipeline_uuid,
                execution_partition=execution_partition,
                io_config=rust.block_io_config(
                    block.pipeline.get_block(steps[-1]['block_uuid']) or block,
                ),
            )
        except rust.RustChainError as error:
            raise _chain_failure(error.block_uuid, str(error), steps) from error
        return run_
    from mage_ai.data_preparation.models.block.r import execution

    try:
        run_ = execution.execute_r_chain(
            [dict(block_type=s['block_type'], code=s['code'], block_uuid=s['block_uuid'])
             for s in steps],
            input_vars=plan.inputs,
            global_vars=plan.global_vars,
            repo_path=block.repo_path,
            pipeline_uuid=block.pipeline_uuid,
            execution_partition=execution_partition,
        )
    except execution.RChainError as error:
        raise _chain_failure(error.block_uuid, str(error), steps) from error
    return run_


class ChainFailure(Exception):
    """A block of a compiled chain failed; block_uuid names it."""

    def __init__(self, message: str, block_uuid: Optional[str]):
        super().__init__(message)
        self.block_uuid = block_uuid


def _chain_failure(block_uuid: Optional[str], message: str, steps: List[Dict]) -> ChainFailure:
    last = steps[-1]['block_uuid']
    if block_uuid and block_uuid != last:
        mark_failed(block_uuid, message)
        chain = ', '.join(s['block_uuid'] for s in steps)
        message = (
            f'Block {block_uuid} failed when {last} ran the compiled chain {chain}:\n\n'
            f'{message}'
        )
    return ChainFailure(message, block_uuid)
