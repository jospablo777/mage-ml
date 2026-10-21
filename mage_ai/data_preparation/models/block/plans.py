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

DuckDB SQL blocks chain too: a planned block whose table is replaced on each run
(export_write_policy replace) becomes a view of its query instead of a table, and the
block that needs its output writes its table from a query DuckDB plans across the views,
as one statement. A block that appends keeps its table, as the rows it accumulates are
what the blocks after it read.
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


VIEW_KEY = '__mage_view__'


def chains_with(block, following) -> bool:
    """
    Whether the block may plan for the next block of its stage: both Rust, both R, or a
    DuckDB SQL block that replaces its table followed by a DuckDB SQL block of its profile.
    """
    from mage_ai.data_preparation.models.constants import BlockLanguage

    if block is None or following is None:
        return False
    if block.language in (BlockLanguage.RUST, BlockLanguage.R):
        return following.language == block.language
    if block.language == BlockLanguage.SQL and following.language == BlockLanguage.SQL:
        return sql_view_problem(block) is None and _duckdb(following) and (
            (block.configuration or {}).get('data_provider_profile')
            == (following.configuration or {}).get('data_provider_profile')
        )
    return False


def _duckdb(block) -> bool:
    return (block.configuration or {}).get('data_provider') == 'duckdb'


def sql_view_problem(block) -> Optional[str]:
    """Why a SQL block cannot become a view in a compiled chain; None when it can."""
    from mage_ai.data_preparation.models.block.dynamic.utils import (
        is_dynamic_block,
        is_dynamic_block_child,
        is_replicated_block,
    )

    configuration = block.configuration or {}
    if not _duckdb(block):
        return 'For now, compiling SQL block chains supports only DuckDB.'
    if configuration.get('use_raw_sql'):
        return 'It runs raw SQL, which can hold several statements.'
    if str(configuration.get('export_write_policy') or 'append') != 'replace':
        return ('It appends to its table on each run, and the blocks after it read the rows '
                'it accumulated; set its write policy to replace to compile it into the chain.')
    if is_dynamic_block(block) or is_dynamic_block_child(block) or is_replicated_block(block):
        return 'It is dynamic or replicated.'
    return None


def view_output(block, schema: str, name: str) -> Dict:
    """What a SQL block that became a view stores as its output."""
    return {VIEW_KEY: dict(
        schema=schema,
        view=name,
        note=(f'Compiled into a view, {schema}.{name}: the block that needs its rows reads '
              'them with the whole chain. Run the block in the editor to see its output.'),
    )}


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
    following = _next_in_stage.get()
    if not following or os.getenv('MAGE_COMPILED_CHAINS') == 'off':
        return False
    downstream = block.downstream_blocks or []
    if len(downstream) != 1 or downstream[0].uuid != following:
        return False
    return not output_needed(block, bool(step.get('tests')))


def output_needed(block, has_tests: bool) -> bool:
    """
    Whether something needs the block's own output, so it computes its chain instead of
    planning: it is an exporter or a conditional, has tests, a data contract or monitors,
    or the pipeline keeps every block's output. compilation.py builds the same chains.
    """
    from mage_ai.data_preparation.models.constants import BlockType

    if block.type in (BlockType.DATA_EXPORTER, BlockType.CONDITIONAL) or has_tests:
        return True
    pipeline = block.pipeline
    if pipeline is not None and getattr(pipeline, 'keep_block_outputs', False):
        return True
    try:
        from mage_ai.data_preparation import contracts

        if contracts.binding_of(block) is not None:
            return True
    except Exception:
        return True
    try:
        from mage_ai.orchestration.monitors import monitors_for

        if pipeline is not None and monitors_for(block.repo_path, pipeline.uuid, block.uuid):
            return True
    except Exception:
        return True
    return False


def mark_failed(block_uuid: str, message: str) -> None:
    """Marks the block run of a planned block that failed when a later block ran the
    chain; the block that ran it fails with an error that names this one."""
    _mark(block_uuid, 'FAILED', message)


def mark_cancelled(block_uuid: str, message: str) -> None:
    """Marks the block run of a planned block after the one that failed in the chain: it
    never ran, as block by block it would have been cancelled."""
    _mark(block_uuid, 'CANCELLED', message)


def _mark(block_uuid: str, status: str, message: str) -> None:
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
            block_run.update(status=getattr(BlockRun.BlockRunStatus, status), metrics=metrics)
    except Exception:
        pass


def resolve(
    value: Any,
    block,
    execution_partition: Optional[str] = None,
    global_vars: Optional[Dict] = None,
) -> Any:
    """
    A stored plan (read from storage, not passed in memory) run to its value, for a reader
    outside the chain: a retry, a later stage, the editor. Storage keeps the plan's steps
    but not the run's variables, which global_vars gives.
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
        inputs.append(resolve(variable, upstream, execution_partition, global_vars))
    plan = Plan(
        stored['language'], stored['steps'], inputs, stored.get('upstream_uuids') or [],
        global_vars,
    )
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
        # The planned blocks after it were marked completed when they planned; none ran.
        uuids = [s['block_uuid'] for s in steps]
        if block_uuid in uuids:
            for later in uuids[uuids.index(block_uuid) + 1:-1]:
                mark_cancelled(later, f'Not run: block {block_uuid} before it in the '
                                      f'compiled chain failed.')
        chain = ', '.join(s['block_uuid'] for s in steps)
        message = (
            f'Block {block_uuid} failed when {last} ran the compiled chain {chain}:\n\n'
            f'{message}'
        )
    return ChainFailure(message, block_uuid)
