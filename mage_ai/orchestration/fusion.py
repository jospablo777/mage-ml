"""
Block fusion: a chain of blocks runs as one stage, in one process, and each block
receives the previous block's output from memory. Every block keeps its block run,
status, logs, retries and stored output. See docs/development/block-fusion.md.
"""
import copy
import gc
import os
import sys
import threading
import time
import warnings
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import exists, func, select, update

from mage_ai.data_preparation.models.constants import (
    BlockLanguage,
    BlockType,
    ExecutorType,
    PipelineType,
)
from mage_ai.orchestration.db import db_connection
from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun
from mage_ai.orchestration.trigger_rules import (
    Decision,
    block_trigger_rule,
    continues_after_failure,
    decide,
    has_trigger_rule,
)

BLOCK_FUSION_CHAINS = 'chains'

# Block types and languages that run in a stage. Sensors and global data products can
# wait for hours; dbt blocks manage their own upstream tables.
FUSIBLE_BLOCK_TYPES = frozenset([
    BlockType.CUSTOM,
    BlockType.DATA_EXPORTER,
    BlockType.DATA_LOADER,
    BlockType.TRANSFORMER,
])
FUSIBLE_LANGUAGES = frozenset([
    BlockLanguage.PYTHON, BlockLanguage.R, BlockLanguage.RUST, BlockLanguage.SQL,
])
LOCAL_EXECUTOR_TYPES = frozenset([ExecutorType.LOCAL_PYTHON, ExecutorType.LOCAL_PYTHON_FORCE])
# Block run metrics that mean the run is not the plain run of a block: dynamic children,
# data integration controllers and streams, replicas, hooks.
SPECIAL_METRICS = frozenset([
    'child',
    'children',
    'controller',
    'controller_block_uuid',
    'dynamic_block_index',
    'dynamic_block_indexes',
    'dynamic_upstream_block_uuids',
    'hook',
    'original_block_uuid',
    'stream',
    'upstream_blocks',
])

# A stage ends after a block when its process holds more than this share of the
# machine's memory; the next block starts a new stage in a new process.
MAX_MEMORY_SHARE = float(os.getenv('MAGE_STAGE_MAX_MEMORY_SHARE') or 0.5)


# A pipeline run with this metric is run in process by `mage verify-fusion`, with fusion
# set by its value, BLOCK_FUSION_CHAINS or BLOCK_FUSION_OFF, whatever the pipeline says.
# Schedulers leave such runs alone.
VERIFY_FUSION_METRIC = 'verify_fusion'
BLOCK_FUSION_OFF = 'off'


def verification_mode(pipeline_run) -> Optional[str]:
    if pipeline_run is None:
        return None
    return (pipeline_run.metrics or {}).get(VERIFY_FUSION_METRIC)


# Production runs (triggers, schedules, backfills, the API) fuse chains of blocks unless a
# pipeline says block_fusion: off. Running blocks one by one is for developing them in the
# editor, which is unchanged. MAGE_BLOCK_FUSION_DEFAULT=off changes the default.
DEFAULT_BLOCK_FUSION = os.getenv('MAGE_BLOCK_FUSION_DEFAULT') or BLOCK_FUSION_CHAINS


def block_fusion_mode(pipeline) -> str:
    """The pipeline's block fusion: its own setting, or the default."""
    return (getattr(pipeline, 'block_fusion', None) or DEFAULT_BLOCK_FUSION) \
        if pipeline is not None else BLOCK_FUSION_OFF


def fusion_enabled(pipeline, pipeline_run=None) -> bool:
    from mage_ai.settings.server import MEMORY_MANAGER_V2

    mode = verification_mode(pipeline_run)
    if mode is None and pipeline is not None:
        mode = block_fusion_mode(pipeline)
    return (
        pipeline is not None
        and mode == BLOCK_FUSION_CHAINS
        and pipeline.type == PipelineType.PYTHON
        and not pipeline.run_pipeline_in_one_process
        and not MEMORY_MANAGER_V2
    )


def run_controlled(block) -> bool:
    """Whether the block runs depending on a condition, on being disabled, or on how its
    upstream blocks ended (a trigger rule)."""
    return bool(
        (block.configuration or {}).get('disabled')
        or has_trigger_rule(block)
        or getattr(block, 'conditional_blocks', None)
    )


def block_can_fuse(pipeline, block) -> bool:
    """Whether a block can run in a stage with the blocks before and after it."""
    if block is None:
        return False
    if (block.configuration or {}).get('fusion') is False:
        return False
    # Whether it runs is decided on its own, before it starts.
    if run_controlled(block):
        return False
    return block_can_run_in_stage(pipeline, block)


def block_can_run_in_stage(pipeline, block) -> bool:
    """Whether a block can run in a stage of `mage run` in this process."""
    from mage_ai.data_preparation.executors.executor_factory import ExecutorFactory
    from mage_ai.data_preparation.models.block.dynamic.utils import (
        is_dynamic_block,
        is_dynamic_block_child,
        is_replicated_block,
        should_reduce_output,
    )

    if block is None:
        return False
    configuration = block.configuration or {}
    if configuration.get('variables'):
        return False
    # A block that declares resources waits for them on its own (resources.py).
    if configuration.get('resources'):
        return False
    if block.type not in FUSIBLE_BLOCK_TYPES or block.language not in FUSIBLE_LANGUAGES:
        return False
    if (
        is_dynamic_block(block)
        or is_dynamic_block_child(block)
        or should_reduce_output(block)
        or is_replicated_block(block)
        or block.is_data_integration()
    ):
        return False
    return ExecutorFactory.get_block_executor_type(pipeline, block) in LOCAL_EXECUTOR_TYPES


def block_run_can_fuse(block_run: BlockRun) -> bool:
    metrics = block_run.metrics or {}
    # A block run whose process died runs alone.
    return not (SPECIAL_METRICS & set(metrics)) and not int(metrics.get('crashes') or 0)


def next_in_chain(pipeline, block):
    """
    The block that runs after this one in the same stage, or None: this block's only
    downstream block, when that block has no other upstream block and both can fuse.
    """
    downstream = block.downstream_blocks or []
    if len(downstream) != 1:
        return None
    following = downstream[0]
    if len(following.upstream_blocks or []) != 1:
        return None
    if not block_can_fuse(pipeline, block) or not block_can_fuse(pipeline, following):
        return None
    return following


def stage_block_runs(pipeline, head: BlockRun, block_runs: List[BlockRun]) -> List[BlockRun]:
    """
    The block runs of the stage that starts with head: head, then each next block of the
    chain while its block run is the only one of its block and still INITIAL.
    """
    block = pipeline.get_block(head.block_uuid)
    if not block_can_fuse(pipeline, block) or not block_run_can_fuse(head):
        return [head]
    runs_by_block = {}
    for block_run in block_runs:
        runs_by_block.setdefault(block_run.block_uuid, []).append(block_run)

    stage = [head]
    while True:
        following = next_in_chain(pipeline, block)
        if following is None:
            break
        runs = runs_by_block.get(following.uuid) or []
        if len(runs) != 1 or runs[0].status != BlockRun.BlockRunStatus.INITIAL:
            break
        if not block_run_can_fuse(runs[0]):
            break
        stage.append(runs[0])
        block = following
    return stage


def fusion_plan(pipeline) -> List[List[str]]:
    """
    The stages a run of the pipeline as saved compiles into, as lists of block uuids:
    each chain of blocks that run in one process, and each block that runs alone. A run
    builds the same stages from its block runs when it starts.
    """
    if pipeline is None or pipeline.type != PipelineType.PYTHON:
        return []
    # Markdown, charts and scratchpads are not run.
    blocks = [b for b in pipeline.blocks_by_uuid.values() if executable(b)]
    continues = {
        following.uuid for following in (next_in_chain(pipeline, b) for b in blocks)
        if following is not None
    }
    stages = []
    for block in blocks:
        if block.uuid in continues:
            continue
        stage = [block.uuid]
        following = next_in_chain(pipeline, block)
        while following is not None:
            stage.append(following.uuid)
            following = next_in_chain(pipeline, following)
        stages.append(stage)
    return stages


def passes_in_memory(output: Any) -> bool:
    """
    Whether the next block may receive this output from memory: only when storage would
    give it the same value. A single pandas DataFrame without object columns or an
    object index, or a Polars DataFrame. Storage casts pandas object columns of strings
    to the str dtype, and changes JSON values, NumPy arrays and LazyFrames; those are
    read from storage.
    """
    if not isinstance(output, list) or len(output) != 1:
        return False
    value = output[0]
    from mage_ai.data_preparation.models.block.plans import Plan

    if isinstance(value, Plan):
        # A planned block: the next block of its language extends the plan (plans.py).
        return True
    import numpy as np
    import pandas as pd
    import polars as pl

    if type(value) is pl.DataFrame:
        return True
    if type(value) is pd.DataFrame:
        # The kind of pandas' str and category dtypes is also 'O'; only NumPy's object
        # dtype holds arbitrary Python values.
        object_dtype = np.dtype('O')
        if value.attrs or value.index.dtype == object_dtype:
            return False
        if isinstance(value.index, pd.MultiIndex) or isinstance(value.columns, pd.MultiIndex):
            return False
        # Integer labels come back as an Index, not the RangeIndex they may have been.
        if not all(isinstance(label, str) for label in value.columns):
            return False
        return not any(dtype == object_dtype for dtype in value.dtypes)
    return False


def copy_for_reader(output: Optional[List]) -> Optional[List]:
    """A copy whose changes do not reach the original: shallow for DataFrames."""
    if not isinstance(output, list):
        return output
    import pandas as pd
    import polars as pl

    copied = []
    for value in output:
        if isinstance(value, pd.DataFrame):
            # Copy-on-write: changing a column of the copy copies that column.
            value = value.copy(deep=False)
        elif isinstance(value, pl.DataFrame):
            value = value.clone()
        copied.append(value)
    return copied


def _now():
    return datetime.now(tz=pytz.UTC)


def claim_block_run(block_run_id: int, pipeline_run_id: int) -> Optional[int]:
    """
    Marks a block run RUNNING if it is still waiting to run and its pipeline run is
    running, and returns its new attempt; None when another worker or the scheduler got
    there first.
    """
    return _claim(block_run_id, pipeline_run_id, [
        BlockRun.BlockRunStatus.INITIAL,
        BlockRun.BlockRunStatus.QUEUED,
    ])


# The first block run of a stage is claimed as any block run.
claim_first = claim_block_run


def complete_and_claim(
    completed_id: int,
    next_id: Optional[int],
    pipeline_run_id: int,
    metrics: Optional[Dict] = None,
    attempt: Optional[int] = None,
) -> Tuple[bool, Optional[int]]:
    """
    In one transaction: marks a block run COMPLETED and the next block run of the stage
    RUNNING, if the next one is still INITIAL and the pipeline run still RUNNING. The
    scheduler never sees the completed run with its next run waiting, so it never starts
    the next run on its own.

    With attempt, the completion applies only while the block run is RUNNING in that
    attempt: a stage whose block run was reset, timed out or cancelled meanwhile records
    nothing and claims nothing. Returns whether the completion was recorded and the next
    run's attempt, None when it was not claimed.
    """
    session = db_connection.session
    try:
        completed = session.get(BlockRun, completed_id)
        values = dict(status=BlockRun.BlockRunStatus.COMPLETED, completed_at=_now())
        if metrics:
            values['metrics'] = dict(completed.metrics or {}, **metrics)
        statement = update(BlockRun).where(BlockRun.id == completed_id)
        if attempt is not None:
            statement = statement.where(
                BlockRun.attempt == attempt,
                BlockRun.status == BlockRun.BlockRunStatus.RUNNING,
            )
        result = session.execute(
            statement.values(**values).execution_options(synchronize_session=False),
        )
        if result.rowcount != 1:
            session.rollback()
            session.expire_all()
            return False, None
        next_attempt = None
        if next_id is not None:
            next_attempt = _claim_statement(session, next_id, pipeline_run_id, [
                BlockRun.BlockRunStatus.INITIAL,
            ])
        session.commit()
    except Exception:
        session.rollback()
        raise
    session.expire_all()
    return True, next_attempt


def _claim(block_run_id: int, pipeline_run_id: int, statuses: List) -> Optional[int]:
    session = db_connection.session
    try:
        attempt = _claim_statement(session, block_run_id, pipeline_run_id, statuses)
        session.commit()
    except Exception:
        session.rollback()
        raise
    session.expire_all()
    return attempt


def _claim_statement(session, block_run_id: int, pipeline_run_id: int, statuses) -> Optional[int]:
    """The claim's attempt, or None when the block run was not claimable."""
    running = exists(select(PipelineRun.id).where(
        PipelineRun.id == pipeline_run_id,
        PipelineRun.status == PipelineRun.PipelineRunStatus.RUNNING,
    ))
    result = session.execute(
        update(BlockRun)
        .where(BlockRun.id == block_run_id, BlockRun.status.in_(statuses), running)
        .values(
            status=BlockRun.BlockRunStatus.RUNNING,
            started_at=_now(),
            attempt=func.coalesce(BlockRun.attempt, 0) + 1,
        )
        .execution_options(synchronize_session=False)
    )
    if result.rowcount != 1:
        return None
    # Read in the same transaction: the claim made this block run ours.
    return session.execute(
        select(BlockRun.attempt).where(BlockRun.id == block_run_id),
    ).scalar_one()


class ProcessState:
    """
    Process state a block can change and the next block would inherit in a stage:
    environment variables, the working directory, sys.path, warning filters and NumPy
    error settings. Restored after each block, as a new process would start clean.
    """

    def __init__(self):
        import numpy as np

        self.environ = dict(os.environ)
        self.cwd = os.getcwd()
        self.sys_path = list(sys.path)
        self.warning_filters = list(warnings.filters)
        self.numpy_errors = np.geterr()
        self.threads = self.__non_daemon_threads()

    def restore(self) -> None:
        import numpy as np

        for key in set(os.environ) - set(self.environ):
            del os.environ[key]
        for key, value in self.environ.items():
            if os.environ.get(key) != value:
                os.environ[key] = value
        try:
            os.chdir(self.cwd)
        except OSError:
            pass
        sys.path[:] = self.sys_path
        warnings.filters[:] = self.warning_filters
        warnings._filters_mutated()
        np.seterr(**self.numpy_errors)

    def threads_left_running(self) -> List[threading.Thread]:
        """Non-daemon threads a block started that still run; they would outlive it."""
        before = {id(thread) for thread in self.threads}
        return [t for t in self.__non_daemon_threads() if id(t) not in before]

    @staticmethod
    def __non_daemon_threads() -> List[threading.Thread]:
        return [t for t in threading.enumerate() if not t.daemon and t.is_alive()]


def over_memory_limit() -> bool:
    try:
        import psutil

        rss = psutil.Process().memory_info().rss
        return rss > MAX_MEMORY_SHARE * psutil.virtual_memory().total
    except Exception:
        return False


def release_block(block) -> None:
    """
    Drops what a block holds after it ran: the globals of its code, which hold its
    inputs in a reference cycle with its functions, and its variables. Then returns the
    freed memory, which would otherwise stay with the process for the next block.
    """
    if block is not None:
        block.test_functions = []
        block.global_vars = None
        block.module = None
        block._outputs = None
        block._store_variables_in_block_function = None
    gc.collect()
    try:
        import pyarrow as pa

        pa.default_memory_pool().release_unused()
    except Exception:
        pass


def member_variables(variables: Optional[Dict]) -> Dict:
    """Each block gets its own copy of the run's variables, as a new process would."""
    try:
        return copy.deepcopy(variables or {})
    except Exception:
        return dict(variables or {})


# ---- stages in one process, for mage run -------------------------------------------------


def cli_tracking(pipeline, execution_partition: Optional[str] = None) -> Optional[Dict]:
    """
    The tags MLflow runs get while a block runs in `mage run` (experiments.py): the
    project, pipeline, block, partition, code digest and an id shared by the blocks of
    one invocation. Runs started from the CLI had no mage.* tags at all.
    """
    import uuid as uuid_lib

    from mage_ai.orchestration import experiments, run_records

    if not experiments.enabled():
        return None
    try:
        digest = run_records.code_digest(pipeline)
    except Exception:
        digest = None
    return dict(
        project=os.path.basename(os.path.realpath(pipeline.repo_path)),
        pipeline_uuid=pipeline.uuid,
        execution_partition=execution_partition,
        code_digest=digest,
        cli_run_id=uuid_lib.uuid4().hex[:16],
    )


def _conditions_pass(block, global_vars: Dict, execution_partition: Optional[str]) -> bool:
    """Whether a block runs: not disabled, and its conditional blocks return true. A
    condition that raises counts as false, as in a scheduled run (block_executor.py)."""
    if (block.configuration or {}).get('disabled'):
        print(f'Block {block.uuid} is disabled: it and the blocks after it are skipped.')
        return False
    for conditional in getattr(block, 'conditional_blocks', None) or []:
        try:
            passed = conditional.execute_conditional(
                block, execution_partition=execution_partition, global_vars=global_vars,
            )
        except Exception as error:
            print(f'Conditional block {conditional.uuid} of {block.uuid} failed: {error}')
            passed = False
        if not passed:
            print(f'Conditional block {conditional.uuid} returned false: block {block.uuid} '
                  'and the blocks after it are skipped.')
            return False
    return True


def _run_stage_in_process(pipeline, stage: List[str], global_vars: Dict, run_tests: bool,
                          update_status: bool, execution_partition: Optional[str] = None,
                          tracking: Optional[Dict] = None,
                          summary: Optional[List] = None,
                          statuses: Optional[Dict[str, str]] = None) -> None:
    """Runs a stage's blocks in order, each taking the previous block's output in memory;
    a Rust or R block followed by one of its language may plan (block/plans.py). Each
    block's status goes in statuses as a block run's would."""
    from mage_ai.data_preparation.models.block import plans
    from mage_ai.orchestration.db import end_transaction

    statuses = statuses if statuses is not None else {}
    previous = None
    for index, uuid in enumerate(stage):
        block = pipeline.get_block(uuid)
        following = pipeline.get_block(stage[index + 1]) if index + 1 < len(stage) else None
        next_block = following.uuid if plans.chains_with(block, following) else None
        # Each block gets its own copy of the run's variables, as in a scheduled run.
        variables = member_variables(global_vars)
        if not _conditions_pass(block, variables, execution_partition):
            for skipped in stage[index:]:
                statuses[skipped] = BlockRun.BlockRunStatus.CONDITION_FAILED
                if summary is not None:
                    summary.append(dict(block=skipped, seconds=None, tests=None, failed=False,
                                        status=BlockRun.BlockRunStatus.CONDITION_FAILED))
            return
        if previous:
            outputs_cache = {previous[0]: previous[1]}
        else:
            # A block with a trigger rule gets None from an upstream block that did not
            # complete, not that block's output of an earlier run.
            outputs_cache = {
                u: [None] for u in block.upstream_block_uuids
                if has_trigger_rule(block)
                and statuses.get(u) != BlockRun.BlockRunStatus.COMPLETED
            } or None
        started = time.monotonic()
        row = dict(block=uuid, seconds=None, tests=None, failed=True,
                   started_at=datetime.now(timezone.utc), completed_at=None)
        if summary is not None:
            summary.append(row)
        block.test_summary = None
        end_transaction()
        statuses[uuid] = BlockRun.BlockRunStatus.RUNNING
        try:
            with _block_tracking(tracking, uuid), plans.stage_context(next_block, None):
                result = block.execute_sync(
                    block_run_outputs_cache=outputs_cache,
                    execution_partition=execution_partition,
                    global_vars=variables,
                    run_all_blocks=True,
                    update_status=update_status,
                )
                # Without tests the data contract is still checked (contracts_only).
                try:
                    block.run_tests(
                        contracts_only=not run_tests,
                        execution_partition=execution_partition,
                        global_vars=variables, update_tests=False,
                    )
                finally:
                    row.update(seconds=time.monotonic() - started,
                               tests=getattr(block, 'test_summary', None),
                               completed_at=datetime.now(timezone.utc))
        except BaseException:
            statuses[uuid] = BlockRun.BlockRunStatus.FAILED
            for later in stage[index + 1:]:
                statuses[later] = BlockRun.BlockRunStatus.UPSTREAM_FAILED
            raise
        row['failed'] = False
        statuses[uuid] = BlockRun.BlockRunStatus.COMPLETED
        output = result.get('output') if isinstance(result, dict) else None
        previous = (uuid, output) if passes_in_memory(output) else None
        release_block(block)


def _record_cli_run(pipeline, prepared, summary, global_vars, started_at, failed,
                    execution_partition) -> None:
    """Records the run as a pipeline run (cli_runs.py); a failure to record is reported,
    not raised: the run itself is done."""
    from mage_ai.orchestration import cli_runs

    try:
        pipeline_run = cli_runs.record(
            pipeline, prepared, summary, global_vars, started_at, failed, execution_partition,
        )
        print(f'Recorded as pipeline run {pipeline_run.id} (mage run-diff, mage reproduce).')
    except Exception as error:
        print(f'[WARNING] The run could not be recorded: {error}')


def executable(block) -> bool:
    """Whether a run runs the block: markdown, charts and scratchpads document a pipeline."""
    from mage_ai.data_preparation.models.constants import (
        NON_PIPELINE_EXECUTABLE_BLOCK_TYPES,
    )

    return block is not None and block.type not in NON_PIPELINE_EXECUTABLE_BLOCK_TYPES


def stages_apply_to_cli(pipeline) -> bool:
    """Whether `mage run` runs the pipeline in stages: fusion on, or a block whose run is
    decided (run_controlled), which only the stages decide; and only blocks a stage can
    hold (no dynamic, integration or replicated blocks, or callbacks). Markdown, charts
    and scratchpads are not run, so they do not count."""
    if pipeline is None or pipeline.type != PipelineType.PYTHON:
        return False
    blocks = [b for b in pipeline.blocks_by_uuid.values() if executable(b)]
    if not fusion_enabled(pipeline) and not any(map(run_controlled, blocks)):
        return False
    if getattr(pipeline, 'callbacks', None):
        return False
    return all(block_can_run_in_stage(pipeline, block) for block in blocks)


def print_run_summary(rows: List[Dict], run_tests: bool) -> None:
    """Each block's time and tests, then the tests in total: tests are off by default in
    mage run, so a pipeline full of @test functions passed without running one, silently."""
    if not rows:
        return
    width = max(len(r['block']) for r in rows)
    lines = [f'{"Block":<{width}}  {"Seconds":>8}  Tests']
    defined = ran = passed = 0
    for row in rows:
        tests = row.get('tests') or {}
        defined += tests.get('defined', 0)
        ran += tests.get('ran', 0)
        passed += tests.get('passed', 0)
        if tests.get('ran'):
            text = f'{tests["passed"]}/{tests["ran"]} passed'
        elif tests.get('defined'):
            text = f'{tests["defined"]} not run'
        else:
            text = '-'
        seconds = '' if row.get('seconds') is None else f'{row["seconds"]:.2f}'
        if row.get('failed'):
            text += ' (failed)' if text != '-' else 'failed'
        elif row.get('status') == BlockRun.BlockRunStatus.CONDITION_FAILED:
            text = 'skipped'
        elif row.get('status') == BlockRun.BlockRunStatus.UPSTREAM_FAILED:
            text = 'not run: upstream failed'
        lines.append(f'{row["block"]:<{width}}  {seconds:>8}  {text}')
    blocks = len(rows)
    if ran:
        outcome = 'all passed' if passed == ran else f'{ran - passed} failed'
        lines.append(f'{ran} tests in {blocks} blocks, {outcome}.')
    if defined > ran and not run_tests:
        lines.append(f'{defined - ran} block tests were not run: run with --test to run them.')
    print('\n'.join(lines))


def _block_tracking(tracking: Optional[Dict], block_uuid: str):
    from contextlib import nullcontext

    if not tracking:
        return nullcontext()
    from mage_ai.orchestration import experiments

    return experiments.block_context(dict(tracking, block_uuid=block_uuid))


def stage_limit(pipeline, max_parallel: Optional[int] = None) -> Optional[int]:
    """How many stages `mage run` runs at once: `--max-parallel`, else the pipeline's
    `concurrency_config.block_run_limit` (a stage runs one block at a time), else no limit."""
    configured = (getattr(pipeline, 'concurrency_config', None) or {}).get('block_run_limit')
    for value in (max_parallel, configured):
        try:
            if value is not None and int(value) > 0:
                return int(value)
        except (TypeError, ValueError):
            continue
    return None


async def execute_in_stages(
    pipeline,
    global_vars: Optional[Dict] = None,
    run_tests: bool = True,
    update_status: bool = False,
    execution_partition: Optional[str] = None,
    max_parallel: Optional[int] = None,
    record_run: bool = False,
    fuse: bool = True,
) -> None:
    """
    `mage run` with block fusion (fuse), or with each block a stage of its own: the
    pipeline's stages in this process, as a fused run runs them. With record_run, the run is
    recorded as a pipeline run when it ends (cli_runs.py). A stage starts as soon as its
    upstream blocks are done, up to the stage limit (stage_limit), and not while the process
    holds more memory than MAGE_STAGE_MAX_MEMORY_SHARE allows and another stage runs. A
    disabled block, or one whose condition is false, is skipped with the blocks after it; a
    stage whose first block has a trigger rule starts as the rule says (trigger_rules.py).
    After a stage fails, no stage starts, unless the pipeline has an all_done block: then
    the stages that can still run do. The run waits for the stages already running (a thread
    cannot be stopped) and raises the first failure, with the others noted.
    """
    import asyncio

    global_vars = global_vars or {}
    if fuse:
        pending = [list(stage) for stage in fusion_plan(pipeline)]
    else:
        # Block fusion off: each block is a stage of its own.
        pending = [[b.uuid] for b in pipeline.blocks_by_uuid.values() if executable(b)]
    limit = stage_limit(pipeline, max_parallel)
    started_at = datetime.now(timezone.utc)
    prepared = None
    if record_run:
        from mage_ai.orchestration import run_records

        try:
            prepared = await asyncio.to_thread(run_records.prepare, pipeline, global_vars)
        except Exception as error:
            print(f'[WARNING] The run record could not be prepared: {error}')
    tracking = await asyncio.to_thread(cli_tracking, pipeline, execution_partition)
    summary: List[Dict] = []
    # Block uuid to the status its block run would have.
    statuses: Dict[str, str] = {}
    running: Dict = {}
    failures: List = []
    # An all_done block runs after a failed block, as in a scheduled run.
    keep_going = continues_after_failure(pipeline)

    def decision(stage: List[str]) -> Decision:
        """Whether the stage runs, waits, or ends as its first block's rule says."""
        head = pipeline.get_block(stage[0])
        return decide(block_trigger_rule(head), [
            statuses.get(u.uuid) for u in (head.upstream_blocks or [])
            if getattr(u, 'type', None) is None or executable(u)
        ])

    def settle() -> None:
        """Stages that will not run: their blocks end skipped or upstream failed."""
        settled = True
        while settled:
            settled = False
            for stage in list(pending):
                outcome = decision(stage)
                if outcome in (Decision.UPSTREAM_FAILED, Decision.CONDITION_FAILED):
                    pending.remove(stage)
                    settled = True
                    for uuid in stage:
                        statuses[uuid] = BlockRun.BlockRunStatus(outcome.value)
                        summary.append(dict(block=uuid, seconds=None, tests=None,
                                            failed=False, status=outcome.value))

    def exclusive(stage: List[str]) -> bool:
        # Blocks that change process-wide state (environment, MLflow's tracking URI,
        # global settings) run with no other stage beside them (I-110).
        return any(
            (getattr(pipeline.get_block(uuid), 'configuration', None) or {}).get('exclusive')
            for uuid in stage
        )

    while pending or running:
        if not failures or keep_going:
            settle()
            for stage in [s for s in pending if decision(s) == Decision.RUN]:
                if limit is not None and len(running) >= limit:
                    break
                if running and over_memory_limit():
                    break
                # An exclusive stage starts when nothing runs, and nothing starts beside
                # it; admission stops while it waits, so it is not starved.
                if running and (exclusive(stage) or any(map(exclusive, running.values()))):
                    break
                task = asyncio.ensure_future(asyncio.to_thread(
                    _run_stage_in_process, pipeline, stage, global_vars, run_tests,
                    update_status, execution_partition, tracking, summary, statuses,
                ))
                running[task] = stage
                pending.remove(stage)
        if not running:
            if failures or not pending:
                break
            raise RuntimeError(
                'The pipeline has blocks whose upstream blocks never run: '
                + ', '.join(s[0] for s in pending),
            )
        finished, _ = await asyncio.wait(list(running), return_when=asyncio.FIRST_COMPLETED)
        for task in finished:
            stage = running.pop(task)
            error = task.exception()
            if error is not None:
                failures.append((stage, error))
    print_run_summary(summary, run_tests)
    if record_run:
        _record_cli_run(pipeline, prepared, summary, global_vars, started_at, bool(failures),
                        execution_partition)
    if failures:
        error = failures[0][1]
        for stage, other in failures[1:]:
            error.add_note(f'The stage starting at {stage[0]} failed too: {other}')
        if pending:
            error.add_note(
                'Not started after the failure: ' + ', '.join(s[0] for s in pending),
            )
        raise error
