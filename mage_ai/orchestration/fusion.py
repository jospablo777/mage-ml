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
import warnings
from datetime import datetime
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


def fusion_enabled(pipeline, pipeline_run=None) -> bool:
    from mage_ai.settings.server import MEMORY_MANAGER_V2

    mode = verification_mode(pipeline_run)
    if mode is None and pipeline is not None:
        mode = pipeline.block_fusion
    return (
        pipeline is not None
        and mode == BLOCK_FUSION_CHAINS
        and pipeline.type == PipelineType.PYTHON
        and not pipeline.run_pipeline_in_one_process
        and not MEMORY_MANAGER_V2
    )


def block_can_fuse(pipeline, block) -> bool:
    """Whether a block can run in a stage with the blocks before and after it."""
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
    if configuration.get('fusion') is False or configuration.get('variables'):
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
    blocks = list(pipeline.blocks_by_uuid.values())
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
