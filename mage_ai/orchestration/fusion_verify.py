"""
Verify fusion: run a pipeline once block by block and once with block fusion, and compare
every stored output. The first block whose output differs is the place where fusion
changes the pipeline's results. See docs/development/block-fusion.md.

Both runs go through the scheduler and the functions workers run (run_block, run_stage).
Each job the scheduler enqueues runs in its own process, as on a worker, and the
scheduler looks at the run again once the jobs end: a block that runs alone gets a fresh
process, and a stage one process for its chain, so state a block leaves in its process
reaches the next block only when fusion would pass it. The runs carry
VERIFY_FUSION_METRIC, so a scheduler of a running server leaves them alone, and their
fusion mode does not depend on the pipeline's setting. Each mode has its own trigger, so
the two runs store their outputs in separate partitions.

Both runs execute the whole pipeline: exporters write twice.
"""
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from mage_ai.orchestration import fusion
from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun, PipelineSchedule

TRIGGER_NAMES = {
    fusion.BLOCK_FUSION_OFF: 'Verify fusion: block by block',
    fusion.BLOCK_FUSION_CHAINS: 'Verify fusion: fused',
}
# Scheduler passes without a job to run before a run counts as stuck.
IDLE_PASSES = 3
MAX_PASSES = 10_000


class FusionVerificationError(Exception):
    pass


@dataclass
class BlockResult:
    block_uuid: str
    # same, differs, failed or not compared
    result: str
    detail: str = ''
    stage: Optional[int] = None


@dataclass
class Verification:
    pipeline_uuid: str
    plan: List[List[str]]
    runs: Dict[str, int]
    statuses: Dict[str, str]
    blocks: List[BlockResult] = field(default_factory=list)
    # Whether floats that differ only by rounding count as a difference.
    exact: bool = False

    @property
    def first_difference(self) -> Optional[BlockResult]:
        failing = ('differs', 'failed', 'close') if self.exact else ('differs', 'failed')
        return next((b for b in self.blocks if b.result in failing), None)

    @property
    def passed(self) -> bool:
        return self.first_difference is None and all(
            status == PipelineRun.PipelineRunStatus.COMPLETED for status in self.statuses.values()
        )

    def to_dict(self) -> Dict:
        first = self.first_difference
        return dict(
            blocks=[asdict(b) for b in self.blocks],
            exact=self.exact,
            first_difference=asdict(first) if first else None,
            passed=self.passed,
            pipeline_uuid=self.pipeline_uuid,
            plan=self.plan,
            runs=self.runs,
            statuses=self.statuses,
        )


def _run_job(target: Callable, args: tuple) -> None:
    from mage_ai.orchestration.db.process import start_session_and_run
    from mage_ai.shared.processes import exit_with_parent

    exit_with_parent()
    start_session_and_run(target, *args)


class JobRunner:
    """
    Takes the jobs the scheduler enqueues; run_pending runs each in its own process, at
    the same time, as the worker pool would, and waits for them. It stands in for the
    job manager only in a process that runs no server or scheduler.
    """

    def __init__(self):
        self.pending: List[Tuple[Callable, tuple, dict]] = []
        self.started = set()

    def add_job(self, job_type, uid, target, *args, **kwargs):
        self.started.add(f'{job_type}_{uid}')
        self.pending.append((target, args, kwargs))

    def run_pending(self, log: Callable[[str], None]) -> int:
        import multiprocessing

        context = multiprocessing.get_context('spawn')
        processes = []
        while self.pending:
            target, args, _ = self.pending.pop(0)
            # Not a daemon: blocks may start processes of their own.
            process = context.Process(target=_run_job, args=(target, args))
            process.start()
            processes.append((target.__name__, process))
        for name, process in processes:
            process.join()
            if process.exitcode:
                # The block run holds the error; the scheduler handles it.
                log(f'Job {name} exited with code {process.exitcode}.')
        return len(processes)

    def _has(self, job_id: str) -> bool:
        return job_id in self.started

    def has_block_run_job(self, block_run_id, logger=None, logging_tags=None) -> bool:
        return self._has(f'block_run_{block_run_id}')

    def has_pipeline_run_job(self, pipeline_run_id, logger=None, logging_tags=None) -> bool:
        return self._has(f'pipeline_run_{pipeline_run_id}')

    def has_integration_stream_job(self, pipeline_run_id, stream, logger=None,
                                   logging_tags=None) -> bool:
        return self._has(f'pipeline_run_{pipeline_run_id}_{stream}')

    def has_generic_job(self, generic_job_id, logger=None, logging_tags=None) -> bool:
        return self._has(f'generic_job_{generic_job_id}')

    def kill_block_run_job(self, block_run_id):
        pass

    def kill_pipeline_run_job(self, pipeline_run_id):
        pass

    def kill_integration_stream_job(self, pipeline_run_id, stream):
        pass

    def kill_generic_job(self, generic_job_id):
        pass

    def clean_up_jobs(self):
        pass

    def jobs_finished(self) -> bool:
        return not self.pending

    def start(self):
        pass

    def stop(self):
        pass


def _trigger(pipeline, mode: str) -> PipelineSchedule:
    from mage_ai.data_preparation.models.triggers import ScheduleStatus, ScheduleType

    name = TRIGGER_NAMES[mode]
    schedule = PipelineSchedule.repo_query.filter(
        PipelineSchedule.name == name,
        PipelineSchedule.pipeline_uuid == pipeline.uuid,
    ).first()
    if schedule is None:
        # Inactive: schedulers start no runs of it.
        schedule = PipelineSchedule.create(
            name=name,
            pipeline_uuid=pipeline.uuid,
            repo_path=pipeline.repo_path,
            schedule_type=ScheduleType.API,
            status=ScheduleStatus.INACTIVE,
        )
    return schedule


def _create_run(pipeline, mode: str, variables: Dict, execution_date: datetime) -> PipelineRun:
    from mage_ai.orchestration.pipeline_scheduler import configure_pipeline_run_payload

    payload, _ = configure_pipeline_run_payload(
        _trigger(pipeline, mode),
        pipeline.type,
        dict(execution_date=execution_date, variables=dict(variables or {})),
    )
    return PipelineRun.create(metrics={fusion.VERIFY_FUSION_METRIC: mode}, **payload)


def _cancel_unfinished(pipeline_uuid: str) -> None:
    """Earlier verifications whose process stopped leave runs no scheduler finishes."""
    for run in PipelineRun.query.filter(
        PipelineRun.pipeline_uuid == pipeline_uuid,
        PipelineRun.status.in_([
            PipelineRun.PipelineRunStatus.INITIAL,
            PipelineRun.PipelineRunStatus.RUNNING,
        ]),
    ).all():
        if fusion.verification_mode(run):
            run.update(status=PipelineRun.PipelineRunStatus.CANCELLED)


def run_in_process(pipeline_run: PipelineRun, log: Callable[[str], None] = print) -> PipelineRun:
    """Runs a pipeline run to its end in this process, through the scheduler."""
    from mage_ai.orchestration import job_manager as job_manager_module
    from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler

    unfinished = (PipelineRun.PipelineRunStatus.INITIAL, PipelineRun.PipelineRunStatus.RUNNING)
    manager = JobRunner()
    previous = job_manager_module.job_manager
    job_manager_module.job_manager = manager
    try:
        PipelineScheduler(pipeline_run).start(should_schedule=False)
        idle = 0
        for _ in range(MAX_PASSES):
            pipeline_run.refresh()
            if pipeline_run.status not in unfinished:
                break
            PipelineScheduler(pipeline_run).schedule()
            pipeline_run.refresh()
            if pipeline_run.status not in unfinished:
                break
            if manager.run_pending(log):
                idle = 0
                continue
            idle += 1
            if idle >= IDLE_PASSES:
                waiting = [
                    f'{b.block_uuid} ({b.status})' for b in pipeline_run.block_runs
                    if b.status not in (
                        BlockRun.BlockRunStatus.COMPLETED,
                        BlockRun.BlockRunStatus.FAILED,
                        BlockRun.BlockRunStatus.CANCELLED,
                        BlockRun.BlockRunStatus.UPSTREAM_FAILED,
                        BlockRun.BlockRunStatus.CONDITION_FAILED,
                    )
                ]
                pipeline_run.update(status=PipelineRun.PipelineRunStatus.FAILED)
                raise FusionVerificationError(
                    f'Pipeline run {pipeline_run.id} stopped making progress; block runs '
                    f'left: {", ".join(waiting) or "none"}.',
                )
    except BaseException:
        pipeline_run.refresh()
        if pipeline_run.status in unfinished:
            pipeline_run.update(status=PipelineRun.PipelineRunStatus.CANCELLED)
        raise
    finally:
        job_manager_module.job_manager = previous
    pipeline_run.refresh()
    return pipeline_run


def _collect(value: Any) -> Any:
    try:
        import polars as pl

        if isinstance(value, pl.LazyFrame):
            return value.collect()
    except ImportError:
        pass
    return value


def _type_name(value: Any) -> str:
    kind = type(value)
    return f'{kind.__module__.split(".")[0]}.{kind.__qualname__}'


def _first_lines(text: str, count: int = 12) -> str:
    lines = [line for line in str(text).strip().splitlines() if line.strip()]
    return '\n'.join(lines[:count])


RELATIVE_TOLERANCE = 1e-9


def _compare_floats(left, right, what: str) -> Tuple[str, str]:
    import numpy as np

    left = np.asarray(left, dtype='complex128' if np.iscomplexobj(left) else 'float64')
    right = np.asarray(right, dtype=left.dtype)
    left_nan, right_nan = np.isnan(left), np.isnan(right)
    if (left_nan != right_nan).any():
        row = int(np.argmax(left_nan != right_nan))
        return 'differs', f'{what}: row {row} is {left[row]} block by block, {right[row]} fused'
    valid = ~left_nan
    left, right = left[valid], right[valid]
    if left.size == 0 or np.array_equal(left, right):
        return 'same', ''
    infinite = np.isinf(left) | np.isinf(right)
    if (left[infinite] != right[infinite]).any():
        return 'differs', f'{what}: infinite values differ'
    left, right = left[~infinite], right[~infinite]
    scale = np.maximum(np.abs(left), np.abs(right))
    relative = np.abs(left - right) / np.where(scale == 0, 1, scale)
    largest = float(relative.max()) if relative.size else 0.0
    count = int((left != right).sum())
    if largest <= RELATIVE_TOLERANCE:
        return 'close', f'{what}: {count} values differ by up to {largest:.1e} (relative)'
    row = int(np.argmax(relative))
    return 'differs', (
        f'{what}: {count} values differ, by up to {largest:.1e} (relative); for example '
        f'{left[row]!r} block by block, {right[row]!r} fused'
    )


def _combine(results: List[Tuple[str, str]]) -> Tuple[str, str]:
    for wanted in ('differs', 'close'):
        details = [detail for result, detail in results if result == wanted]
        if details:
            return wanted, '\n'.join(details)
    return 'same', ''


def _differing_rows(mask, left_values, right_values, what: str) -> Tuple[str, str]:
    import numpy as np

    mask = np.asarray(mask, dtype=bool)
    row = int(np.argmax(mask))
    return 'differs', (
        f'{what}: {int(mask.sum())} rows differ; row {row} is {left_values[row]!r} block '
        f'by block, {right_values[row]!r} fused'
    )


COLUMN_ORDER_HINT = (
    'The same columns in another order: the block\'s column order is not deterministic, as '
    'with a pivot, whose columns follow the order values appear. Select the columns in a '
    'fixed order.'
)
ORDER_HINT = (
    'The same rows in another order: the block\'s output order is not deterministic, so it '
    'depends on how its input is chunked. Sort the output on a unique key, as after a '
    'group_by, to make it deterministic.'
)


def _same_rows_in_another_order(expected, actual) -> bool:
    """Whether two tables of the same shape hold the same rows in a different order."""
    import pandas as pd
    import polars as pl

    try:
        if isinstance(expected, pl.DataFrame):
            columns = expected.columns
            return expected.sort(columns, nulls_last=True).equals(
                actual.sort(columns, nulls_last=True), null_equal=True,
            )
        if isinstance(expected, pd.DataFrame):
            columns = list(expected.columns)

            def ordered(frame):
                return frame.sort_values(columns, kind='stable').reset_index(drop=True)

            in_order = expected.reset_index(drop=True).equals(actual.reset_index(drop=True))
            return not in_order and ordered(expected).equals(ordered(actual))
    except Exception:
        return False
    return False


def _compare_pandas(expected, actual) -> Tuple[str, str]:
    import numpy as np
    import pandas as pd

    if isinstance(expected, pd.Series):
        expected = expected.to_frame(name=expected.name if expected.name is not None else 0)
        actual = actual.to_frame(name=actual.name if actual.name is not None else 0)
    if list(expected.columns) != list(actual.columns):
        if (
            sorted(map(str, expected.columns)) == sorted(map(str, actual.columns))
            and _compare_pandas(expected, actual[list(expected.columns)])[0] == 'same'
        ):
            return 'differs', COLUMN_ORDER_HINT
        return 'differs', (
            f'columns {list(expected.columns)} block by block, {list(actual.columns)} fused'
        )
    if len(expected) != len(actual):
        return 'differs', f'{len(expected)} rows block by block, {len(actual)} fused'
    dtypes = [
        f'{column}: {left} block by block, {right} fused'
        for column, left, right in zip(expected.columns, expected.dtypes, actual.dtypes)
        if left != right
    ]
    if dtypes:
        return 'differs', '\n'.join(dtypes)
    if not expected.index.equals(actual.index) or expected.index.dtype != actual.index.dtype:
        if _same_rows_in_another_order(expected, actual):
            return 'differs', ORDER_HINT
        return 'differs', 'The index differs.'
    results = []
    for position, column in enumerate(expected.columns):
        left, right = expected.iloc[:, position], actual.iloc[:, position]
        if left.equals(right):
            continue
        if pd.api.types.is_float_dtype(left.dtype) or pd.api.types.is_complex_dtype(left.dtype):
            results.append(_compare_floats(
                left.to_numpy(dtype='float64', na_value=np.nan),
                right.to_numpy(dtype='float64', na_value=np.nan),
                str(column),
            ))
            continue
        try:
            mask = ~((left == right) | (left.isna() & right.isna())).fillna(False)
            if mask.any():
                results.append(_differing_rows(
                    mask.to_numpy(), left.to_list(), right.to_list(), str(column),
                ))
                continue
        except (TypeError, ValueError):
            pass
        results.append(('differs', f'{column}: the values differ'))
    result = _combine(results)
    if result[0] == 'differs' and _same_rows_in_another_order(expected, actual):
        return 'differs', ORDER_HINT
    if result[0] == 'same':
        try:
            pd.testing.assert_frame_equal(expected, actual, check_exact=True)
        except AssertionError as error:
            return 'differs', _first_lines(error)
    return result


def _compare_polars(expected, actual) -> Tuple[str, str]:
    import polars as pl

    if isinstance(expected, pl.Series):
        expected, actual = expected.to_frame(), actual.to_frame()
    if expected.columns != actual.columns:
        if (
            sorted(expected.columns) == sorted(actual.columns)
            and _compare_polars(expected, actual.select(expected.columns))[0] == 'same'
        ):
            return 'differs', COLUMN_ORDER_HINT
        return 'differs', f'columns {expected.columns} block by block, {actual.columns} fused'
    if expected.height != actual.height:
        return 'differs', f'{expected.height} rows block by block, {actual.height} fused'
    dtypes = [
        f'{column}: {left} block by block, {right} fused'
        for column, left, right in zip(expected.columns, expected.dtypes, actual.dtypes)
        if left != right
    ]
    if dtypes:
        return 'differs', '\n'.join(dtypes)
    results = []
    for column in expected.columns:
        left, right = expected[column], actual[column]
        if left.equals(right, check_dtypes=True, check_names=True, null_equal=True):
            continue
        if left.dtype.is_float():
            results.append(_compare_floats(
                left.cast(pl.Float64).fill_null(float('nan')).to_numpy(),
                right.cast(pl.Float64).fill_null(float('nan')).to_numpy(),
                column,
            ))
            continue
        try:
            mask = (~left.eq_missing(right)).to_numpy()
            results.append(_differing_rows(mask, left.to_list(), right.to_list(), column))
        except Exception:
            results.append(('differs', f'{column}: the values differ'))
    result = _combine(results)
    if result[0] == 'differs' and _same_rows_in_another_order(expected, actual):
        return 'differs', ORDER_HINT
    return result


def compare_values(expected: Any, actual: Any) -> Tuple[str, str]:
    """
    ('same', ''), ('close', how), ('differs', why) or ('not compared', why). Tables
    compare values, dtypes, column names and order, and the index. Floats that differ by
    at most RELATIVE_TOLERANCE are close: summing in another order, as Polars does on
    differently chunked input, changes the last bits.
    """
    import numpy as np
    import pandas as pd

    expected, actual = _collect(expected), _collect(actual)
    if type(expected) is not type(actual):
        return 'differs', f'{_type_name(expected)} block by block, {_type_name(actual)} fused'

    try:
        import geopandas

        if isinstance(expected, geopandas.GeoDataFrame):
            from geopandas.testing import assert_geodataframe_equal

            try:
                assert_geodataframe_equal(expected, actual, check_less_precise=False)
            except AssertionError as error:
                return 'differs', _first_lines(error)
            return 'same', ''
    except ImportError:
        pass

    if isinstance(expected, pd.Index):
        if expected.equals(actual) and expected.dtype == actual.dtype:
            return 'same', ''
        return 'differs', 'The index values differ.'

    if isinstance(expected, (pd.DataFrame, pd.Series)):
        return _compare_pandas(expected, actual)

    try:
        import polars as pl

        if isinstance(expected, (pl.DataFrame, pl.Series)):
            return _compare_polars(expected, actual)
    except ImportError:
        pass

    try:
        import pyarrow as pa

        if isinstance(expected, (pa.Table, pa.RecordBatch, pa.Array, pa.ChunkedArray)):
            if expected.equals(actual):
                return 'same', ''
            return 'differs', 'The Arrow values differ.'
    except ImportError:
        pass

    if isinstance(expected, np.ndarray):
        if expected.dtype != actual.dtype or expected.shape != actual.shape:
            return 'differs', (
                f'array {expected.dtype} {expected.shape} block by block, '
                f'{actual.dtype} {actual.shape} fused'
            )
        if expected.dtype.kind in 'fc':
            return _compare_floats(expected.ravel(), actual.ravel(), 'array')
        same = np.array_equal(expected, actual)
        return ('same', '') if same else ('differs', 'The array values differ.')

    if isinstance(expected, dict):
        if list(expected) != list(actual):
            return 'differs', f'keys {list(expected)} block by block, {list(actual)} fused'
        results = []
        for key in expected:
            result, detail = compare_values(expected[key], actual[key])
            if result == 'differs' or result == 'not compared':
                return result, f'[{key!r}] {detail}'
            results.append((result, f'[{key!r}] {detail}'))
        return _combine(results)

    if isinstance(expected, (list, tuple)):
        if len(expected) != len(actual):
            return 'differs', f'{len(expected)} items block by block, {len(actual)} fused'
        results = []
        for index, (left, right) in enumerate(zip(expected, actual)):
            result, detail = compare_values(left, right)
            if result == 'differs' or result == 'not compared':
                return result, f'[{index}] {detail}'
            results.append((result, f'[{index}] {detail}'))
        return _combine(results)

    if isinstance(expected, float):
        return _compare_floats(np.array([expected]), np.array([actual]), 'value')

    if expected is None or isinstance(expected, (str, bytes, int, float, bool, datetime)):
        if expected == actual:
            return 'same', ''
        return 'differs', f'{expected!r} block by block, {actual!r} fused'

    try:
        same = expected == actual
        if isinstance(same, bool):
            return ('same', '') if same else ('differs', 'The values are not equal.')
    except Exception:
        pass
    return 'not compared', f'Values of type {_type_name(expected)} cannot be compared.'


def _ordered_blocks(pipeline) -> List:
    """The pipeline's blocks, each after its upstream blocks."""
    blocks = list(pipeline.blocks_by_uuid.values())
    done, ordered = set(), []
    while len(ordered) < len(blocks):
        ready = [
            b for b in blocks if b.uuid not in done
            and all(u.uuid in done for u in (b.upstream_blocks or []))
        ]
        if not ready:
            ready = [b for b in blocks if b.uuid not in done]
        for block in ready:
            done.add(block.uuid)
            ordered.append(block)
    return ordered


def _outputs(pipeline, block_uuid: str, run: PipelineRun) -> Dict[str, Any]:
    manager = pipeline.variable_manager
    names = manager.get_variables_by_block(
        pipeline.uuid, block_uuid, partition=run.execution_partition,
        output_variable_only=True,
    )
    return {
        name: manager.get_variable(
            pipeline.uuid, block_uuid, name, partition=run.execution_partition,
            raise_exception=True,
        )
        for name in names
    }


def compare_runs(pipeline, unfused: PipelineRun, fused: PipelineRun) -> List[BlockResult]:
    stage_of = {
        uuid: index for index, stage in enumerate(_chains(fusion.fusion_plan(pipeline)), start=1)
        for uuid in stage
    }
    block_runs = [{b.block_uuid: b for b in run.block_runs} for run in (unfused, fused)]
    statuses = [{uuid: b.status for uuid, b in runs.items()} for runs in block_runs]

    def errors(uuid: str) -> str:
        texts = []
        for label, runs in zip(('block by block', 'fused'), block_runs):
            error = ((getattr(runs.get(uuid), 'metrics', None) or {}).get('error') or {})
            text = error.get('message') or error.get('error')
            if text and text != 'None':
                texts.append(f'{label}: {_first_lines(text, 6)}')
        return '\n'.join(texts)

    results = []
    for block in _ordered_blocks(pipeline):
        uuid = block.uuid
        stage = stage_of.get(uuid)
        before, after = statuses[0].get(uuid), statuses[1].get(uuid)
        if before != after:
            detail = f'Block run {before} block by block, {after} fused.'
            results.append(BlockResult(
                uuid, 'differs', '\n'.join(filter(None, [detail, errors(uuid)])), stage,
            ))
            continue
        if before != BlockRun.BlockRunStatus.COMPLETED:
            detail = f'Block run {before} in both runs.'
            results.append(BlockResult(
                uuid, 'failed' if before == BlockRun.BlockRunStatus.FAILED else 'not compared',
                '\n'.join(filter(None, [detail, errors(uuid)])), stage,
            ))
            continue
        try:
            expected = _outputs(pipeline, uuid, unfused)
            actual = _outputs(pipeline, uuid, fused)
        except Exception as error:
            results.append(BlockResult(
                uuid, 'not compared', f'Reading the outputs failed: {error}', stage,
            ))
            continue
        if list(expected) != list(actual):
            results.append(BlockResult(
                uuid, 'differs',
                f'Outputs {list(expected)} block by block, {list(actual)} fused.', stage,
            ))
            continue
        compared = []
        for name in expected:
            result, detail = compare_values(expected[name], actual[name])
            compared.append((result, f'{name}: {detail}' if detail else ''))
            if result in ('differs', 'not compared'):
                break
        result, detail = _combine(compared)
        skipped = [c for c in compared if c[0] == 'not compared']
        if result != 'differs' and skipped:
            result, detail = skipped[0]
        results.append(BlockResult(uuid, result, detail, stage))
    return results


def verify_fusion(
    pipeline,
    variables: Optional[Dict] = None,
    log: Callable[[str], None] = print,
    exact: bool = False,
) -> Verification:
    """
    Runs the pipeline block by block, then fused, from the same variables and execution
    date, and compares the outputs of each block. With exact, floats that differ only by
    rounding count as a difference.
    """
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.settings.platform import project_platform_activated
    from mage_ai.shared.dates import utc_now

    if pipeline.type != PipelineType.PYTHON:
        raise FusionVerificationError(
            f'Block fusion applies to standard (Python) pipelines; {pipeline.uuid} is a '
            f'{pipeline.type} pipeline.',
        )
    if project_platform_activated():
        raise FusionVerificationError('Verify fusion does not support the project platform.')
    if not fusion.fusion_enabled(pipeline, _Mode(fusion.BLOCK_FUSION_CHAINS)):
        raise FusionVerificationError(
            'Fusion cannot apply to this pipeline: it runs in one process '
            '(run_pipeline_in_one_process) or MEMORY_MANAGER_V2 is on.',
        )
    plan = fusion.fusion_plan(pipeline)
    if not any(len(stage) > 1 for stage in plan):
        log('No blocks of this pipeline form a chain, so fusion changes nothing; '
            'the runs compare anyway.')

    _cancel_unfinished(pipeline.uuid)
    execution_date = utc_now()
    runs = {}
    for mode in (fusion.BLOCK_FUSION_OFF, fusion.BLOCK_FUSION_CHAINS):
        run = _create_run(pipeline, mode, variables, execution_date)
        log(f'{TRIGGER_NAMES[mode]}: pipeline run {run.id}.')
        runs[mode] = run_in_process(run, log=log)

    verification = Verification(
        pipeline_uuid=pipeline.uuid,
        plan=plan,
        runs={mode: run.id for mode, run in runs.items()},
        statuses={mode: str(run.status) for mode, run in runs.items()},
        exact=exact,
    )
    verification.blocks = compare_runs(
        pipeline, runs[fusion.BLOCK_FUSION_OFF], runs[fusion.BLOCK_FUSION_CHAINS],
    )
    return verification


@dataclass
class _Mode:
    """A pipeline run stand-in that carries only a verification mode."""

    mode: str

    @property
    def metrics(self) -> Dict:
        return {fusion.VERIFY_FUSION_METRIC: self.mode}


def _chains(plan: List[List[str]]) -> List[List[str]]:
    return [stage for stage in plan if len(stage) > 1]


def format_report(verification: Verification) -> str:
    lines = []
    # Numbered as in the pipeline settings and the dependency graph: chains only.
    for index, stage in enumerate(_chains(verification.plan), start=1):
        lines.append(f'Stage {index}: {" -> ".join(stage)}')
    alone = [stage[0] for stage in verification.plan if len(stage) == 1]
    if alone:
        lines.append(f'Run alone: {", ".join(alone)}')
    lines.append('')
    width = max((len(b.block_uuid) for b in verification.blocks), default=0)
    for block in verification.blocks:
        line = f'  {block.block_uuid.ljust(width)}  {block.result}'
        if block.detail:
            detail = block.detail.replace('\n', '\n' + ' ' * (width + 6))
            line += f'\n{" " * (width + 4)}  {detail}'
        lines.append(line)
    lines.append('')
    first = verification.first_difference
    statuses = ', '.join(f'{TRIGGER_NAMES[m]} {s}' for m, s in verification.statuses.items())
    close = [b.block_uuid for b in verification.blocks if b.result == 'close']
    if verification.passed and close:
        lines.append(
            f'Fusion verified: outputs are the same, except float rounding in '
            f'{", ".join(close)} (at most {RELATIVE_TOLERANCE:g} relative). Use --exact to '
            f'count rounding as a difference.',
        )
    elif verification.passed:
        lines.append(f'Fusion verified: every output is the same. ({statuses})')
    elif first is not None:
        lines.append(
            f'First difference: {first.block_uuid} ({first.result}). Runs: '
            + ', '.join(f'{TRIGGER_NAMES[m]} #{i}' for m, i in verification.runs.items()),
        )
    else:
        lines.append(f'The runs did not complete: {statuses}.')
    return '\n'.join(lines)
