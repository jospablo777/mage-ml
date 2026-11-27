"""
The state of a pipeline's partitions (ADR-0039, PARTITIONS.md section 4), from its runs:

- missing: expected, and no run yet;
- queued, running: its latest run is initial or running;
- failed: its latest run failed or was cancelled;
- done: its latest run completed with the current code, and no upstream partition completed
  after it;
- stale: its latest run completed, but with code that changed since (and was not accepted),
  or a declared upstream pipeline's same partition completed after it.

The latest run of each partition comes from one grouped query over pipeline_run's
(pipeline_uuid, partition, id) index. The code a run had is its run record's digest; the code
now is the digest a run record would give it now (run_records.code_digest), kept a few
seconds. "Accept current code" records, per partition, the digest it was accepted with.
"""
import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

from sqlalchemy import func

from mage_ai.orchestration import partitions
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.orchestration.partitions import PartitionError

STATES = ('missing', 'queued', 'running', 'failed', 'stale', 'done')
Status = PipelineRun.PipelineRunStatus
CODE_SECONDS = 10
_codes: Dict[str, tuple] = {}


def _latest(pipeline_uuid: str, completed: bool = False) -> Dict[str, PipelineRun]:
    """The latest run (by id) of each partition of the pipeline; completed runs only when
    `completed`."""
    latest = PipelineRun.query.with_entities(
        PipelineRun.partition, func.max(PipelineRun.id).label('id'),
    ).filter(
        PipelineRun.pipeline_uuid == pipeline_uuid,
        PipelineRun.partition.isnot(None),
    )
    if completed:
        latest = latest.filter(PipelineRun.status == Status.COMPLETED)
    ids = [row.id for row in latest.group_by(PipelineRun.partition).all()]
    if not ids:
        return {}
    return {run.partition: run for run in PipelineRun.query.filter(PipelineRun.id.in_(ids))}


def _code_now(pipeline) -> Optional[str]:
    from mage_ai.orchestration import run_records

    if not run_records.enabled():
        return None
    cached = _codes.get(pipeline.uuid)
    if cached and time.monotonic() - cached[0] < CODE_SECONDS:
        return cached[1]
    digest = run_records.code_digest(pipeline)
    _codes[pipeline.uuid] = (time.monotonic(), digest)
    return digest


def _accepted_path(pipeline) -> str:
    return os.path.join(
        pipeline.variable_manager.pipeline_path(pipeline.uuid), '.partitions', 'accepted.json',
    )


def _accepted(pipeline) -> Dict[str, str]:
    try:
        with open(_accepted_path(pipeline), encoding='utf-8') as file:
            return json.load(file)
    except (OSError, ValueError):
        return {}


def accept_code(pipeline, values: Iterable[str]) -> List[str]:
    """Marks the partitions' runs as done with the code as it is now, without running them:
    for changes known not to change results. Returns the values accepted."""
    declaration = _declaration(pipeline)
    digest = _code_now(pipeline)
    if digest is None:
        raise PartitionError('Accepting code needs run records (MAGE_RUN_RECORDS on).')
    values = list(values)
    for value in values:
        partitions.run_variables(declaration, value)
    accepted = dict(_accepted(pipeline), **{value: digest for value in values})
    path = _accepted_path(pipeline)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temporary = f'{path}.{os.getpid()}.tmp'
    with open(temporary, 'w', encoding='utf-8') as file:
        json.dump(accepted, file, indent=2, sort_keys=True)
    os.replace(temporary, path)
    return values


def _declaration(pipeline):
    declaration = pipeline.partition_declaration
    if declaration is None:
        raise PartitionError(f'Pipeline {pipeline.uuid} declares no partitions.')
    return declaration


def _code_of(run: PipelineRun) -> Optional[str]:
    return ((run.metrics or {}).get('run_record') or {}).get('code')


def _moment(value: Optional[datetime]) -> Optional[datetime]:
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def states(pipeline, now: Optional[datetime] = None) -> Dict:
    """Each expected partition with its state, why it is stale, and its latest run; and the
    count of each state."""
    declaration = _declaration(pipeline)
    expected = partitions.expected(declaration, now)
    latest = _latest(pipeline.uuid)
    code = _code_now(pipeline)
    accepted = _accepted(pipeline)
    upstream = {name: _latest(name, completed=True) for name in declaration.upstream}
    rows = []
    for value in expected:
        run = latest.get(value)
        reasons = []
        if run is None:
            state = 'missing'
        elif run.status == Status.INITIAL:
            state = 'queued'
        elif run.status == Status.RUNNING:
            state = 'running'
        elif run.status != Status.COMPLETED:
            state = 'failed'
        else:
            ran_with = _code_of(run)
            if code and ran_with and ran_with != code and accepted.get(value) != code:
                reasons.append(dict(kind='code', ran_with=ran_with, now=code))
            finished = _moment(run.completed_at)
            for name, done in upstream.items():
                other = done.get(value)
                if other is not None and finished is not None \
                        and _moment(other.completed_at) > finished:
                    reasons.append(dict(kind='upstream', pipeline=name, run_id=other.id))
            state = 'stale' if reasons else 'done'
        rows.append(dict(
            partition=value,
            state=state,
            stale_because=reasons,
            run_id=run.id if run else None,
            completed_at=run.completed_at.isoformat() if run and run.completed_at else None,
        ))
    counts = {state: 0 for state in STATES}
    for row in rows:
        counts[row['state']] += 1
    return dict(key=declaration.key, kind=declaration.kind, partitions=rows, counts=counts,
                total=len(rows))


def select(pipeline, choice, now: Optional[datetime] = None) -> List[str]:
    """The partitions an action names: 'missing', 'stale', 'failed', or a list of values."""
    if isinstance(choice, (list, tuple)):
        declaration = _declaration(pipeline)
        for value in choice:
            partitions.run_variables(declaration, value)
        return list(choice)
    if choice not in ('missing', 'stale', 'failed'):
        raise PartitionError('Select missing, stale, failed, or a list of partitions.')
    return [row['partition'] for row in states(pipeline, now)['partitions']
            if row['state'] == choice]


def check(pipeline) -> List[str]:
    """What is wrong with the pipeline's partitions declaration, for the Compile panel: its
    own errors, and each upstream pipeline that does not exist or has other partitions."""
    from mage_ai.data_preparation.models.pipeline import Pipeline

    try:
        declaration = pipeline.partition_declaration
    except PartitionError as error:
        return [str(error)]
    if declaration is None:
        return []
    problems = []
    for name in declaration.upstream:
        other = Pipeline.get(name, repo_path=pipeline.repo_path, check_if_exists=True)
        if other is None:
            problems.append(f'partitions: upstream pipeline {name} does not exist.')
            continue
        try:
            theirs = other.partition_declaration
        except PartitionError:
            theirs = None
        same = theirs is not None and (theirs.kind, theirs.values, theirs.zone, theirs.start) \
            == (declaration.kind, declaration.values, declaration.zone, declaration.start)
        if not same:
            problems.append(
                f'partitions: upstream pipeline {name} has other partitions; an upstream '
                'pipeline has the same kind, start, time zone and values, so a partition of '
                'one is the same slice as in the other.'
            )
    return problems
