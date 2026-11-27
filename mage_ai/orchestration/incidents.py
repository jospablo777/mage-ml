"""
What a failed or cancelled pipeline run's page shows first (the incident panel of the run
record): each failed block with its error, the end of its log and how long it ran against
its usual time, its upstream blocks' states in the run, the blocks that did not complete,
and the recovery actions the pipeline run API offers.

It reads recorded state only (block runs, their metrics and logs) and starts nothing.
"""
import json
import re
import statistics
from typing import Dict, List, Optional

# The lines of a failed block's log the panel shows: its end, where the error is.
LOG_LINES = 60
# The completed runs of a block its usual time is taken from, the latest.
USUAL_RUNS = 20

FAILED = 'failed'
# Blocks a failed run left without completing: a failed run cancels the blocks it had not
# run. A block whose condition skipped it is not among them.
NOT_COMPLETED = ('initial', 'queued', 'running', 'cancelled', 'upstream_failed')


def _status(block_run) -> str:
    status = block_run.status
    return getattr(status, 'value', status)


def _ended_at(block_run):
    """When the block run ended: a failed one has no completed_at, and its last update is
    the failure."""
    if block_run.completed_at is not None:
        return block_run.completed_at
    if _status(block_run) == FAILED:
        return block_run.updated_at
    return None


def _seconds(block_run) -> Optional[float]:
    ended = _ended_at(block_run)
    if block_run.started_at is None or ended is None:
        return None
    started = block_run.started_at
    if (started.tzinfo is None) != (ended.tzinfo is None):
        # SQLite returns naive times where PostgreSQL returns aware ones.
        started, ended = started.replace(tzinfo=None), ended.replace(tzinfo=None)
    return round((ended - started).total_seconds(), 3)


def usual_seconds(pipeline_uuid: str, block_uuid: str, before_id: int,
                  limit: int = USUAL_RUNS) -> Dict:
    """
    The median time of the block's latest completed runs in the pipeline before block run
    `before_id`, and how many it is taken from; None without any.
    """
    from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun

    rows = (
        BlockRun.query.with_entities(BlockRun.started_at, BlockRun.completed_at)
        .join(PipelineRun, BlockRun.pipeline_run_id == PipelineRun.id)
        .filter(
            PipelineRun.pipeline_uuid == pipeline_uuid,
            BlockRun.block_uuid == block_uuid,
            BlockRun.status == BlockRun.BlockRunStatus.COMPLETED,
            BlockRun.id < before_id,
            BlockRun.started_at.isnot(None),
            BlockRun.completed_at.isnot(None),
        )
        .order_by(BlockRun.id.desc())
        .limit(limit)
        .all()
    )
    durations = [(done - started).total_seconds() for started, done in rows]
    return dict(
        seconds=round(statistics.median(durations), 3) if durations else None,
        runs=len(durations),
    )


_BLOCK_FAILED = re.compile(r'^Block \S+ \(.*\) failed\.$')


def error_line(error: Optional[str]) -> Optional[str]:
    """
    The line of an error that says what it was: the last line of a traceback that is not
    indented (`KeyError: 'amount'`), else its first line.
    """
    if not error:
        return None
    lines = [line.rstrip() for line in str(error).splitlines() if line.strip()]
    # MageML ends a block's traceback with `Block <uuid> (<file>) failed.`.
    while lines and _BLOCK_FAILED.match(lines[-1]):
        lines.pop()
    if not lines:
        return None
    if lines[0].startswith('Traceback'):
        for line in reversed(lines):
            if not line.startswith((' ', '\t')):
                return line
    return lines[0]


# A block's log line: its time, then the JSON the logger wrote.
_LOG_LINE = re.compile(r'^(\S+) (\{.*\})\s*$')


def readable(content: str) -> str:
    """A log's lines as time, level and message; lines that are not JSON stay as they are."""
    out = []
    for line in content.splitlines():
        match = _LOG_LINE.match(line)
        if match:
            try:
                record = json.loads(match.group(2))
            except ValueError:
                record = None
            if isinstance(record, dict) and 'message' in record:
                level = record.get('level') or ''
                out.append(f"{match.group(1)} {level} {record['message']}".replace('  ', ' '))
                continue
        out.append(line)
    return '\n'.join(out) + ('\n' if out else '')


def _log_end(pipeline, run, block_run, lines: int) -> Optional[Dict]:
    """The last lines of the block run's log, over its rotated segments."""
    from mage_ai.data_preparation.logging import log_windows
    from mage_ai.data_preparation.logging.logger_manager_factory import (
        LoggerManagerFactory,
    )
    from mage_ai.shared.utils import clean_name

    try:
        path = LoggerManagerFactory.get_logger_manager(
            pipeline_uuid=pipeline.uuid,
            block_uuid=clean_name(block_run.block_uuid),
            partition=run.execution_partition,
            repo_config=pipeline.repo_config,
        ).get_log_filepath()
        item = log_windows.window(path, lines=lines)
    except Exception:
        # A log that cannot be read leaves the rest of the panel.
        return None
    if not item.get('content'):
        return None
    return dict(
        content=readable(item['content']),
        first_line=item['first_line'],
        total_lines=item['total_lines'],
        path=path,
    )


def _upstream_partitions(run, pipeline) -> List[Dict]:
    """For a partition's run: the same partition of each upstream pipeline the partitions
    declare, its latest run and the latest that completed."""
    partition = getattr(run, 'partition', None)
    if not partition:
        return []
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.orchestration.partitions import PartitionError

    try:
        declaration = pipeline.partition_declaration
    except PartitionError:
        return []
    if declaration is None or not declaration.upstream:
        return []
    rows = []
    for name in declaration.upstream:
        runs = PipelineRun.query.filter(
            PipelineRun.pipeline_uuid == name,
            PipelineRun.partition == partition,
        ).order_by(PipelineRun.id.desc())
        latest = runs.first()
        done = runs.filter(PipelineRun.status == PipelineRun.PipelineRunStatus.COMPLETED).first()
        rows.append(dict(
            pipeline_uuid=name,
            partition=partition,
            status=_status(latest) if latest else None,
            run_id=latest.id if latest else None,
            completed_run_id=done.id if done else None,
        ))
    return rows


def _failed_monitors(block_runs) -> List[Dict]:
    """The monitors of the run that failed (monitors.py), each with its failed checks."""
    failed = []
    for block_run in sorted(block_runs, key=lambda br: br.id):
        for monitor in (block_run.metrics or {}).get('monitors') or []:
            if monitor.get('result') not in ('fail', 'error'):
                continue
            failed.append(dict(
                block_uuid=block_run.block_uuid,
                monitor=monitor.get('monitor'),
                result=monitor.get('result'),
                error=monitor.get('error'),
                checks=[
                    dict(check=c.get('check'), column=c.get('column'), detail=c.get('detail'))
                    for c in monitor.get('checks') or []
                    if c.get('result') in ('fail', 'error')
                ][:5],
            ))
    return failed


def summary(run, pipeline, log_lines: int = LOG_LINES) -> Optional[Dict]:
    """The incident panel of a failed or cancelled run; None for another run."""
    if str(getattr(run.status, 'value', run.status)) not in ('failed', 'cancelled'):
        return None
    block_runs = list(run.block_runs)
    states = {br.block_uuid: _status(br) for br in block_runs}
    blocks: List[Dict] = []
    for block_run in sorted(block_runs, key=lambda br: br.id):
        if _status(block_run) != FAILED:
            continue
        error = (block_run.metrics or {}).get('error') or {}
        block = pipeline.get_block(block_run.block_uuid)
        upstream = block.upstream_block_uuids if block is not None else []
        blocks.append(dict(
            block_uuid=block_run.block_uuid,
            block_run_id=block_run.id,
            error=error.get('message') or error.get('error'),
            error_line=error_line(error.get('message') or error.get('error')),
            started_at=block_run.started_at.isoformat() if block_run.started_at else None,
            ended_at=_ended_at(block_run).isoformat() if _ended_at(block_run) else None,
            seconds=_seconds(block_run),
            usual=usual_seconds(run.pipeline_uuid, block_run.block_uuid, block_run.id),
            log=_log_end(pipeline, run, block_run, log_lines),
            upstream={uuid: states.get(uuid) for uuid in upstream},
        ))
    not_completed = [
        dict(block_uuid=br.block_uuid, status=_status(br))
        for br in sorted(block_runs, key=lambda br: br.block_uuid)
        if _status(br) in NOT_COMPLETED
    ]
    recovery = []
    if blocks or not_completed:
        recovery.append(dict(
            action='retry_incomplete',
            label='Retry the blocks that did not complete',
            blocks=sorted(
                uuid for uuid, state in states.items()
                if state not in ('completed', 'condition_failed')
            ),
        ))
    recovery += [
        dict(
            action='retry_from',
            block_uuid=block['block_uuid'],
            label=f"Retry from {block['block_uuid']} and the blocks after it",
        )
        for block in blocks
    ]
    return dict(
        blocks=blocks,
        not_completed=not_completed,
        recovery=recovery,
        monitors=_failed_monitors(block_runs),
        upstream_partitions=_upstream_partitions(run, pipeline),
    )


def recent(days: int = 7, limit: int = 200) -> List[Dict]:
    """
    The pipelines with a failed run in the last `days`, the latest failure first: how many
    runs failed, the latest one's failed blocks and error lines, and whether a later run
    completed (recovered) or the pipeline is still failing. From the run records only, no
    logs, so the list stays fast.
    """
    from datetime import datetime, timedelta, timezone

    from mage_ai.orchestration.db.models.schedules import PipelineRun

    Status = PipelineRun.PipelineRunStatus
    since = datetime.now(timezone.utc) - timedelta(days=days)
    failed = (
        PipelineRun.query
        .filter(PipelineRun.status == Status.FAILED, PipelineRun.created_at >= since)
        .order_by(PipelineRun.id.desc())
        .limit(limit)
        .all()
    )
    by_pipeline: Dict[str, List] = {}
    for run in failed:
        by_pipeline.setdefault(run.pipeline_uuid, []).append(run)
    rows = []
    for pipeline_uuid, runs in by_pipeline.items():
        latest = runs[0]
        later = (
            PipelineRun.query.with_entities(PipelineRun.id)
            .filter(
                PipelineRun.pipeline_uuid == pipeline_uuid,
                PipelineRun.status == Status.COMPLETED,
                PipelineRun.id > latest.id,
            )
            .order_by(PipelineRun.id.desc())
            .first()
        )
        blocks = []
        for block_run in sorted(latest.block_runs, key=lambda br: br.id):
            if _status(block_run) != FAILED:
                continue
            error = (block_run.metrics or {}).get('error') or {}
            blocks.append(dict(
                block_uuid=block_run.block_uuid,
                error_line=error_line(error.get('message') or error.get('error')),
            ))
        rows.append(dict(
            id=pipeline_uuid,
            pipeline_uuid=pipeline_uuid,
            failed_runs=len(runs),
            latest_run_id=latest.id,
            latest_at=latest.created_at.isoformat() if latest.created_at else None,
            partition=getattr(latest, 'partition', None),
            blocks=blocks,
            recovered_by=later.id if later else None,
        ))
    # Still failing first, then the newest failure.
    rows.sort(key=lambda r: (r['recovered_by'] is not None, -r['latest_run_id']))
    return rows
