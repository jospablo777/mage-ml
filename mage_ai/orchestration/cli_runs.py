"""
Command line runs (`mage run`) recorded as pipeline runs, so run-diff, reproduce and the
runs page see them (I-103).

The run is recorded when it ends, with its final status: a pipeline run created while it
ran would be RUNNING, and the scheduler of a server on the same database would take it and
run its blocks again. The code and environment are hashed when the run starts
(run_records.prepare), so the record shows what it started with. Runs go under a trigger
of the pipeline named "Command line", which is inactive and starts nothing.
"""
import os
import socket
from datetime import datetime, timezone
from typing import Dict, List, Optional

TRIGGER_NAME = 'Command line'


def _trigger(pipeline):
    from mage_ai.data_preparation.models.triggers import ScheduleStatus, ScheduleType
    from mage_ai.orchestration.db.models.schedules import PipelineSchedule

    trigger = PipelineSchedule.query.filter(
        PipelineSchedule.pipeline_uuid == pipeline.uuid,
        PipelineSchedule.name == TRIGGER_NAME,
    ).first()
    if trigger is None:
        trigger = PipelineSchedule.create(
            name=TRIGGER_NAME,
            description='Runs started with mage run.',
            pipeline_uuid=pipeline.uuid,
            schedule_type=ScheduleType.API,
            status=ScheduleStatus.INACTIVE,
            repo_path=pipeline.repo_path,
        )
    return trigger


def record(
    pipeline,
    prepared: Optional[Dict],
    summary: List[Dict],
    variables: Dict,
    started_at: datetime,
    failed: bool,
    execution_partition: Optional[str] = None,
):
    """
    Records a finished command line run: its pipeline run and block runs with their
    statuses, times and output digests, and its run record. Returns the pipeline run.
    """
    from mage_ai.orchestration import run_records
    from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun
    from mage_ai.orchestration.fusion import executable

    trigger = _trigger(pipeline)
    completed_at = datetime.now(timezone.utc)
    pipeline_run = PipelineRun.create(
        create_block_runs=False,
        pipeline_schedule_id=trigger.id,
        pipeline_uuid=pipeline.uuid,
        execution_date=started_at,
        started_at=started_at,
        completed_at=completed_at,
        status=PipelineRun.PipelineRunStatus.FAILED if failed
        else PipelineRun.PipelineRunStatus.COMPLETED,
        variables=dict(variables or {}),
        metrics=dict(
            command_line=dict(host=socket.gethostname(), pid=os.getpid()),
            execution_partition=execution_partition,
        ),
    )
    rows = {row['block']: row for row in summary}
    for block in pipeline.blocks_by_uuid.values():
        if not executable(block):
            continue
        row = rows.get(block.uuid)
        metrics = {}
        if row is None:
            status = BlockRun.BlockRunStatus.UPSTREAM_FAILED if failed \
                else BlockRun.BlockRunStatus.CANCELLED
        elif row.get('failed'):
            status = BlockRun.BlockRunStatus.FAILED
        else:
            status = BlockRun.BlockRunStatus.COMPLETED
            try:
                metrics['outputs'] = run_records.output_digests(
                    pipeline, block.uuid, execution_partition,
                )
            except Exception as error:
                metrics['outputs_error'] = str(error)
        BlockRun.create(
            pipeline_run_id=pipeline_run.id,
            block_uuid=block.uuid,
            status=status,
            started_at=(row or {}).get('started_at'),
            completed_at=(row or {}).get('completed_at'),
            metrics=metrics or None,
        )
    if prepared is not None:
        run_records.write_manifest(pipeline, pipeline_run, prepared)
    return pipeline_run
