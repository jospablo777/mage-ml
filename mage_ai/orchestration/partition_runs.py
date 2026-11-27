"""
Runs of a pipeline's partitions (ADR-0039): a partition's run is an ordinary pipeline run with
the partition's variables, its execution_date and its own output folder, recorded in
pipeline_run.partition. A partition has at most one active run.
"""
from typing import Tuple

from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.orchestration.partitions import (
    PartitionError,
    run_start,
    run_variables,
)

SCHEDULE_NAME = 'Partitions'
ACTIVE = (PipelineRun.PipelineRunStatus.INITIAL, PipelineRun.PipelineRunStatus.RUNNING)


def _active(pipeline_uuid: str, value: str):
    return (
        PipelineRun.query.filter(
            PipelineRun.pipeline_uuid == pipeline_uuid,
            PipelineRun.partition == value,
            PipelineRun.status.in_(ACTIVE),
        )
        .order_by(PipelineRun.id)
        .all()
    )


def run_partition(pipeline, value: str) -> Tuple[PipelineRun, bool]:
    """
    A run of the partition: its active run when it has one (and False), else a new queued
    run (and True), which the scheduler starts within the pipeline's concurrency.
    """
    from mage_ai.orchestration.triggers import api
    from mage_ai.orchestration.triggers.utils import create_and_start_pipeline_run

    declaration = pipeline.partition_declaration
    if declaration is None:
        raise PartitionError(f'Pipeline {pipeline.uuid} declares no partitions.')
    variables = run_variables(declaration, value)
    active = _active(pipeline.uuid, value)
    if active:
        return active[0], False
    # The API trigger the partitions' runs belong to, made once per pipeline.
    schedule = getattr(api, '__fetch_or_create_pipeline_schedule')(
        pipeline, schedule_name=SCHEDULE_NAME,
    )
    payload = dict(variables=variables, partition=value)
    start = run_start(declaration, value)
    if start is not None:
        payload['execution_date'] = start
    run = create_and_start_pipeline_run(pipeline, schedule, payload)
    # Two requests at once can both have found no active run: the first run created stands
    # and the later one is cancelled before it starts.
    active = _active(pipeline.uuid, value)
    if active and active[0].id != run.id:
        run.update(status=PipelineRun.PipelineRunStatus.CANCELLED)
        return active[0], False
    return run, True
