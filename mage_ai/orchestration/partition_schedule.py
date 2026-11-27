"""
Partitions that run when they close (ADR-0039): a pipeline whose partitions declaration has
`schedule: on_close` gets a queued run for each time partition that has closed and never ran,
the newest first and at most `catch_up` at a time, at each scheduler tick. After downtime it
catches up the partitions that closed meanwhile. A partition that ran and failed is not run
again by itself: its state is failed, and Run failed runs it.

The first time it is turned on, every partition from the start has never run: only the newest
`catch_up` are queued then, and the older ones wait for Run missing, so a pipeline declared
with a start two years back does not queue 730 runs.
"""
from datetime import datetime
from typing import Iterable, List, Optional

from mage_ai.orchestration import partitions
from mage_ai.orchestration.db.models.schedules import PipelineRun


def due(pipeline, now: Optional[datetime] = None) -> List[str]:
    """The partitions to queue now: closed, never run, newest first, at most catch_up."""
    declaration = pipeline.partition_declaration
    if declaration is None or declaration.schedule != 'on_close':
        return []
    ran = {
        row.partition for row in PipelineRun.query.with_entities(PipelineRun.partition).filter(
            PipelineRun.pipeline_uuid == pipeline.uuid,
            PipelineRun.partition.isnot(None),
        ).distinct()
    }
    newest = list(reversed(partitions.expected(declaration, now)))[:declaration.catch_up]
    return [value for value in newest if value not in ran]


def tick(pipelines: Iterable, now: Optional[datetime] = None) -> List[str]:
    """Queues the due partitions of each pipeline; returns '<pipeline>/<partition>' queued.
    A pipeline whose declaration does not parse is left out (the Compile panel says why)."""
    from mage_ai.orchestration.partition_runs import run_partition
    from mage_ai.orchestration.partitions import PartitionError

    queued = []
    for pipeline in pipelines:
        if not getattr(pipeline, 'partitions', None):
            continue
        try:
            values = due(pipeline, now)
        except PartitionError:
            continue
        for value in values:
            run_partition(pipeline, value)
            queued.append(f'{pipeline.uuid}/{value}')
    return queued
