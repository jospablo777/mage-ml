"""
Partitions that run when they close (ADR-0039): a pipeline whose partitions declaration has
`schedule: on_close` gets a queued run for each time partition that has closed and never ran,
the newest first and at most `catch_up` at a time, at each scheduler tick. After downtime it
catches up the partitions that closed meanwhile. A partition that ran and failed is not run
again by itself: its state is failed, and Run failed runs it.

The first time it is turned on, every partition from the start has never run: only the newest
`catch_up` are queued then, and the older ones wait for Run missing, so a pipeline declared
with a start two years back does not queue 730 runs.

With `schedule: on_upstream`, a partition is queued once the same partition of every upstream
pipeline is done, when it never ran or is stale only because an upstream one ran again after
it: a chain of pipelines follows its first one, and repairing upstream partitions repairs the
ones after them. A partition stale because the code changed still waits for Run stale, and a
failed one for Run failed.
"""
from datetime import datetime
from typing import Iterable, List, Optional

from mage_ai.orchestration import partitions
from mage_ai.orchestration.db.models.schedules import PipelineRun


def due(pipeline, now: Optional[datetime] = None) -> List[str]:
    """The partitions to queue now, at most catch_up: on_close, closed and never run, newest
    first; on_upstream, those whose upstream partitions are done (_upstream_due)."""
    declaration = pipeline.partition_declaration
    if declaration is not None and declaration.schedule == 'on_upstream':
        return _upstream_due(pipeline, declaration, now)
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


def _upstream_due(pipeline, declaration, now: Optional[datetime]) -> List[str]:
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.orchestration import partition_state

    done_upstream = None
    for name in declaration.upstream:
        upstream = Pipeline.get(name, repo_path=pipeline.repo_path, check_if_exists=True)
        if upstream is None or upstream.partition_declaration is None:
            # The Compile panel says which; nothing runs until it is fixed.
            return []
        done = {row['partition'] for row in partition_state.states(upstream, now)['partitions']
                if row['state'] == 'done'}
        done_upstream = done if done_upstream is None else done_upstream & done
    rows = partition_state.states(pipeline, now)['partitions']
    if declaration.kind != 'values':
        rows = list(reversed(rows))
    ready = [
        row['partition'] for row in rows
        if row['partition'] in (done_upstream or set())
        and (row['state'] == 'missing'
             or (row['state'] == 'stale'
                 and all(r['kind'] == 'upstream' for r in row['stale_because'])))
    ]
    return ready[:declaration.catch_up]


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
