"""
Which pipeline run started which: a run that a block starts with trigger_pipeline records
the run of that block, and cancelling a run cancels the runs it started (upstream #5641).
"""
from contextvars import ContextVar
from typing import List, Optional

PARENT_KEY = 'parent_pipeline_run_id'

# The pipeline run whose block runs in this thread.
current_pipeline_run_id: ContextVar[Optional[int]] = ContextVar(
    'mage_pipeline_run_id', default=None,
)


def active_children(pipeline_run_id: int) -> List:
    """The waiting or running pipeline runs that the run's blocks started."""
    from mage_ai.orchestration.db.models.schedules import PipelineRun

    runs = PipelineRun.query.filter(PipelineRun.status.in_([
        PipelineRun.PipelineRunStatus.INITIAL,
        PipelineRun.PipelineRunStatus.RUNNING,
    ])).all()
    return [
        run for run in runs
        if isinstance(run.metrics, dict) and run.metrics.get(PARENT_KEY) == pipeline_run_id
    ]
