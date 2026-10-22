"""
A time trigger with skip_if_previous_running, as schedule_all runs it: while the run of the
previous hour still runs, this hour's run is created cancelled (the run's log says why),
and the running one is left alone.
"""
import uuid
from datetime import timedelta
from unittest.mock import patch


def schedule_once():
    from mage_ai.orchestration import pipeline_scheduler_original
    from mage_ai.orchestration.pipeline_scheduler_original import schedule_all

    job_manager = pipeline_scheduler_original.get_job_manager()
    with patch.object(job_manager, 'add_job'), \
            patch.object(job_manager, 'has_pipeline_run_job', return_value=False), \
            patch.object(job_manager, 'has_block_run_job', return_value=False):
        schedule_all()


def test_a_run_is_skipped_while_the_previous_one_runs(mage_project):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.data_preparation.models.triggers import (
        ScheduleInterval,
        ScheduleStatus,
        ScheduleType,
    )
    from mage_ai.orchestration.db.models.schedules import PipelineRun, PipelineSchedule
    from mage_ai.shared.dates import utc_now

    name = f'skip_{uuid.uuid4().hex[:8]}'
    pipeline = Pipeline.create(name, repo_path=mage_project)
    Block.create(f'{name}_load', 'data_loader', mage_project, pipeline=pipeline)
    now = utc_now()
    schedule = PipelineSchedule.create(
        name=name, pipeline_uuid=pipeline.uuid, schedule_type=ScheduleType.TIME,
        schedule_interval=ScheduleInterval.HOURLY, status=ScheduleStatus.ACTIVE,
        start_time=now - timedelta(days=1), settings=dict(skip_if_previous_running=True),
        repo_path=mage_project,
    )
    this_hour = schedule.current_execution_date()
    # With its block run: a running run without any is complete.
    running = PipelineRun.create(
        pipeline_schedule_id=schedule.id, pipeline_uuid=pipeline.uuid,
        execution_date=this_hour - timedelta(hours=1),
    )
    running.update(status=PipelineRun.PipelineRunStatus.RUNNING)
    assert len(running.block_runs) == 1

    schedule_once()

    runs = PipelineRun.query.filter(PipelineRun.pipeline_schedule_id == schedule.id).all()
    skipped = [r for r in runs if r.id != running.id]
    assert len(skipped) == 1, [(r.id, r.status, r.execution_date) for r in runs]
    assert skipped[0].status == PipelineRun.PipelineRunStatus.CANCELLED
    assert skipped[0].execution_date.replace(tzinfo=None) == this_hour.replace(tzinfo=None)
    running.refresh()
    assert running.status == PipelineRun.PipelineRunStatus.RUNNING
