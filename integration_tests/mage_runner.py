"""
Run the pipelines in integration_tests/project through Mage's trigger, scheduler and
executor, in a temporary project with its own metadata database.
"""
import logging
import shutil
import types
from pathlib import Path
from typing import Dict

import pytest

PROJECT = Path(__file__).resolve().parent / 'project'


# One project per session. xdist gives a worker tests from several modules in turn, so a
# module-scoped project was removed and created again mid-run, and a connection left on
# the removed database failed with "attempt to write a readonly database".
@pytest.fixture(scope='session')
def mage_project():
    from mage_ai.orchestration.db import db_connection
    from mage_ai.orchestration.db.database_manager import database_manager
    from mage_ai.tests.base_test import (
        drop_test_db,
        remove_test_directory,
        set_up_test_repo,
    )

    project = types.SimpleNamespace()
    set_up_test_repo(project)
    shutil.copytree(PROJECT, project.repo_path, dirs_exist_ok=True)
    database_manager.run_migrations(log_level=logging.ERROR)
    db_connection.start_session(force=True)
    yield project.repo_path
    db_connection.close_session()
    drop_test_db()
    remove_test_directory(project._test_directory)


def execute_pipeline(pipeline_uuid: str, **variables):
    """Run a pipeline to its end and return the pipeline run, whatever its status."""
    from mage_ai.data_preparation.executors.pipeline_executor import PipelineExecutor
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
    from mage_ai.orchestration.triggers.api import trigger_pipeline

    run = trigger_pipeline(pipeline_uuid, variables=variables)
    scheduler = PipelineScheduler(run)
    scheduler.start(should_schedule=False)
    error = None
    try:
        PipelineExecutor(
            Pipeline.get(pipeline_uuid),
            execution_partition=run.execution_partition,
        ).execute(
            pipeline_run_id=run.id,
            global_vars=run.get_variables(),
            allow_blocks_to_fail=False,
        )
    except BaseException as err:
        # A failed block is re-raised by the executor. BaseException covers Rust panics
        # from Polars, which do not derive from Exception.
        if isinstance(err, (KeyboardInterrupt, SystemExit)):
            raise
        error = err
    scheduler.schedule()
    run.refresh()
    run.executor_error = error
    return run


def run_integration_pipeline(pipeline_uuid: str, **variables):
    """
    Run a data integration pipeline as the scheduler does, in this process: each stream of
    its catalog through run_integration_stream; every block run must complete.
    """
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
    from mage_ai.orchestration.pipeline_scheduler_original import run_integration_stream
    from mage_ai.orchestration.triggers.api import trigger_pipeline
    from mage_ai.shared.dates import utc_now

    run = trigger_pipeline(pipeline_uuid, variables=variables)
    scheduler = PipelineScheduler(run)
    scheduler.start(should_schedule=False)
    run.refresh()
    block_runs = {b.id for b in run.block_runs}
    runtime_arguments = dict(
        _execution_date=utc_now().isoformat(),
        _execution_partition=run.execution_partition,
    )
    for stream in scheduler.streams:
        run_integration_stream(
            stream, block_runs, {}, runtime_arguments, run.id, run.get_variables(),
        )
    scheduler.schedule()
    run.refresh()
    statuses = block_statuses(run)
    assert set(statuses.values()) == {'completed'}, statuses
    assert run.status == PipelineRun.PipelineRunStatus.COMPLETED, run.status
    return run


def block_statuses(run) -> Dict[str, str]:
    return {b.block_uuid: b.status.value for b in run.block_runs}


def run_pipeline(pipeline_uuid: str, **variables):
    """Run a pipeline and require every block to complete."""
    from mage_ai.orchestration.db.models.schedules import PipelineRun

    run = execute_pipeline(pipeline_uuid, **variables)
    statuses = block_statuses(run)
    assert set(statuses.values()) == {'completed'}, statuses
    assert run.status == PipelineRun.PipelineRunStatus.COMPLETED
    return run


def run_scheduled_pipeline(pipeline_uuid: str, **variables):
    """Run a pipeline through the scheduler, its jobs in this process: data integration
    blocks in batch pipelines need the scheduler's callbacks and controller runs."""
    from unittest.mock import patch

    from mage_ai.orchestration import pipeline_scheduler_original
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
    from mage_ai.orchestration.triggers.api import trigger_pipeline

    run = trigger_pipeline(pipeline_uuid, variables=variables)
    jobs = []
    job_manager = pipeline_scheduler_original.get_job_manager()
    with patch.object(job_manager, 'add_job', side_effect=lambda _type, _uid, target, *a, **k:
                      jobs.append((target, a, k))), \
            patch.object(job_manager, 'has_pipeline_run_job', return_value=False), \
            patch.object(job_manager, 'has_block_run_job', return_value=False):
        scheduler = PipelineScheduler(run)
        scheduler.start(should_schedule=False)
        for _ in range(60):
            scheduler.schedule()
            run.refresh()
            if run.status != PipelineRun.PipelineRunStatus.RUNNING:
                break
            while jobs:
                target, args, kwargs = jobs.pop(0)
                target(*args, **kwargs)
    statuses = {b.block_uuid: b.status.value for b in run.block_runs}
    assert run.status == PipelineRun.PipelineRunStatus.COMPLETED, statuses
    return run
