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


@pytest.fixture(scope='module')
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
