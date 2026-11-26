import os


def run(
    pipeline_uuid: str,
    project_path: str = None,
    block_uuid: str = None,
    run_sensors: bool = True,
    **global_vars,
) -> None:
    """
    Runs a pipeline, or one of its blocks, in this process. Keyword arguments are runtime
    variables, over the pipeline's own variables as `mage run` takes them.
    """
    from mage_ai.data_preparation.executors.executor_factory import ExecutorFactory
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.data_preparation.variable_manager import get_global_variables
    from mage_ai.shared.hash import merge_dict

    project_path = os.getcwd() if project_path is None else os.path.abspath(project_path)
    from mage_ai.settings.repo import add_project_to_sys_path

    add_project_to_sys_path(project_path)
    pipeline = Pipeline(pipeline_uuid, project_path)
    # Without them a block that reads a pipeline variable failed here and ran with `mage run`.
    global_vars = merge_dict(
        get_global_variables(pipeline_uuid, pipeline=pipeline, repo_path=project_path) or {},
        global_vars,
    )
    if block_uuid is None:
        ExecutorFactory.get_pipeline_executor(pipeline).execute(
            analyze_outputs=False,
            global_vars=global_vars,
            run_sensors=run_sensors,
            update_status=False,
        )
    else:
        ExecutorFactory.get_block_executor(pipeline, block_uuid).execute(
            analyze_outputs=False,
            global_vars=global_vars,
            update_status=False,
        )
