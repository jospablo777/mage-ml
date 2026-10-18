import json
import os
import sys
from typing import List, Union

import typer
from click import Context
from rich import print
from rich.markup import escape
from typer.core import TyperGroup

from mage_ai.cli.utils import parse_runtime_variables, parse_var_options
from mage_ai.data_preparation.repo_manager import ProjectType
from mage_ai.services.newrelic import initialize_new_relic
from mage_ai.settings.repo import add_project_to_sys_path
from mage_ai.shared.constants import ENV_VAR_INSTANCE_TYPE, InstanceType


def _e(value) -> str:
    """A value printed with rich, which would drop text in brackets such as [dependencies]."""
    return escape(str(value))


class OrderCommands(TyperGroup):
    def list_commands(self, ctx: Context):
        """Return list of commands in the order they appear."""
        return list(self.commands)


app = typer.Typer(
    cls=OrderCommands,
    pretty_exceptions_show_locals=False,
)

# Defaults

INIT_PROJECT_PATH_DEFAULT = typer.Argument(..., help='path of the MageML project to be created.')
INIT_PROJECT_TYPE_DEFAULT = typer.Option(
    ProjectType.STANDALONE.value,
    help='type of project to create, options are main, sub, or standalone',
)
INIT_CLUSTER_TYPE_DEFAULT = typer.Option(
    None,
    help='type of instance to create for workspace management',
)
INIT_PROJECT_UUID_DEFAULT = typer.Option(
    None,
    help='project uuid for the new project',
)

START_PROJECT_PATH_DEFAULT = typer.Argument(
    os.getcwd(), help='path of the MageML project to be loaded.'
)
START_HOST_DEFAULT = typer.Option('localhost', help='specify the host.')
START_PORT_DEFAULT = typer.Option('6789', help='specify the port.')
START_MANAGE_INSTANCE_DEFAULT = typer.Option('0', help='')
START_DBT_DOCS_INSTANCE_DEFAULT = typer.Option('0', help='')
START_INSTANCE_TYPE_DEFAULT = typer.Option(
    InstanceType.SERVER_AND_SCHEDULER.value, help='specify the instance type.'
)
START_PROJECT_TYPE_DEFAULT = typer.Option(
    ProjectType.STANDALONE.value,
    help='create project of this type if does not exist, options are main, sub, or standalone',
)
START_CLUSTER_TYPE_DEFAULT = typer.Option(
    None,
    help='type of instance to create for workspace management',
)
START_PROJECT_UUID_DEFAULT = typer.Option(
    None,
    help='set project uuid for the repo that is being started',
)

RUN_PROJECT_PATH_DEFAULT = typer.Argument(
    ..., help='path of the MageML project that contains the pipeline.'
)
RUN_PIPELINE_UUID_DEFAULT = typer.Argument(..., help='uuid of the pipeline to be run.')
RUN_TEST_DEFAULT = typer.Option(False, help='specify if tests should be run.')
RUN_BLOCK_UUID_DEFAULT = typer.Option(None, help='uuid of the block to be run.')
RUN_EXECUTION_PARTITION_DEFAULT = typer.Option(None, help='')
RUN_EXECUTOR_TYPE_DEFAULT = typer.Option(None, help='')
RUN_CALLBACK_URL_DEFAULT = typer.Option(None, help='')
RUN_BLOCK_RUN_ID_DEFAULT = typer.Option(None, help='')
RUN_PIPELINE_RUN_ID_DEFAULT = typer.Option(None, help='')
RUN_RUNTIME_VARS_DEFAULT = typer.Option(
    None, help='specify runtime variables. These will overwrite the pipeline global variables.'
)
RUN_SKIP_SENSORS_DEFAULT = typer.Option(False, help='specify if the sensors should be skipped.')
RUN_VAR_DEFAULT = typer.Option(
    None, '--var', help='a runtime variable, key=value; repeat for more. Values that read as '
    'JSON keep their type (2, true, [1, 2]); others are text. Overrides --runtime-vars.',
)
RUN_CLEAN_AFTER_SUCCESS_DEFAULT = typer.Option(
    False, help='remove the outputs the run stored once it succeeds (a failed run keeps them '
    'for debugging).',
)
RUN_MAX_PARALLEL_DEFAULT = typer.Option(
    None, help='most stages that run at once (default: the pipeline\'s block_run_limit, '
    'else no limit).',
)
RUN_TEMPLATE_RUNTIME_CONFIGURATION_DEFAULT = typer.Option(
    None, help='runtime configuration of data integration block runs.'
)
CLEAN_LOGS_PROJECT_PATH_DEFAULT = typer.Argument(
    ..., help='path of the MageML project to clean old logs.'
)
CLEAN_LOGS_PIPELINE_UUID_DEFAULT = typer.Option(None, help='uuid of the pipeline to clean.')
CLEAN_VARIABLES_PROJECT_PATH_DEFAULT = typer.Argument(
    ..., help='path of the MageML project to clean variables.'
)
CLEAN_VARIABLES_PIPELINE_UUID_DEFAULT = typer.Option(None, help='uuid of the pipeline to clean.')
CLEAN_VARIABLES_OLDER_THAN_DEFAULT = typer.Option(
    None, help='remove stored outputs older than this, such as 12h or 7d (0d removes all); '
    'the project\'s variables_retention_period by default.',
)
CLEAN_VARIABLES_DRY_RUN_DEFAULT = typer.Option(
    False, help='list what would be removed, with sizes, and remove nothing.',
)

CREATE_SPARK_CLUSTER_PROJECT_PATH_DEFAULT = typer.Argument(
    ..., help='path of the MageML project that contains the EMR config.'
)


@app.command()
def init(
    project_path: str = INIT_PROJECT_PATH_DEFAULT,
    project_type: Union[str, None] = INIT_PROJECT_TYPE_DEFAULT,
    cluster_type: str = INIT_CLUSTER_TYPE_DEFAULT,
    project_uuid: str = INIT_PROJECT_UUID_DEFAULT,
):
    """
    Initialize MageML project.
    """
    from mage_ai.data_preparation.repo_manager import init_repo

    repo_path = os.path.join(os.getcwd(), project_path)
    init_repo(
        repo_path,
        project_type=project_type,
        cluster_type=cluster_type,
        project_uuid=project_uuid,
    )
    print(f'Initialized MageML project at {_e(repo_path)}')


@app.command()
def start(
    project_path: str = START_PROJECT_PATH_DEFAULT,
    host: str = START_HOST_DEFAULT,
    port: str = START_PORT_DEFAULT,
    manage_instance: str = START_MANAGE_INSTANCE_DEFAULT,
    dbt_docs_instance: str = START_DBT_DOCS_INSTANCE_DEFAULT,
    instance_type: str = START_INSTANCE_TYPE_DEFAULT,
    project_type: str = START_PROJECT_TYPE_DEFAULT,
    cluster_type: str = START_CLUSTER_TYPE_DEFAULT,
    project_uuid: str = START_PROJECT_UUID_DEFAULT,
):
    """
    Start the MageML server and UI.
    """
    from mage_ai.settings.repo import set_repo_path

    # Set repo_path before intializing the DB so that we can get correct db_connection_url
    project_path = os.path.abspath(project_path)
    set_repo_path(project_path)

    from mage_ai.server.server import start_server
    from mage_ai.server.setup import initialize_globals

    initialize_globals()

    start_server(
        host=host,
        port=port,
        project=project_path,
        manage=manage_instance == '1',
        dbt_docs=dbt_docs_instance == '1',
        instance_type=os.getenv(ENV_VAR_INSTANCE_TYPE, instance_type),
        project_type=project_type,
        cluster_type=cluster_type,
        project_uuid=project_uuid,
    )


@app.command()
def run(
    project_path: str = RUN_PROJECT_PATH_DEFAULT,
    pipeline_uuid: str = RUN_PIPELINE_UUID_DEFAULT,
    test: bool = RUN_TEST_DEFAULT,
    block_uuid: Union[str, None] = RUN_BLOCK_UUID_DEFAULT,
    execution_partition: Union[str, None] = RUN_EXECUTION_PARTITION_DEFAULT,
    executor_type: Union[str, None] = RUN_EXECUTOR_TYPE_DEFAULT,
    callback_url: Union[str, None] = RUN_CALLBACK_URL_DEFAULT,
    block_run_id: Union[int, None] = RUN_BLOCK_RUN_ID_DEFAULT,
    pipeline_run_id: Union[int, None] = RUN_PIPELINE_RUN_ID_DEFAULT,
    runtime_vars: Union[str, None] = RUN_RUNTIME_VARS_DEFAULT,
    skip_sensors: bool = RUN_SKIP_SENSORS_DEFAULT,
    template_runtime_configuration: Union[str, None] = RUN_TEMPLATE_RUNTIME_CONFIGURATION_DEFAULT,
    max_parallel: Union[int, None] = RUN_MAX_PARALLEL_DEFAULT,
    clean_after_success: bool = RUN_CLEAN_AFTER_SUCCESS_DEFAULT,
    var: Union[List[str], None] = RUN_VAR_DEFAULT,
):
    """
    Run pipeline.
    """
    from mage_ai.settings.repo import set_repo_path

    # Set repo_path before intializing the DB so that we can get correct db_connection_url
    project_path = os.path.abspath(project_path)
    if not os.path.isdir(project_path):
        print(f'[red]Project {_e(project_path)} does not exist.[/red]')
        raise typer.Exit(code=2)
    set_repo_path(project_path)

    from contextlib import nullcontext

    import newrelic.agent
    import sentry_sdk

    from mage_ai.data_preparation.contracts import ContractViolation
    from mage_ai.data_preparation.executors.executor_factory import ExecutorFactory
    from mage_ai.data_preparation.models.block.r.runtime import REnvironmentError
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.data_preparation.sync.git_sync import get_sync_config
    from mage_ai.data_preparation.variable_manager import get_global_variables
    from mage_ai.orchestration.db import db_connection
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.orchestration.utils.git import log_git_sync, run_git_sync
    from mage_ai.server.logger import Logger
    from mage_ai.settings import (
        SENTRY_DSN,
        SENTRY_SERVER_NAME,
        SENTRY_TRACES_SAMPLE_RATE,
    )
    from mage_ai.shared.hash import merge_dict

    logger = Logger().new_server_logger(__name__)

    sentry_dsn = SENTRY_DSN
    if sentry_dsn:
        sentry_sdk.init(
            sentry_dsn,
            traces_sample_rate=SENTRY_TRACES_SAMPLE_RATE,
            server_name=SENTRY_SERVER_NAME,
            # Frames of block code hold DataFrames; their values would be sent.
            include_local_variables=False,
        )
        import atexit
        atexit.register(lambda: sentry_sdk.flush(timeout=5))
    (enable_new_relic, application) = initialize_new_relic()

    with (
        newrelic.agent.BackgroundTask(application, name='mage-run', group='Task')
        if enable_new_relic
        else nullcontext()
    ):
        sync_config = get_sync_config()
        if sync_config and sync_config.sync_on_executor_start:
            result = run_git_sync(sync_config=sync_config, setup_repo=True)
            log_git_sync(result, logger)

        runtime_variables = dict()
        try:
            if runtime_vars is not None:
                runtime_variables = parse_runtime_variables(runtime_vars)
            runtime_variables.update(parse_var_options(var))
        except ValueError as error:
            print(f'[red]{_e(error)}[/red]')
            raise typer.Exit(code=2)

        add_project_to_sys_path(project_path)
        # Initialize db_connection session before getting the pipeline in case
        # "mage_secret_var" syntax is used in the project's metadata.yaml
        db_connection.start_session()
        if pipeline_uuid not in Pipeline.get_all_pipelines(project_path):
            names = sorted(Pipeline.get_all_pipelines(project_path))
            print(
                f'[red]Pipeline {_e(pipeline_uuid)} is not in project {_e(project_path)}. '
                + (f'Its pipelines: {_e(", ".join(names))}.' if names else 'It has no pipelines.')
                + '[/red]'
            )
            raise typer.Exit(code=2)
        pipeline = Pipeline.get(pipeline_uuid, repo_path=project_path)

        if pipeline_run_id is None:
            default_variables = get_global_variables(pipeline_uuid)
            global_vars = merge_dict(default_variables, runtime_variables)
        else:
            pipeline_run = PipelineRun.get_by_id(pipeline_run_id)
            global_vars = pipeline_run.get_variables(extra_variables=runtime_variables)

        if template_runtime_configuration is not None:
            template_runtime_configuration = json.loads(template_runtime_configuration)

        try:
            if block_uuid is None:
                ExecutorFactory.get_pipeline_executor(
                    pipeline,
                    execution_partition=execution_partition,
                    executor_type=executor_type,
                ).execute(
                    analyze_outputs=False,
                    global_vars=global_vars,
                    max_parallel=max_parallel,
                    pipeline_run_id=pipeline_run_id,
                    run_sensors=not skip_sensors,
                    run_tests=test,
                    update_status=False,
                )
            else:
                ExecutorFactory.get_block_executor(
                    pipeline,
                    block_uuid,
                    block_run_id=block_run_id,
                    execution_partition=execution_partition,
                    executor_type=executor_type,
                ).execute(
                    analyze_outputs=False,
                    block_run_id=block_run_id,
                    callback_url=callback_url,
                    global_vars=global_vars,
                    pipeline_run_id=pipeline_run_id,
                    template_runtime_configuration=template_runtime_configuration,
                    update_status=False,
                )
        except (REnvironmentError, ContractViolation) as error:
            # A setup or data problem with its explanation in the message (a contract's
            # broken rules are printed above it), not a failure of the block's code.
            print(f'[red]{_e(error)}[/red]')
            raise typer.Exit(code=1)
        except Exception as error:
            # The whole traceback, in plain text: the block's file and line are in it, and
            # the notes at its end name the block and what to do.
            import traceback

            traceback.print_exception(error, file=sys.stderr)
            print('[red]Pipeline run failed.[/red]')
            raise typer.Exit(code=1)
        print('Pipeline run completed.')
        if clean_after_success and block_uuid is None:
            from mage_ai.data_preparation.variable_manager import VariableManager

            removed = VariableManager(variables_dir=pipeline.variables_dir).clean_run_outputs(
                pipeline.uuid, list(pipeline.blocks_by_uuid), execution_partition,
            )
            print(f'Removed the outputs the run stored ({len(removed)} folders).')


@app.command()
def clean_cached_variables(
    project_path: str = CLEAN_VARIABLES_PROJECT_PATH_DEFAULT,
    pipeline_uuid: str = CLEAN_VARIABLES_PIPELINE_UUID_DEFAULT,
    older_than: Union[str, None] = CLEAN_VARIABLES_OLDER_THAN_DEFAULT,
    dry_run: bool = CLEAN_VARIABLES_DRY_RUN_DEFAULT,
):
    """
    Remove stored block outputs: scheduled runs' partitions, and outputs of editor and
    mage run runs, older than the retention period or --older-than.
    """
    from mage_ai.settings.repo import set_repo_path

    project_path = os.path.abspath(project_path)
    set_repo_path(project_path)

    from mage_ai.data_preparation.variable_manager import clean_variables

    try:
        report = clean_variables(pipeline_uuid=pipeline_uuid, older_than=older_than,
                                 dry_run=dry_run)
    except ValueError as error:
        print(f'[red]{_e(error)}[/red]')
        raise typer.Exit(code=2)
    print(f'{"Would remove" if dry_run else "Removed"} {len(report["removed"])} folders, '
          f'{report["bytes_removed"] / 1e6:.1f} MB.')
    if report['errors']:
        raise typer.Exit(code=1)


@app.command()
def clean_old_logs(
    project_path: str = CLEAN_LOGS_PROJECT_PATH_DEFAULT,
    pipeline_uuid: str = CLEAN_LOGS_PIPELINE_UUID_DEFAULT,
):
    from mage_ai.settings.repo import set_repo_path

    project_path = os.path.abspath(project_path)
    set_repo_path(project_path)

    from mage_ai.data_preparation.logging.logger_manager_factory import (
        LoggerManagerFactory,
    )

    LoggerManagerFactory.get_logger_manager(
        pipeline_uuid=pipeline_uuid,
    ).delete_old_logs()


@app.command()
def create_spark_cluster(
    project_path: str = CREATE_SPARK_CLUSTER_PROJECT_PATH_DEFAULT,
):
    """
    Create EMR cluster for MageML project.
    """
    from mage_ai.services.aws.emr.launcher import create_cluster

    project_path = os.path.abspath(project_path)
    create_cluster(project_path)


def _contract_block(project_path: str, pipeline_uuid: str, block_uuid: str):
    from mage_ai.settings.repo import set_repo_path

    set_repo_path(project_path)
    add_project_to_sys_path(project_path)

    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.get(pipeline_uuid, repo_path=project_path, check_if_exists=True)
    block = pipeline.get_block(block_uuid) if pipeline else None
    if block is None:
        print(f'[red]Block {_e(block_uuid)} does not exist in pipeline {_e(pipeline_uuid)}.[/red]')
        raise typer.Exit(code=2)
    return block


@app.command('contract-draft')
def contract_draft(
    project_path: str = typer.Argument(..., help='path of the MageML project.'),
    pipeline_uuid: str = typer.Argument(..., help='uuid of the pipeline.'),
    block_uuid: str = typer.Argument(..., help='uuid of the block whose output to describe.'),
    name: Union[str, None] = typer.Option(None, help='contract name; the block uuid by default.'),
    write: bool = typer.Option(False, help='save it as contracts/<name>.yaml.'),
    force: bool = typer.Option(False, help='replace an existing contract file.'),
):
    """
    Draft a data contract from a block's stored output (run the block first): its
    columns, types and which columns have no missing values. Add ranges, allowed values
    and unique keys, then set the contract in the block's settings.
    """
    from mage_ai.data_preparation import contracts

    project_path = os.path.abspath(project_path)
    block = _contract_block(project_path, pipeline_uuid, block_uuid)
    name = name or block.uuid.replace('/', '_')
    try:
        document = contracts.draft(block, name)
    except contracts.ContractError as error:
        print(f'[red]{_e(error)}[/red]')
        raise typer.Exit(code=2)
    if write:
        path = contracts.contracts_dir(project_path) / f'{name}.yaml'
        if path.exists() and not force:
            print(f'[red]{_e(path)} exists; pass --force to replace it.[/red]')
            raise typer.Exit(code=2)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(document)
        typer.echo(f'Wrote {path}')
    else:
        typer.echo(document, nl=False)


@app.command('contract-check')
def contract_check(
    project_path: str = typer.Argument(..., help='path of the MageML project.'),
    pipeline_uuid: str = typer.Argument(..., help='uuid of the pipeline.'),
    block_uuid: str = typer.Argument(..., help='uuid of the block with a contract.'),
    partition: Union[str, None] = typer.Option(
        None, help="a pipeline run's execution partition; the notebook output by default.",
    ),
):
    """
    Check a block's stored output against its data contract. Exits with 1 when the
    output breaks it, 2 when the check cannot run.
    """
    from mage_ai.data_preparation import contracts

    project_path = os.path.abspath(project_path)
    block = _contract_block(project_path, pipeline_uuid, block_uuid)
    try:
        checked = contracts.check_output(block, execution_partition=partition)
    except contracts.ContractError as error:
        print(f'[red]{_e(error)}[/red]')
        raise typer.Exit(code=2)
    if checked is None:
        typer.echo(f'Block {block_uuid} has no contract, or its enforcement is off.')
        return
    binding, report = checked
    typer.echo(contracts.summary(binding, report))
    if not report['passed']:
        raise typer.Exit(code=1)


def _run_records_setup(project_path: str, pipeline_uuid: str):
    from mage_ai.settings.repo import set_repo_path

    set_repo_path(project_path)
    add_project_to_sys_path(project_path)

    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.orchestration.db import db_connection

    db_connection.start_session()
    pipeline = Pipeline.get(pipeline_uuid, repo_path=project_path, check_if_exists=True)
    if pipeline is None:
        print(f'[red]Pipeline {_e(pipeline_uuid)} does not exist in {_e(project_path)}.[/red]')
        raise typer.Exit(code=2)
    return pipeline


def _pipeline_run(pipeline, run_id: int):
    from mage_ai.orchestration.db.models.schedules import PipelineRun

    run = PipelineRun.get(run_id)
    if run is None or run.pipeline_uuid != pipeline.uuid:
        print(f'[red]Pipeline run {_e(run_id)} of {_e(pipeline.uuid)} does not exist.[/red]')
        raise typer.Exit(code=2)
    return run


@app.command('run-diff')
def run_diff(
    project_path: str = typer.Argument(..., help='path of the MageML project.'),
    pipeline_uuid: str = typer.Argument(..., help='uuid of the pipeline.'),
    first_run_id: int = typer.Argument(..., help='id of the earlier pipeline run.'),
    second_run_id: int = typer.Argument(..., help='id of the later pipeline run.'),
    as_json: bool = typer.Option(False, '--json', help='print the comparison as JSON.'),
):
    """
    Compare what two runs of a pipeline ran with: code (with a diff of each changed file),
    Python, MageML and package versions, variables, and which blocks' outputs differ.
    """
    from mage_ai.orchestration import run_records

    project_path = os.path.abspath(project_path)
    pipeline = _run_records_setup(project_path, pipeline_uuid)
    runs = [_pipeline_run(pipeline, run_id) for run_id in (first_run_id, second_run_id)]
    try:
        comparison = run_records.compare(pipeline, *runs)
    except run_records.RunRecordError as error:
        print(f'[red]{_e(error)}[/red]')
        raise typer.Exit(code=2)
    if as_json:
        typer.echo(json.dumps(comparison, indent=2, default=str))
    else:
        typer.echo(run_records.format_comparison(comparison))


@app.command('reproduce')
def reproduce(
    project_path: str = typer.Argument(..., help='path of the MageML project.'),
    pipeline_uuid: str = typer.Argument(..., help='uuid of the pipeline.'),
    run_id: int = typer.Argument(..., help='id of the pipeline run to reproduce.'),
    yes: bool = typer.Option(False, '--yes', '-y', help='run without asking first.'),
    report: Union[str, None] = typer.Option(None, help='also write the result as JSON here.'),
    keep: bool = typer.Option(False, help='keep the restored copy of the project.'),
):
    """
    Run a pipeline run again with the code it ran with (restored from its run record),
    its variables and its execution date, then compare every block's outputs with the
    original run's. The pipeline runs in full, so exporters write again.
    """
    from mage_ai.orchestration import run_records

    project_path = os.path.abspath(project_path)
    pipeline = _run_records_setup(project_path, pipeline_uuid)
    original = _pipeline_run(pipeline, run_id)
    if not yes and not typer.confirm(
        f'This runs {pipeline_uuid} again, exporters included. Continue?',
    ):
        raise typer.Exit(code=1)
    try:
        result = run_records.reproduce(pipeline, original, log=typer.echo, keep=keep)
    except run_records.RunRecordError as error:
        print(f'[red]{_e(error)}[/red]')
        raise typer.Exit(code=2)
    if report:
        with open(report, 'w') as file:
            json.dump(result, file, indent=2, default=str)
    typer.echo(run_records.format_reproduction(result))
    if not result['passed']:
        raise typer.Exit(code=1)


@app.command('reproduce-run', hidden=True)
def reproduce_run(
    project_path: str = typer.Argument(..., help='path of the restored project.'),
    pipeline_uuid: str = typer.Argument(..., help='uuid of the pipeline.'),
    run_id: int = typer.Argument(..., help='id of the original pipeline run.'),
    report: str = typer.Option(..., help='where to write the new run id.'),
):
    """The second half of `mage reproduce`, run in the restored copy of the project."""
    from mage_ai.orchestration import run_records
    from mage_ai.orchestration.db.models.schedules import PipelineRun

    project_path = os.path.abspath(project_path)
    # The restored code, not a project of the same name in the working folder.
    sys.path.insert(0, os.path.dirname(project_path))
    add_project_to_sys_path(project_path)
    try:
        pipeline = _run_records_setup(project_path, pipeline_uuid)
        original = PipelineRun.get(run_id)
        run = run_records.run_reproduction(pipeline, original, log=typer.echo)
        result = dict(pipeline_run_id=run.id, status=str(run.status))
    except Exception as error:
        result = dict(error=f'{type(error).__name__}: {error}')
    with open(report, 'w') as file:
        json.dump(result, file)


def _releases_setup(project_path: str):
    from mage_ai.settings.repo import set_repo_path

    project_path = os.path.abspath(project_path)
    set_repo_path(project_path)
    return project_path


@app.command('release-status')
def release_status(
    project_path: str = typer.Argument(..., help='path of the MageML project.'),
    model: str = typer.Argument(..., help='name of the registered model.'),
    version: Union[str, None] = typer.Option(None, help='evaluate this version now.'),
):
    """
    Show a model's release policy, the version holding its alias and its release log;
    with --version, evaluate that version against the policy and the current champion.
    """
    from mage_ai.orchestration import releases

    project_path = _releases_setup(project_path)
    try:
        policy = releases.load_policy(model, project_path)
        if policy is None:
            print(f'[red]{_e(model)} has no release policy in releases/.[/red]')
            raise typer.Exit(code=2)
        champion = releases.champion_of(releases._client(), policy)
        typer.echo(
            f'{model}: {policy.alias} is version {champion.version if champion else "none"}; '
            f'approval {policy.approval}.'
        )
        for entry in releases.release_log(project_path, model)[-10:]:
            typer.echo(
                f'- {entry["at"]} {entry["action"]} to version {entry["version"]} '
                f'(was {entry["previous"] or "none"}) by {entry["actor"]}'
            )
        if version:
            typer.echo(releases.summary(releases.evaluate_version(policy, version)))
    except releases.ReleaseError as error:
        print(f'[red]{_e(error)}[/red]')
        raise typer.Exit(code=2)


@app.command('release-promote')
def release_promote(
    project_path: str = typer.Argument(..., help='path of the MageML project.'),
    model: str = typer.Argument(..., help='name of the registered model.'),
    version: str = typer.Argument(..., help='the version to promote.'),
    force: bool = typer.Option(False, help='promote even if the release check does not pass.'),
):
    """
    Evaluate a version against the model's release policy and, if it passes, move the
    policy's alias to it.
    """
    import getpass

    from mage_ai.orchestration import releases

    project_path = _releases_setup(project_path)
    try:
        policy = releases.load_policy(model, project_path)
        if policy is None:
            print(f'[red]{_e(model)} has no release policy in releases/.[/red]')
            raise typer.Exit(code=2)
        evaluation = releases.evaluate_version(policy, version)
        typer.echo(releases.summary(evaluation))
        if evaluation['decision'] != 'pass' and not force:
            print('[red]Not promoted; pass --force to promote anyway.[/red]')
            raise typer.Exit(code=1)
        entry = releases.promote(
            project_path, model, version, evaluation['champion'], getpass.getuser(),
        )
    except releases.ReleaseError as error:
        print(f'[red]{_e(error)}[/red]')
        raise typer.Exit(code=2)
    typer.echo(f'{model} {entry["alias"]} is now version {entry["version"]}.')


@app.command('release-rollback')
def release_rollback(
    project_path: str = typer.Argument(..., help='path of the MageML project.'),
    model: str = typer.Argument(..., help='name of the registered model.'),
):
    """Give the model's alias back to the version that held it before the last promotion."""
    import getpass

    from mage_ai.orchestration import releases

    project_path = _releases_setup(project_path)
    try:
        entry = releases.rollback(project_path, model, getpass.getuser())
    except releases.ReleaseError as error:
        print(f'[red]{_e(error)}[/red]')
        raise typer.Exit(code=2)
    typer.echo(f'{model} {entry["alias"]} is back to version {entry["version"]}.')


@app.command('monitor-status')
def monitor_status(
    project_path: str = typer.Argument(..., help='path of the MageML project.'),
    monitor: str = typer.Argument(..., help='name of the monitor (monitors/<name>.yaml).'),
    runs: int = typer.Option(10, help='how many of the latest runs to show.'),
):
    """Show a monitor's checks for the latest pipeline runs it watched."""
    from mage_ai.orchestration import monitors

    project_path = os.path.abspath(project_path)
    try:
        definition = monitors.load_monitor(monitor, project_path)
        pipeline = _run_records_setup(project_path, definition.pipeline)
    except monitors.MonitorError as error:
        print(f'[red]{_e(error)}[/red]')
        raise typer.Exit(code=2)
    history = monitors.saved(pipeline, definition)[-runs:]
    if not history:
        typer.echo(f'Monitor {monitor} has not checked a run yet.')
        return
    for record in history:
        typer.echo(f'Pipeline run {record["pipeline_run_id"]}: ' + monitors.summary(record))


@app.command('verify-fusion')
def verify_fusion(
    project_path: str = typer.Argument(..., help='path of the MageML project.'),
    pipeline_uuid: str = typer.Argument(..., help='uuid of the pipeline.'),
    runtime_vars: Union[str, None] = RUN_RUNTIME_VARS_DEFAULT,
    yes: bool = typer.Option(False, '--yes', '-y', help='run without asking first.'),
    report: Union[str, None] = typer.Option(None, help='also write the result as JSON here.'),
    exact: bool = typer.Option(
        False, help='count floats that differ only by rounding as a difference.',
    ),
):
    """
    Run a pipeline block by block, then with block fusion, and compare every block's
    stored outputs. Names the first block whose output differs. Both runs execute the
    whole pipeline, so exporters write twice.
    """
    from mage_ai.settings.repo import set_repo_path

    project_path = os.path.abspath(project_path)
    set_repo_path(project_path)
    add_project_to_sys_path(project_path)

    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.data_preparation.variable_manager import get_global_variables
    from mage_ai.orchestration.db import db_connection
    from mage_ai.orchestration.fusion_verify import (
        FusionVerificationError,
        format_report,
    )
    from mage_ai.orchestration.fusion_verify import verify_fusion as verify
    from mage_ai.shared.hash import merge_dict

    db_connection.start_session()
    pipeline = Pipeline.get(pipeline_uuid, repo_path=project_path)
    if not yes and not typer.confirm(
        f'This runs {pipeline_uuid} twice, exporters included. Continue?',
    ):
        raise typer.Exit(code=1)
    variables = merge_dict(
        get_global_variables(pipeline_uuid),
        parse_runtime_variables(runtime_vars) if runtime_vars else {},
    )
    try:
        verification = verify(pipeline, variables=variables, exact=exact)
    except FusionVerificationError as error:
        print(f'[red]{_e(error)}[/red]')
        raise typer.Exit(code=2)
    if report:
        with open(report, 'w') as file:
            json.dump(verification.to_dict(), file, indent=2)
    # Plain text: rich would read brackets in values as markup.
    typer.echo(format_report(verification))
    if not verification.passed:
        raise typer.Exit(code=1)


r_app = typer.Typer(
    cls=OrderCommands,
    help='Manage the rv environment that R blocks run in.',
    pretty_exceptions_show_locals=False,
)
app.add_typer(r_app, name='r')

R_PROJECT_PATH_DEFAULT = typer.Argument('.', help='path of the MageML project.')
R_PACKAGES_DEFAULT = typer.Option(
    None,
    '--package',
    '-p',
    help='an R package to install; repeat for more. Defaults to the tidyverse.',
)
R_VERSION_DEFAULT = typer.Option('4.6', help='the R version of the environment.')
R_INSTALL_DEFAULT = typer.Option(
    False,
    '--install-r',
    help='install the R version with rig (https://github.com/r-lib/rig) when it is missing.',
)


def _r_config(project_path: str):
    from mage_ai.data_preparation.models.block.r import runtime

    try:
        config = runtime.r_config(os.path.abspath(project_path))
    except runtime.REnvironmentError as error:
        print(f'[red]{_e(error)}[/red]')
        raise typer.Exit(code=1)
    if not config.uses_rv:
        print(
            f'[red]{_e(os.path.abspath(project_path))} has no R environment. Create it with '
            f'`mage r init {_e(project_path)}`.[/red]'
        )
        raise typer.Exit(code=1)
    return config


@r_app.command('setup')
def r_setup(
    project_path: str = R_PROJECT_PATH_DEFAULT,
    r_version: str = R_VERSION_DEFAULT,
):
    """
    Check the tools R blocks need, R, rv and rig, and print how to install the missing ones.
    """
    from mage_ai.data_preparation.models.block.r import runtime

    steps = runtime.setup_steps(r_version)
    missing = False
    for step in steps:
        if step['found']:
            print(f'[green]✓[/green] {_e(step["name"])}: {_e(step["found"])}')
            continue
        optional = step.get('optional', False)
        missing = missing or not optional
        print(f'[yellow]✗[/yellow] {_e(step["name"])}: install it with')
        for command in step['commands']:
            print(f'    {_e(command)}')
    if missing:
        print('Then run `mage r setup` again.')
        raise typer.Exit(code=1)
    print(
        f'R blocks can be set up. Create the R environment with `mage r init {_e(project_path)}`; '
        'edit <project>/r/rproject.toml or run `rv add` there to change its packages.'
    )


@r_app.command('init')
def r_init(
    project_path: str = R_PROJECT_PATH_DEFAULT,
    packages: List[str] = R_PACKAGES_DEFAULT,
    r_version: str = R_VERSION_DEFAULT,
    install_r: bool = R_INSTALL_DEFAULT,
):
    """
    Create the project's R environment in <project>/r with rv and install its packages.
    """
    from pathlib import Path

    from mage_ai.data_preparation.models.block.r import runtime

    directory = Path(os.path.abspath(project_path)) / runtime.R_PROJECT_DIRECTORY
    try:
        if install_r and not runtime.has_r_version(r_version):
            print(f'Installing R {_e(r_version)} with rig.')
            runtime.install_r(r_version)
        runtime.init_project(directory, packages=packages or None, r_version=r_version)
    except runtime.REnvironmentError as error:
        print(f'[red]{_e(error)}[/red]')
        raise typer.Exit(code=1)
    print(f'Created the R environment in {_e(directory)}. Add packages with `rv add` there.')


@r_app.command('sync')
def r_sync(project_path: str = R_PROJECT_PATH_DEFAULT):
    """
    Install the packages of the project's R environment, as rv.lock pins them.
    """
    from mage_ai.data_preparation.models.block.r import runtime

    config = _r_config(project_path)
    try:
        runtime.sync(config)
    except runtime.REnvironmentError as error:
        print(f'[red]{_e(error)}[/red]')
        raise typer.Exit(code=1)
    print(f'The R library of {_e(config.project_dir)} is synced.')


@r_app.command('status')
def r_status(project_path: str = R_PROJECT_PATH_DEFAULT):
    """
    Check that R blocks can run: the R version, the library and the packages MageML needs.
    """
    from mage_ai.data_preparation.models.block.r import runtime

    config = _r_config(project_path)
    report = runtime.status(config)
    for key, value in report['details'].items():
        print(f'{_e(key)}: {_e(value)}')
    for problem in report['problems']:
        print(f'[red]{_e(problem)}[/red]')
    if report['problems']:
        raise typer.Exit(code=1)
    print('R blocks can run.')


rust_app = typer.Typer(
    cls=OrderCommands,
    help='Manage the Cargo workspace that Rust blocks build in.',
    pretty_exceptions_show_locals=False,
)
app.add_typer(rust_app, name='rust')

RUST_PROJECT_PATH_DEFAULT = typer.Argument('.', help='path of the MageML project.')


@rust_app.command('init')
def rust_init(project_path: str = RUST_PROJECT_PATH_DEFAULT):
    """
    Create the project's Rust environment in <project>/rust: the crates Rust blocks use.
    """
    from mage_ai.data_preparation.models.block.rust import workspace

    created = workspace.init(os.path.abspath(project_path))
    print(
        f'The Rust environment is in {_e(created.root)}. Add crates under '
        f'\\[workspace.dependencies] in {_e(created.cargo_toml)}.'
    )


@rust_app.command('build')
def rust_build(project_path: str = RUST_PROJECT_PATH_DEFAULT):
    """
    Build every Rust block of the project, so runs start without compiling. Use it when
    deploying, or to compile the dependencies once.
    """
    from mage_ai.data_preparation.models.block.rust import project

    reports = project.build_all(os.path.abspath(project_path))
    if not reports:
        print('The project has no Rust blocks.')
        return
    for report in reports:
        if report.ok:
            how = 'cached' if report.cached else f'built in {report.seconds:.1f} s'
            print(f'[green]✓[/green] {_e(report.block.label)}: {_e(how)}')
        else:
            print(f'[red]✗[/red] {_e(report.block.label)}\n{_e(report.error)}')
    failed = sum(not report.ok for report in reports)
    if failed:
        print(f'[red]{_e(failed)} of {len(reports)} Rust blocks do not build.[/red]')
        raise typer.Exit(code=1)
    print(f'All {len(reports)} Rust blocks are built.')


@rust_app.command('status')
def rust_status(project_path: str = RUST_PROJECT_PATH_DEFAULT):
    """
    Check that Rust blocks can build: cargo, rustc, the workspace and its crates.
    """
    from mage_ai.data_preparation.models.block.rust import project

    report = project.status(os.path.abspath(project_path))
    for key, value in report['details'].items():
        print(f'{_e(key)}: {_e(value)}')
    for problem in report['problems']:
        print(f'[red]{_e(problem)}[/red]')
    if report['problems']:
        raise typer.Exit(code=1)
    print('Rust blocks can build.')


examples_app = typer.Typer(
    cls=OrderCommands,
    help='Write the example datasets of the tutorials into a project.',
    pretty_exceptions_show_locals=False,
)
app.add_typer(examples_app, name='examples')


@examples_app.command('retail')
def examples_retail(
    project_path: str = typer.Argument('.', help='path of the MageML project.'),
):
    """
    Write a shop's customers and a year of orders into <project>/data/shop as Parquet, for
    the retail tutorials in Python, R, Rust and SQL.
    """
    from mage_ai.examples import retail

    try:
        written = retail.write(os.path.abspath(project_path))
    except FileNotFoundError as error:
        print(f'[red]{_e(error)}[/red]')
        raise typer.Exit(code=1)
    print(
        f'Wrote {written.customers:,} customers and {written.orders:,} order rows '
        f'({written.duplicates:,} of them duplicated) to {_e(written.folder)}.'
    )


export_app = typer.Typer(
    cls=OrderCommands,
    help='Export pipelines to run without MageML.',
    pretty_exceptions_show_locals=False,
)
app.add_typer(export_app, name='export')


EXPORT_PROJECT_PATH = typer.Argument(..., help='path of the MageML project.')
EXPORT_PIPELINES = typer.Argument(
    None, help='uuids of the pipelines to export; optional with --service.',
)
EXPORT_INCLUDES = typer.Option(
    [], '--include', '-i',
    help='a project file or folder to put in the image, such as a model the blocks load '
         'by a computed path; repeat for more. Paths named in the blocks are found '
         'without it.',
)
EXPORT_PLATFORM = typer.Option(
    None, '--platform',
    help='platforms to build for with --build, such as linux/amd64,linux/arm64; uses docker '
         'buildx. Several platforms need --push.',
)
EXPORT_PUSH = typer.Option(
    False, '--push',
    help='with --build, push the image to the registry its --tag names, such as '
         'icr.io/team/scoring:1.0 or docker.io/team/scoring:1.0; log in first.',
)
EXPORT_SERVICE = typer.Option(
    None, '--service', '-s',
    help='a service saved in the project (services/<name>/service.yaml, the Services page): '
         'its pipelines, settings and included files.',
)
EXPORT_MODELS = typer.Option(
    [], '--model', '-m',
    help='an MLflow model to embed, NAME=URI (models:/name/3, models:/name@alias, '
         'runs:/<run id>/model); repeat for more. Calls to load_model("name", uri="...") '
         'in the blocks are found without it.',
)


@export_app.command('service')
def export_service(
    project_path: str = EXPORT_PROJECT_PATH,
    pipelines: List[str] = EXPORT_PIPELINES,
    out: str = typer.Option(..., '--out', '-o', help='directory for the Docker build context.'),
    name: Union[str, None] = typer.Option(None, help='service name; defaults to the pipeline.'),
    max_runs: Union[int, None] = typer.Option(
        None, help='runs of each pipeline at once; defaults to the pipeline setting or 1.',
    ),
    build: bool = typer.Option(False, help='also build the Docker image.'),
    tag: Union[str, None] = typer.Option(None, help='image tag for --build; defaults to the name.'),
    force: bool = typer.Option(False, help='replace a non-empty output directory.'),
    model: List[str] = EXPORT_MODELS,
    include: List[str] = EXPORT_INCLUDES,
    service: Union[str, None] = EXPORT_SERVICE,
    platform: Union[str, None] = EXPORT_PLATFORM,
    push: bool = EXPORT_PUSH,
):
    """
    Export pipelines as a standalone Docker service: an HTTP API, schedules, run history
    and logs, without MageML. Writes a build context with a Dockerfile, compose.yaml and a
    README that shows how to run and call it.
    """
    from mage_ai.pipeline_services import definitions, export
    from mage_ai.pipeline_services.exports import EXPORT_LIMITS, model_registry

    registry = None
    if service:
        try:
            saved = definitions.load_definition(project_path, service)
        except definitions.DefinitionError as error:
            typer.echo(str(error), err=True)
            raise typer.Exit(code=1)
        pipelines = list(dict.fromkeys([*saved['pipelines'], *(pipelines or [])]))
        name = name or service
        max_runs = max_runs or saved['max_runs']
        include = [*saved['include'], *include]
        model = [*[f"{m['name']}={m['uri']}" for m in saved['models']], *model]
        # MLFLOW_TRACKING_URI in the environment wins, so CI can reach the registry its way.
        if not os.environ.get('MLFLOW_TRACKING_URI'):
            registry = saved['model_registry']
    if not pipelines:
        typer.echo('Name the pipelines to export, or a saved service with --service.', err=True)
        raise typer.Exit(code=1)
    try:
        with model_registry(registry, EXPORT_LIMITS):
            captured = export.capture(
                project_path, pipelines, name=name, max_concurrent_runs=max_runs,
                models=model, includes=include,
            )
            out_dir = export.write(captured, out, force=force)
    except export.ExportError as error:
        typer.echo('The pipelines cannot be exported:', err=True)
        for problem in error.problems:
            typer.echo(f'  - {problem}', err=True)
        raise typer.Exit(code=1)
    typer.echo(export.report(captured))
    typer.echo(f'Build context: {out_dir}')
    if push and not build:
        typer.echo('--push needs --build.', err=True)
        raise typer.Exit(code=1)
    if build:
        image = tag or captured.name
        platforms = (platform or '').split(',') if platform else None
        try:
            export.build_image(out_dir, image, platforms=platforms, push=push)
        except export.ExportError as error:
            typer.echo(error.problems[0], err=True)
            raise typer.Exit(code=1)
        if push:
            typer.echo(f'Image {image} is pushed. Run it with:')
        else:
            typer.echo(f'Image {image} is built. Run it with:')
        typer.echo(f'  docker run --rm -p 8080:8080 -e MAGE_SERVICE_TOKEN=change-me {image}')
    else:
        typer.echo(f'Build the image with: docker build -t {captured.name} {out_dir}')


service_app = typer.Typer(
    cls=OrderCommands,
    help='Services saved in the project (services/<name>/): create, check, build, export.',
    pretty_exceptions_show_locals=False,
)
app.add_typer(service_app, name='service')

SERVICE_PROJECT = typer.Option('.', '--project', '-p', help='path of the MageML project.')


def _service_error(error: Exception) -> None:
    problems = getattr(error, 'problems', None) or [str(error)]
    for problem in problems:
        typer.echo(f'  - {problem}' if len(problems) > 1 else problem, err=True)
    raise typer.Exit(code=1)


@service_app.command('list')
def service_list(
    project: str = SERVICE_PROJECT,
    env: Union[str, None] = typer.Option(
        None, '--env', help='only the services that define this environment.'),
    as_json: bool = typer.Option(False, '--json', help='as JSON, for CI.'),
):
    """The project's services, their pipelines and where each is saved."""
    import json

    from mage_ai.pipeline_services import ci, definitions

    if env:
        try:
            plan = ci.environment_plan(project, env)
        except definitions.DefinitionError as error:
            _service_error(error)
        if as_json:
            typer.echo(json.dumps(plan))
        else:
            for item in plan:
                typer.echo(f'{item["service"]}  {env}  {item["target"]}')
        return
    try:
        saved = definitions.load_definitions(project)
    except definitions.DefinitionError as error:
        _service_error(error)
    if not saved:
        typer.echo(
            'No services yet. Create one with: mage service new <name> --pipelines <uuid>',
        )
        return
    for name, value in saved.items():
        where = definitions.location(project, name)
        typer.echo(f'{name}  {", ".join(value["pipelines"])}  ({where})')


@service_app.command('new')
def service_new(
    name: str = typer.Argument(..., help='service name: lowercase letters, digits and -.'),
    pipelines: str = typer.Option(..., '--pipelines', help='pipeline uuids, separated by commas.'),
    description: str = typer.Option('', help='what the service does.'),
    project: str = SERVICE_PROJECT,
):
    """Creates services/<name>/ with service.yaml and a dev environment."""
    from mage_ai.pipeline_services import definitions, lifecycle

    try:
        written = lifecycle.new(
            project, name, [p.strip() for p in pipelines.split(',') if p.strip()], description,
        )
    except definitions.DefinitionError as error:
        _service_error(error)
    for path in written:
        typer.echo(f'Wrote {os.path.relpath(path, project)}')
    typer.echo(f'Next: mage service check {name}')


@service_app.command('check')
def service_check(
    name: Union[str, None] = typer.Argument(None),
    project: str = SERVICE_PROJECT,
    affected: bool = typer.Option(
        False, '--affected', help='check the services that changed files touch, for CI.',
    ),
    base: str = typer.Option(
        'origin/main', '--base', help='with --affected, the Git ref the changes are against.',
    ),
    locked: bool = typer.Option(
        False, '--locked', help='also fail when service.lock is not the build\'s, for CI.',
    ),
):
    """What would stop the service from building, and what to know about it."""
    import subprocess

    from mage_ai.pipeline_services import definitions, export, lifecycle

    if affected:
        try:
            paths = lifecycle.changed_paths(project, base)
        except subprocess.CalledProcessError as error:
            typer.echo(f'Git could not list the changes since {base}: '
                       f'{(error.stderr or "").strip()}', err=True)
            raise typer.Exit(code=1)
        names = lifecycle.affected(project, paths)
        if not names:
            typer.echo(f'No service is affected by the changes since {base}.')
            return
        typer.echo(f'Affected: {", ".join(names)}')
    elif name:
        names = [name]
    else:
        typer.echo('Name a service, or pass --affected.', err=True)
        raise typer.Exit(code=1)

    failed = False
    for service in names:
        result = lifecycle.check(project, service)
        if not result.ok:
            failed = True
            typer.echo(f'{service} cannot be built:', err=True)
            for problem in result.problems:
                typer.echo(f'  - {problem}', err=True)
            continue
        typer.echo(result.report)
        if locked:
            before = lifecycle.read_lock(project, service)
            try:
                lifecycle.build(project, service)
            except (definitions.DefinitionError, export.ExportError) as error:
                failed = True
                typer.echo(f'{service} does not build: {error}', err=True)
                continue
            if lifecycle.read_lock(project, service) != before:
                failed = True
                typer.echo(f'{service}: service.lock is not the build\'s. Run mage service '
                           f'build {service} and commit services/{service}/service.lock.',
                           err=True)
                continue
            typer.echo(f'{service} is ready to build, with its lock committed.')
            continue
        typer.echo(f'{service} is ready to build. Try it here with mage service run '
                   f'{service} and mage service call {service}; deploy it with mage service '
                   f'deploy {service} --env <env>.')
    if failed:
        raise typer.Exit(code=1)


@service_app.command('build')
def service_build(
    name: str = typer.Argument(...),
    project: str = SERVICE_PROJECT,
    image: bool = typer.Option(False, '--image', help='also build the Docker image.'),
    tag: Union[str, None] = typer.Option(None, help='image tag; defaults to the service name.'),
    platform: Union[str, None] = EXPORT_PLATFORM,
    push: bool = EXPORT_PUSH,
):
    """Writes the build to .mage/build/<name>/ and updates services/<name>/service.lock."""
    from mage_ai.pipeline_services import definitions, export, lifecycle

    try:
        box = lifecycle.build(project, name)
    except (definitions.DefinitionError, export.ExportError) as error:
        _service_error(error)
    typer.echo(f'Build: {os.path.relpath(box, project)}')
    typer.echo(f'Lock: {os.path.relpath(lifecycle.lock_path(project, name), project)} '
               '(commit it)')
    if push and not image:
        typer.echo('--push needs --image.', err=True)
        raise typer.Exit(code=1)
    if image:
        platforms = platform.split(',') if platform else None
        try:
            export.build_image(box, tag or name, platforms=platforms, push=push)
        except export.ExportError as error:
            _service_error(error)
        typer.echo(f'Image {tag or name} is {"pushed" if push else "built"}.')


@service_app.command('run')
def service_run(
    name: str = typer.Argument(...),
    project: str = SERVICE_PROJECT,
    env: str = typer.Option('dev', '--env', help='the environment whose values to use.'),
    port: int = typer.Option(6790, help='the port of the service API.'),
):
    """Runs the service on this machine's engine, without Docker, until Ctrl+C."""
    import signal
    import socket
    import subprocess

    from mage_ai.pipeline_services import definitions, engine, export, local_run

    with socket.socket() as probe:
        try:
            probe.bind(('127.0.0.1', port))
        except OSError:
            typer.echo(f'Port {port} is in use on this machine; pass another with --port.',
                       err=True)
            raise typer.Exit(code=1)
    try:
        run = local_run.prepare(project, name, environment=env, port=port)
    except (definitions.DefinitionError, export.ExportError) as error:
        _service_error(error)
    except (engine.EngineNotFound, local_run.LocalRunError) as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1)
    if run.missing:
        typer.echo(
            f'Not set: {", ".join(run.missing)}. Add them to '
            f'services/{name}/.env.{env} (git-ignored) or the environment.',
            err=True,
        )
    typer.echo(f'Service {name} on http://127.0.0.1:{run.port} (environment {env}).')
    typer.echo(f'API token: in {os.path.relpath(run.token_file, project)}, readable by you only.')
    typer.echo('Stop with Ctrl+C.')
    process = subprocess.Popen(run.command, env=run.env)
    try:
        code = process.wait()
    except KeyboardInterrupt:
        process.send_signal(signal.SIGTERM)
        code = process.wait()
    raise typer.Exit(code=code)


DEPLOY_ENV = typer.Option(..., '--env', help='the environment to deploy to.')


def _deploy_target(project: str, name: str, env: str):
    """The environment's deploy target: Docker (the default), Kubernetes or Code Engine."""
    from mage_ai.pipeline_services import code_engine, deploy, kubernetes, local_run

    try:
        settings = local_run.environment_settings(project, name, env)
        target = settings.get('target', 'docker')
        if target == 'docker':
            return deploy.DockerTarget(), settings
        if target == 'kubernetes':
            return kubernetes.KubernetesTarget(settings, environment=env), settings
        if target == 'code_engine':
            return code_engine.CodeEngineTarget(settings, environment=env), settings
    except (local_run.LocalRunError, deploy.DeployError) as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1)
    typer.echo(
        f'Environment {env} deploys to {target}; mage service deploy supports docker, '
        f'kubernetes and code_engine. Use the build context (mage service export {name}) '
        'and its deploy/ files.',
        err=True,
    )
    raise typer.Exit(code=1)


@service_app.command('deploy')
def service_deploy(
    name: str = typer.Argument(...),
    env: str = DEPLOY_ENV,
    project: str = SERVICE_PROJECT,
    timeout: int = typer.Option(120, help='seconds to wait for the service to be healthy.'),
    allow_uncommitted: Union[str, None] = typer.Option(
        None, '--allow-uncommitted', metavar='REASON',
        help='deploy uncommitted changes; the reason is recorded on the deployment.',
    ),
    if_changed: bool = typer.Option(
        False, '--if-changed',
        help='do nothing when the environment already runs this lock with these values.',
    ),
    from_env: Union[str, None] = typer.Option(
        None, '--from', metavar='ENV',
        help='promote: deploy the image ENV runs, tested there, without building.',
    ),
):
    """Builds, deploys to the environment's target, waits for health, rolls back on failure."""
    from mage_ai.pipeline_services import (
        definitions,
        deploy,
        export,
        lifecycle,
        local_run,
    )

    if from_env == env:
        typer.echo('--from names another environment than --env.', err=True)
        raise typer.Exit(code=1)
    target, settings = _deploy_target(project, name, env)
    on_cluster = settings.get('target') in ('kubernetes', 'code_engine')
    try:
        if from_env:
            source_target, _ = _deploy_target(project, name, from_env)
            source = source_target.inspect(name, from_env)
            if source is None or source.state != 'running' or not source.image:
                raise deploy.DeployError(
                    f'{name} is not running in {from_env}, so there is nothing to promote.')
            box, lock_sha, commit = None, source.lock, source.commit
            recorded = dict(promoted_from=from_env)
        else:
            result = lifecycle.check(project, name)
            if not result.ok:
                typer.echo(f'{name} cannot be built:', err=True)
                for problem in result.problems:
                    typer.echo(f'  - {problem}', err=True)
                raise typer.Exit(code=1)
            box = lifecycle.build(project, name)
            _refuse_uncommitted(project, name, result.capture, allow_uncommitted)
            lock_sha = str((lifecycle.read_lock(project, name) or {}).get('manifest_sha256', ''))
            commit = deploy.git_commit(project)
            recorded = {'reason': allow_uncommitted} if allow_uncommitted else {}
        values = local_run.environment_values(project, name, env)
        # The token stays the same across deploys from any machine: the environment's, this
        # machine's, or the one the deployment runs with.
        token_path = deploy.token_file(project, name, env)
        given = values.get('MAGE_SERVICE_TOKEN')
        if not given and not (token_path.is_file() and token_path.read_text().strip()):
            given = target.current_token(name, env)
        values['MAGE_SERVICE_TOKEN'] = local_run.service_token(token_path, given)
        labels = dict(commit=commit, lock=lock_sha, values=deploy.values_digest(values),
                      **recorded)
        if if_changed:
            current = target.inspect(name, env)
            if current is not None and current.state == 'running' \
                    and current.lock == lock_sha and current.values == labels['values']:
                typer.echo(f'{name} in {env} already runs this lock with these values.')
                return
        digest = lock_sha[:12] or 'latest'
        image = target.image(name, digest) if on_cluster else f'mage-{name}:{digest}'
        if from_env:
            if image != source.image:
                deploy.copy_image(source.image, image, push=on_cluster)
        elif on_cluster:
            for note in target.publish(box, image, export.build_image):
                typer.echo(note, err=True)
        else:
            export.build_image(box, image)
    except (definitions.DefinitionError, export.ExportError, local_run.LocalRunError) as error:
        _service_error(error)
    except deploy.DeployError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1)
    port = int(settings.get('port') or 6800)
    try:
        if on_cluster:
            deployment = target.deploy(name, env, image, values, labels, timeout=timeout)
        else:
            deployment = target.deploy(name, env, image, port, values, labels=labels,
                                       timeout=timeout)
    except deploy.DeployError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1)
    if settings.get('target') == 'kubernetes':
        where = f'deployment/mage-{name}-{env} in {target.context}, namespace {target.namespace}'
    elif settings.get('target') == 'code_engine':
        where = target.api_url(name, env)
    else:
        where = f'http://127.0.0.1:{port}'
    promoted = f', promoted from {from_env}' if from_env else ''
    typer.echo(f'{name} runs in {env} on {where} ({deployment.image}{promoted}).')
    typer.echo(f'API token: in {os.path.relpath(deploy.token_file(project, name, env), project)}.')


def _refuse_uncommitted(project: str, name: str, captured, reason: Union[str, None]) -> None:
    """What runs must be a commit: uncommitted inputs stop a deploy unless a reason is given."""
    from mage_ai.pipeline_services import lifecycle

    paths = lifecycle.uncommitted_inputs(project, name, captured)
    if paths is None:
        typer.echo('The project is not in a Git repository, so the deployment records no '
                   'commit.', err=True)
        return
    if not paths or reason:
        return
    lock = os.path.relpath(lifecycle.lock_path(project, name), project)
    typer.echo(f'{name} has uncommitted changes:', err=True)
    for path in paths[:20]:
        note = ' (the build updated it)' if path == lock else ''
        typer.echo(f'  {path}{note}', err=True)
    if len(paths) > 20:
        typer.echo(f'  and {len(paths) - 20} more', err=True)
    typer.echo('Commit them, or deploy anyway with --allow-uncommitted "<reason>"; the '
               'reason is recorded on the deployment.', err=True)
    raise typer.Exit(code=1)


@service_app.command('status')
def service_status(
    name: Union[str, None] = typer.Argument(None),
    project: str = SERVICE_PROJECT,
):
    """Per environment: what runs, its health, and whether it is the repository version."""
    from mage_ai.pipeline_services import deploy, lifecycle

    deployments, problems = deploy.deployments(project, name)
    for problem in problems:
        typer.echo(f'Could not read {problem}', err=True)
    if not deployments:
        typer.echo('No deployment found.' if not name else f'{name} is not deployed.')
        return
    for item in deployments:
        lock = (lifecycle.read_lock(project, item.service) or {}).get('manifest_sha256')
        commit = item.commit[:12] if item.commit else 'unknown commit'
        reason = f'  uncommitted: {item.reason}' if item.reason else ''
        if item.promoted_from:
            reason += f'  promoted from {item.promoted_from}'
        typer.echo(f'{item.service}  {item.environment}  {item.drift(lock)}  '
                   f'{item.image}  {commit}{reason}')


SERVICE_ENV = typer.Option(
    None, '--env', help='a deployed environment; without it, the local `mage service run`.',
)


def _service_endpoint(project: str, name: str, env: Union[str, None]):
    from mage_ai.pipeline_services import client, deploy

    target = _deploy_target(project, name, env)[0] if env else None
    try:
        return client.endpoint(project, name, env, target=target)
    except (client.ClientError, deploy.DeployError) as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1)


@service_app.command('runs')
def service_runs(
    name: str = typer.Argument(...),
    env: Union[str, None] = SERVICE_ENV,
    project: str = SERVICE_PROJECT,
    pipeline: Union[str, None] = typer.Option(None, help='only this pipeline\'s runs.'),
    status: Union[str, None] = typer.Option(
        None, help='queued, running, completed, failed or cancelled.'),
    limit: int = typer.Option(20, help='the most recent runs to show.'),
):
    """The service's latest runs: status, source, duration and error."""
    from mage_ai.pipeline_services import client

    api = _service_endpoint(project, name, env)
    try:
        items = client.runs(api, pipeline=pipeline, status=status, limit=limit)
    except client.ClientError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1)
    finally:
        api.close()
    typer.echo(client.runs_table(items) if items else 'No runs.')


CALL_VARIABLES = typer.Option(
    [], '--var', metavar='KEY=VALUE', help='a run variable (JSON values allowed); repeatable.')


@service_app.command('call')
def service_call(
    name: str = typer.Argument(...),
    pipeline: Union[str, None] = typer.Argument(
        None, help='the pipeline; optional when the service has one.'),
    env: Union[str, None] = SERVICE_ENV,
    project: str = SERVICE_PROJECT,
    variables: List[str] = CALL_VARIABLES,
    block: Union[str, None] = typer.Option(
        None, help='the block whose output to print; the last one with an output by default.'),
    wait: int = typer.Option(120, help='seconds to wait for the run.'),
):
    """Runs a pipeline of the service and prints each block's status and the result."""
    import json

    from mage_ai.pipeline_services import client

    values = {}
    for item in variables:
        key, separator, text = item.partition('=')
        if not separator or not key:
            typer.echo(f'--var {item}: write KEY=VALUE.', err=True)
            raise typer.Exit(code=1)
        try:
            values[key] = json.loads(text)
        except ValueError:
            values[key] = text
    api = _service_endpoint(project, name, env)
    try:
        run = client.call(api, pipeline, values, wait=wait)
        for item in run.get('blocks') or []:
            duration = item.get('duration_ms')
            took = f' in {duration / 1000:.2f} s' if duration is not None else ''
            typer.echo(f'{item["block"]}: {item["status"]}{took}')
            if item.get('error'):
                typer.echo(f'  {item["error"]}', err=True)
        typer.echo(f'Run {run["id"]} {run["status"]}.')
        if run['status'] != 'completed':
            if run.get('error'):
                typer.echo(run['error'], err=True)
            typer.echo(f'Its log: mage service logs {name} --run {run["id"]}'
                       + (f' --env {env}' if env else ''), err=True)
            raise typer.Exit(code=1)
        found = client.result(api, run, block)
        if found is not None:
            typer.echo(f'Output of {found[0]}:')
            typer.echo(json.dumps(found[1], indent=2, default=str))
    except client.ClientError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1)
    finally:
        api.close()


@service_app.command('logs')
def service_logs(
    name: str = typer.Argument(...),
    env: Union[str, None] = SERVICE_ENV,
    project: str = SERVICE_PROJECT,
    run: Union[str, None] = typer.Option(None, '--run', help='the run; the latest by default.'),
    block: Union[str, None] = typer.Option(None, help='only this block\'s lines.'),
    tail: Union[int, None] = typer.Option(None, help='only the last lines.'),
    engine_log: bool = typer.Option(
        False, '--engine', help='the engine\'s own log (start-up, schedules, streams).'),
    follow: bool = typer.Option(False, '--follow', '-f', help='with --engine, keep printing.'),
):
    """A run's log, or with --engine the deployed engine's own log."""
    import subprocess

    from mage_ai.pipeline_services import client, deploy

    if engine_log:
        if env is None:
            typer.echo('The engine of `mage service run` logs to its terminal; --engine needs '
                       '--env.', err=True)
            raise typer.Exit(code=1)
        try:
            command = _deploy_target(project, name, env)[0].log_command(
                name, env, tail=tail or 200, follow=follow)
        except deploy.DeployError as error:
            typer.echo(str(error), err=True)
            raise typer.Exit(code=1)
        try:
            raise typer.Exit(code=subprocess.run(command).returncode)
        except KeyboardInterrupt:
            raise typer.Exit(code=0)
    api = _service_endpoint(project, name, env)
    try:
        text = client.run_log(api, run_id=run, block=block, tail=tail)
    except client.ClientError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1)
    finally:
        api.close()
    typer.echo(text.rstrip('\n') if text.strip() else 'The run logged nothing.')


@service_app.command('diff')
def service_diff(
    name: str = typer.Argument(...),
    env: str = DEPLOY_ENV,
    project: str = SERVICE_PROJECT,
):
    """The service's files the repository changed since the deployed commit."""
    import subprocess

    from mage_ai.pipeline_services import lifecycle

    target, _ = _deploy_target(project, name, env)
    deployment = target.inspect(name, env)
    if deployment is None:
        typer.echo(f'{name} is not deployed in {env}.', err=True)
        raise typer.Exit(code=1)
    commit = deployment.commit.replace('+dirty', '')
    if not commit:
        typer.echo('The deployment has no commit label.', err=True)
        raise typer.Exit(code=1)
    try:
        paths = lifecycle.changed_paths(project, commit)
    except subprocess.CalledProcessError as error:
        typer.echo(f'Git could not compare with {commit}: {(error.stderr or "").strip()}',
                   err=True)
        raise typer.Exit(code=1)
    inputs = lifecycle.service_inputs(project, name, lifecycle.capture(project, name))
    changed = [
        path for path in paths
        if any(path == item or path.startswith(item.rstrip('/') + '/') for item in inputs)
    ]
    dirty = ' (deployed with uncommitted changes)' if '+dirty' in deployment.commit else ''
    if not changed:
        typer.echo(f'No change to {name} since the deployed commit {commit[:12]}{dirty}.')
        return
    typer.echo(f'Changed since the deployed commit {commit[:12]}{dirty}:')
    for path in changed:
        typer.echo(f'  {path}')


@service_app.command('rollback')
def service_rollback(
    name: str = typer.Argument(...),
    env: str = DEPLOY_ENV,
    project: str = SERVICE_PROJECT,
):
    """Runs the previous deployment of the environment again."""
    from mage_ai.pipeline_services import deploy

    target, _ = _deploy_target(project, name, env)
    try:
        deployment = target.rollback(name, env)
    except deploy.DeployError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1)
    typer.echo(f'{name} in {env} runs {deployment.image} again.')


CI_DEPLOY_RULES = typer.Option(
    [], '--deploy', metavar='ENV=BRANCH|ENV=tag:PREFIX*',
    help='deploy the services that define ENV on a push to BRANCH or a tag; repeatable.',
)
CI_PROMOTIONS = typer.Option(
    [], '--promote', metavar='ENV=SOURCE',
    help='ENV\'s rule deploys the image SOURCE runs instead of building; repeatable.',
)


@service_app.command('ci')
def service_ci(
    project: str = SERVICE_PROJECT,
    deploy_rules: List[str] = CI_DEPLOY_RULES,
    promotions: List[str] = CI_PROMOTIONS,
    force: bool = typer.Option(False, help='replace the workflow.'),
):
    """Writes a GitHub Actions workflow that checks pull requests and deploys by rule."""
    from mage_ai.pipeline_services import ci

    try:
        rules = [ci.parse_rule(text) for text in deploy_rules]
        ci.apply_promotions(rules, promotions)
        path = ci.write(project, rules, force=force)
    except ci.CiError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1)
    typer.echo(f'Wrote {path}.')
    typer.echo('Pull requests check the services they change.')
    for rule in rules:
        what = f'the image {rule.source} runs' if rule.source else 'a build'
        typer.echo(f'{rule.environment}: deploys {what} {rule.when}; set its secrets in '
                   f'Settings > Environments > {rule.environment} (see the comments in the '
                   'workflow).')


@service_app.command('export')
def service_export(
    name: str = typer.Argument(...),
    out: str = typer.Option(..., '--out', '-o', help='folder for the build context.'),
    force: bool = typer.Option(False, help='replace a non-empty folder.'),
    project: str = SERVICE_PROJECT,
):
    """Writes the build context to a folder, for a team that builds elsewhere."""
    from mage_ai.pipeline_services import definitions, export, lifecycle

    try:
        out_dir = lifecycle.export_to(project, name, out, force=force)
    except (definitions.DefinitionError, export.ExportError) as error:
        _service_error(error)
    typer.echo(f'Build context: {out_dir}')


@service_app.command('migrate')
def service_migrate(project: str = SERVICE_PROJECT):
    """Moves the services of pipeline_services.yaml to their own folders."""
    from mage_ai.pipeline_services import definitions

    try:
        moved = definitions.migrate(project)
    except definitions.DefinitionError as error:
        _service_error(error)
    if not moved:
        typer.echo('Nothing to move: every service already has its folder.')
    for name in moved:
        typer.echo(f'Moved {name} to services/{name}/service.yaml')

if __name__ == '__main__':
    app()
