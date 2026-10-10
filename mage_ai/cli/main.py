import json
import os
import sys
from typing import List, Union

import typer
from click import Context
from rich import print
from typer.core import TyperGroup

from mage_ai.cli.utils import parse_runtime_variables
from mage_ai.data_preparation.repo_manager import ProjectType
from mage_ai.services.newrelic import initialize_new_relic
from mage_ai.shared.constants import ENV_VAR_INSTANCE_TYPE, InstanceType


class OrderCommands(TyperGroup):
    def list_commands(self, ctx: Context):
        """Return list of commands in the order they appear."""
        return list(self.commands)


app = typer.Typer(
    cls=OrderCommands,
    pretty_exceptions_show_locals=False,
)

# Defaults

INIT_PROJECT_PATH_DEFAULT = typer.Argument(..., help='path of the Mage project to be created.')
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
    os.getcwd(), help='path of the Mage project to be loaded.'
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
    ..., help='path of the Mage project that contains the pipeline.'
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
RUN_TEMPLATE_RUNTIME_CONFIGURATION_DEFAULT = typer.Option(
    None, help='runtime configuration of data integration block runs.'
)
CLEAN_LOGS_PROJECT_PATH_DEFAULT = typer.Argument(
    ..., help='path of the Mage project to clean old logs.'
)
CLEAN_LOGS_PIPELINE_UUID_DEFAULT = typer.Option(None, help='uuid of the pipeline to clean.')
CLEAN_VARIABLES_PROJECT_PATH_DEFAULT = typer.Argument(
    ..., help='path of the Mage project to clean variables.'
)
CLEAN_VARIABLES_PIPELINE_UUID_DEFAULT = typer.Option(None, help='uuid of the pipeline to clean.')

CREATE_SPARK_CLUSTER_PROJECT_PATH_DEFAULT = typer.Argument(
    ..., help='path of the Mage project that contains the EMR config.'
)


@app.command()
def init(
    project_path: str = INIT_PROJECT_PATH_DEFAULT,
    project_type: Union[str, None] = INIT_PROJECT_TYPE_DEFAULT,
    cluster_type: str = INIT_CLUSTER_TYPE_DEFAULT,
    project_uuid: str = INIT_PROJECT_UUID_DEFAULT,
):
    """
    Initialize Mage project.
    """
    from mage_ai.data_preparation.repo_manager import init_repo

    repo_path = os.path.join(os.getcwd(), project_path)
    init_repo(
        repo_path,
        project_type=project_type,
        cluster_type=cluster_type,
        project_uuid=project_uuid,
    )
    print(f'Initialized Mage project at {repo_path}')


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
    Start Mage server and UI.
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
):
    """
    Run pipeline.
    """
    from mage_ai.settings.repo import set_repo_path

    # Set repo_path before intializing the DB so that we can get correct db_connection_url
    project_path = os.path.abspath(project_path)
    set_repo_path(project_path)

    from contextlib import nullcontext

    import newrelic.agent
    import sentry_sdk

    from mage_ai.data_preparation.executors.executor_factory import ExecutorFactory
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
        if runtime_vars is not None:
            runtime_variables = parse_runtime_variables(runtime_vars)

        sys.path.append(os.path.dirname(project_path))
        # Initialize db_connection session before getting the pipeline in case
        # "mage_secret_var" syntax is used in the project's metadata.yaml
        db_connection.start_session()
        pipeline = Pipeline.get(pipeline_uuid, repo_path=project_path)

        if pipeline_run_id is None:
            default_variables = get_global_variables(pipeline_uuid)
            global_vars = merge_dict(default_variables, runtime_variables)
        else:
            pipeline_run = PipelineRun.get_by_id(pipeline_run_id)
            global_vars = pipeline_run.get_variables(extra_variables=runtime_variables)

        if template_runtime_configuration is not None:
            template_runtime_configuration = json.loads(template_runtime_configuration)

        if block_uuid is None:
            ExecutorFactory.get_pipeline_executor(
                pipeline,
                execution_partition=execution_partition,
                executor_type=executor_type,
            ).execute(
                analyze_outputs=False,
                global_vars=global_vars,
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
        print('Pipeline run completed.')


@app.command()
def clean_cached_variables(
    project_path: str = CLEAN_VARIABLES_PROJECT_PATH_DEFAULT,
    pipeline_uuid: str = CLEAN_VARIABLES_PIPELINE_UUID_DEFAULT,
):
    from mage_ai.settings.repo import set_repo_path

    project_path = os.path.abspath(project_path)
    set_repo_path(project_path)

    from mage_ai.data_preparation.variable_manager import clean_variables

    clean_variables(pipeline_uuid=pipeline_uuid)


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
    Create EMR cluster for Mage project.
    """
    from mage_ai.services.aws.emr.launcher import create_cluster

    project_path = os.path.abspath(project_path)
    create_cluster(project_path)


def _contract_block(project_path: str, pipeline_uuid: str, block_uuid: str):
    from mage_ai.settings.repo import set_repo_path

    set_repo_path(project_path)
    sys.path.append(os.path.dirname(project_path))

    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline = Pipeline.get(pipeline_uuid, repo_path=project_path, check_if_exists=True)
    block = pipeline.get_block(block_uuid) if pipeline else None
    if block is None:
        print(f'[red]Block {block_uuid} does not exist in pipeline {pipeline_uuid}.[/red]')
        raise typer.Exit(code=2)
    return block


@app.command('contract-draft')
def contract_draft(
    project_path: str = typer.Argument(..., help='path of the Mage project.'),
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
        print(f'[red]{error}[/red]')
        raise typer.Exit(code=2)
    if write:
        path = contracts.contracts_dir(project_path) / f'{name}.yaml'
        if path.exists() and not force:
            print(f'[red]{path} exists; pass --force to replace it.[/red]')
            raise typer.Exit(code=2)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(document)
        typer.echo(f'Wrote {path}')
    else:
        typer.echo(document, nl=False)


@app.command('contract-check')
def contract_check(
    project_path: str = typer.Argument(..., help='path of the Mage project.'),
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
        print(f'[red]{error}[/red]')
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
    sys.path.append(os.path.dirname(project_path))

    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.orchestration.db import db_connection

    db_connection.start_session()
    pipeline = Pipeline.get(pipeline_uuid, repo_path=project_path, check_if_exists=True)
    if pipeline is None:
        print(f'[red]Pipeline {pipeline_uuid} does not exist in {project_path}.[/red]')
        raise typer.Exit(code=2)
    return pipeline


def _pipeline_run(pipeline, run_id: int):
    from mage_ai.orchestration.db.models.schedules import PipelineRun

    run = PipelineRun.get(run_id)
    if run is None or run.pipeline_uuid != pipeline.uuid:
        print(f'[red]Pipeline run {run_id} of {pipeline.uuid} does not exist.[/red]')
        raise typer.Exit(code=2)
    return run


@app.command('run-diff')
def run_diff(
    project_path: str = typer.Argument(..., help='path of the Mage project.'),
    pipeline_uuid: str = typer.Argument(..., help='uuid of the pipeline.'),
    first_run_id: int = typer.Argument(..., help='id of the earlier pipeline run.'),
    second_run_id: int = typer.Argument(..., help='id of the later pipeline run.'),
    as_json: bool = typer.Option(False, '--json', help='print the comparison as JSON.'),
):
    """
    Compare what two runs of a pipeline ran with: code (with a diff of each changed file),
    Python, Mage and package versions, variables, and which blocks' outputs differ.
    """
    from mage_ai.orchestration import run_records

    project_path = os.path.abspath(project_path)
    pipeline = _run_records_setup(project_path, pipeline_uuid)
    runs = [_pipeline_run(pipeline, run_id) for run_id in (first_run_id, second_run_id)]
    try:
        comparison = run_records.compare(pipeline, *runs)
    except run_records.RunRecordError as error:
        print(f'[red]{error}[/red]')
        raise typer.Exit(code=2)
    if as_json:
        typer.echo(json.dumps(comparison, indent=2, default=str))
    else:
        typer.echo(run_records.format_comparison(comparison))


@app.command('reproduce')
def reproduce(
    project_path: str = typer.Argument(..., help='path of the Mage project.'),
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
        print(f'[red]{error}[/red]')
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
    project_path: str = typer.Argument(..., help='path of the Mage project.'),
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
            print(f'[red]{model} has no release policy in releases/.[/red]')
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
        print(f'[red]{error}[/red]')
        raise typer.Exit(code=2)


@app.command('release-promote')
def release_promote(
    project_path: str = typer.Argument(..., help='path of the Mage project.'),
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
            print(f'[red]{model} has no release policy in releases/.[/red]')
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
        print(f'[red]{error}[/red]')
        raise typer.Exit(code=2)
    typer.echo(f'{model} {entry["alias"]} is now version {entry["version"]}.')


@app.command('release-rollback')
def release_rollback(
    project_path: str = typer.Argument(..., help='path of the Mage project.'),
    model: str = typer.Argument(..., help='name of the registered model.'),
):
    """Give the model's alias back to the version that held it before the last promotion."""
    import getpass

    from mage_ai.orchestration import releases

    project_path = _releases_setup(project_path)
    try:
        entry = releases.rollback(project_path, model, getpass.getuser())
    except releases.ReleaseError as error:
        print(f'[red]{error}[/red]')
        raise typer.Exit(code=2)
    typer.echo(f'{model} {entry["alias"]} is back to version {entry["version"]}.')


@app.command('verify-fusion')
def verify_fusion(
    project_path: str = typer.Argument(..., help='path of the Mage project.'),
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
    sys.path.append(os.path.dirname(project_path))

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
        print(f'[red]{error}[/red]')
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

R_PROJECT_PATH_DEFAULT = typer.Argument('.', help='path of the Mage project.')
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
        print(f'[red]{error}[/red]')
        raise typer.Exit(code=1)
    if not config.uses_rv:
        print(
            f'[red]{os.path.abspath(project_path)} has no R environment. Create it with '
            f'`mage r init {project_path}`.[/red]'
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
            print(f'[green]✓[/green] {step["name"]}: {step["found"]}')
            continue
        optional = step.get('optional', False)
        missing = missing or not optional
        print(f'[yellow]✗[/yellow] {step["name"]}: install it with')
        for command in step['commands']:
            print(f'    {command}')
    if missing:
        print('Then run `mage r setup` again.')
        raise typer.Exit(code=1)
    print(
        f'R blocks can be set up. Create the R environment with `mage r init {project_path}`; '
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
            print(f'Installing R {r_version} with rig.')
            runtime.install_r(r_version)
        runtime.init_project(directory, packages=packages or None, r_version=r_version)
    except runtime.REnvironmentError as error:
        print(f'[red]{error}[/red]')
        raise typer.Exit(code=1)
    print(f'Created the R environment in {directory}. Add packages with `rv add` there.')


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
        print(f'[red]{error}[/red]')
        raise typer.Exit(code=1)
    print(f'The R library of {config.project_dir} is synced.')


@r_app.command('status')
def r_status(project_path: str = R_PROJECT_PATH_DEFAULT):
    """
    Check that R blocks can run: the R version, the library and the packages Mage needs.
    """
    from mage_ai.data_preparation.models.block.r import runtime

    config = _r_config(project_path)
    report = runtime.status(config)
    for key, value in report['details'].items():
        print(f'{key}: {value}')
    for problem in report['problems']:
        print(f'[red]{problem}[/red]')
    if report['problems']:
        raise typer.Exit(code=1)
    print('R blocks can run.')


rust_app = typer.Typer(
    cls=OrderCommands,
    help='Manage the Cargo workspace that Rust blocks build in.',
    pretty_exceptions_show_locals=False,
)
app.add_typer(rust_app, name='rust')

RUST_PROJECT_PATH_DEFAULT = typer.Argument('.', help='path of the Mage project.')


@rust_app.command('init')
def rust_init(project_path: str = RUST_PROJECT_PATH_DEFAULT):
    """
    Create the project's Rust environment in <project>/rust: the crates Rust blocks use.
    """
    from mage_ai.data_preparation.models.block.rust import workspace

    created = workspace.init(os.path.abspath(project_path))
    print(
        f'The Rust environment is in {created.root}. Add crates under '
        f'[workspace.dependencies] in {created.cargo_toml}.'
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
            print(f'[green]✓[/green] {report.block.label}: {how}')
        else:
            print(f'[red]✗[/red] {report.block.label}\n{report.error}')
    failed = sum(not report.ok for report in reports)
    if failed:
        print(f'[red]{failed} of {len(reports)} Rust blocks do not build.[/red]')
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
        print(f'{key}: {value}')
    for problem in report['problems']:
        print(f'[red]{problem}[/red]')
    if report['problems']:
        raise typer.Exit(code=1)
    print('Rust blocks can build.')


export_app = typer.Typer(
    cls=OrderCommands,
    help='Export pipelines to run without Mage.',
    pretty_exceptions_show_locals=False,
)
app.add_typer(export_app, name='export')


EXPORT_PROJECT_PATH = typer.Argument(..., help='path of the Mage project.')
EXPORT_PIPELINES = typer.Argument(..., help='uuids of the pipelines to export.')
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
):
    """
    Export pipelines as a standalone Docker service: an HTTP API, schedules, run history
    and logs, without Mage. Writes a build context with a Dockerfile, compose.yaml and a
    README that shows how to run and call it.
    """
    from mage_ai.pipeline_services import export

    try:
        captured = export.capture(
            project_path, pipelines, name=name, max_concurrent_runs=max_runs, models=model,
        )
        out_dir = export.write(captured, out, force=force)
    except export.ExportError as error:
        typer.echo('The pipelines cannot be exported:', err=True)
        for problem in error.problems:
            typer.echo(f'  - {problem}', err=True)
        raise typer.Exit(code=1)
    typer.echo(export.report(captured))
    typer.echo(f'Build context: {out_dir}')
    if build:
        image = tag or captured.name
        try:
            export.build_image(out_dir, image)
        except export.ExportError as error:
            typer.echo(error.problems[0], err=True)
            raise typer.Exit(code=1)
        typer.echo(f'Image {image} is built. Run it with:')
        typer.echo(f'  docker run --rm -p 8080:8080 -e MAGE_SERVICE_TOKEN=change-me {image}')
    else:
        typer.echo(f'Build the image with: docker build -t {captured.name} {out_dir}')


if __name__ == '__main__':
    app()
