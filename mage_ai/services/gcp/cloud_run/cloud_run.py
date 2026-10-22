import json
import os
from typing import Dict

from google.api.launch_stage_pb2 import LaunchStage
from google.api_core.exceptions import AlreadyExists
from google.cloud import run_v2
from google.oauth2 import service_account
from google.protobuf.duration_pb2 import Duration

from mage_ai.server.logger import Logger
from mage_ai.services.gcp.cloud_run.config import CloudRunConfig
from mage_ai.shared.processes import sigterm_raises

logger = Logger().new_server_logger(__name__)


CLOUD_RUN_CLIENT_SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/compute',
]


def run_job(command: str, job_id: str, cloud_run_config: CloudRunConfig) -> Dict:
    if type(cloud_run_config) is dict:
        cloud_run_config = CloudRunConfig.load(config=cloud_run_config)

    if cloud_run_config.path_to_credentials_json_file:
        credentials = service_account.Credentials.from_service_account_file(
            cloud_run_config.path_to_credentials_json_file,
            scopes=CLOUD_RUN_CLIENT_SCOPES,
        )
    else:
        credentials = None
    jobs_client = run_v2.JobsClient(credentials=credentials)
    services_client = run_v2.ServicesClient(credentials=credentials)

    # Get existing service
    resource_prefix = f'projects/{cloud_run_config.project_id}/locations/{cloud_run_config.region}'
    existing_service_name = os.getenv('GCP_SERVICE_NAME')
    existing_service = services_client.get_service(
        run_v2.GetServiceRequest(
            name=f'{resource_prefix}/services/{existing_service_name}'
        )
    )
    service_template = existing_service.template

    # Create job
    containers_with_cmd = service_template.containers
    for c in containers_with_cmd:
        c.command = command.split(' ') if isinstance(command, str) else command
        # Not pass startup_probe and liveness_probe to cloud run jobs
        c.startup_probe = None
        c.liveness_probe = None
    execution_template = run_v2.ExecutionTemplate(
        task_count=1,
        template=run_v2.TaskTemplate(
            containers=containers_with_cmd,
            volumes=service_template.volumes,
            service_account=service_template.service_account,
            execution_environment=service_template.execution_environment,
            encryption_key=service_template.encryption_key,
            timeout=Duration(seconds=cloud_run_config.timeout_seconds),
            vpc_access=service_template.vpc_access,
        )
    )
    job = run_v2.Job(
        launch_stage=LaunchStage.BETA,
        template=execution_template,
    )

    request = run_v2.CreateJobRequest(
        parent=resource_prefix,
        job=job,
        job_id=job_id,
    )
    try:
        operation = jobs_client.create_job(request=request)
        logger.info("Waiting for create_job operation to complete...")
        response = operation.result()
        logger.info(json.dumps(response, indent=4, default=str))
    except AlreadyExists:
        pass

    job_name = f'{resource_prefix}/jobs/{job_id}'

    # Run job
    operation = jobs_client.run_job(request=run_v2.RunJobRequest(
        name=job_name,
    ))
    logger.info('Waiting for run_job operation to complete...')
    try:
        with sigterm_raises():
            response = operation.result(timeout=cloud_run_config.timeout_seconds)
    except BaseException:
        # A wait that timed out or a block run stopped by the scheduler (SIGTERM) left the
        # execution running and the job in place; a retry ran the block twice.
        cancel_execution(operation, credentials)
        raise
    finally:
        delete_job(jobs_client, job_name)
    logger.info(json.dumps(response, indent=4, default=str))

    return response


def cancel_execution(operation, credentials=None) -> None:
    try:
        name = getattr(operation.metadata, 'name', None)
        if name:
            run_v2.ExecutionsClient(credentials=credentials).cancel_execution(
                request=run_v2.CancelExecutionRequest(name=name),
            )
            logger.info(f'Cancelled execution {name}.')
    except Exception as error:
        logger.warning(f'Could not cancel the Cloud Run execution: {error}')


def delete_job(jobs_client, job_name: str) -> None:
    try:
        delete_operation = jobs_client.delete_job(
            request=run_v2.DeleteJobRequest(name=job_name),
        )
        logger.info('Waiting for delete_job operation to complete...')
        delete_response = delete_operation.result()
        logger.info(json.dumps(delete_response, indent=4, default=str))
    except Exception as error:
        logger.warning(f'Could not delete Cloud Run job {job_name}: {error}')
