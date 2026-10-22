import os
import time
import traceback

from azure.identity import DefaultAzureCredential
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    ContainerGroupRestartPolicy,
    ResourceRequests,
    ResourceRequirements,
)

from mage_ai.server.logger import Logger
from mage_ai.services.azure.constants import (
    ENV_VAR_CONTAINER_GROUP_NAME,
    ENV_VAR_RESOURCE_GROUP_NAME,
    ENV_VAR_STORAGE_ACCOUNT_KEY,
    ENV_VAR_SUBSCRIPTION_ID,
)
from mage_ai.services.azure.container_instance.config import ContainerInstanceConfig
from mage_ai.shared.processes import sigterm_raises

logger = Logger().new_server_logger(__name__)


def run_job(command: str, job_id: str, container_instance_config: ContainerInstanceConfig):
    if type(container_instance_config) is dict:
        container_instance_config = ContainerInstanceConfig.load(config=container_instance_config)

    curr_container_group_name = os.getenv(ENV_VAR_CONTAINER_GROUP_NAME)
    resource_group_name = os.getenv(ENV_VAR_RESOURCE_GROUP_NAME)
    subscription_id = os.getenv(ENV_VAR_SUBSCRIPTION_ID)

    credential = DefaultAzureCredential()

    # Create the ACI management client
    aci_client = ContainerInstanceManagementClient(credential, subscription_id)

    # Get current container group
    curr_container_group = aci_client.container_groups.get(
        resource_group_name,
        curr_container_group_name,
    )
    curr_container = curr_container_group.containers[0]

    # Configure the container
    container_resource_requests = ResourceRequests(
        cpu=container_instance_config.cpu,
        memory_in_gb=container_instance_config.memory,
    )
    container_resource_requirements = ResourceRequirements(
        requests=container_resource_requests,
    )
    container = Container(
        command=command.split(' '),
        environment_variables=curr_container.environment_variables,
        image=curr_container.image,
        name=job_id,
        resources=container_resource_requirements,
        volume_mounts=curr_container.volume_mounts,
    )

    # Configure the container group
    volumes = []
    if curr_container_group.volumes:
        volume = curr_container_group.volumes[0]
        volume.azure_file.storage_account_key = os.getenv(ENV_VAR_STORAGE_ACCOUNT_KEY)
        volumes = [volume]
    group = ContainerGroup(
        containers=[container],
        location=curr_container_group.location,
        os_type=curr_container_group.os_type,
        restart_policy=ContainerGroupRestartPolicy.never,
        volumes=volumes,
    )

    # Create the container group
    result = aci_client.container_groups.begin_create_or_update(
        resource_group_name,
        job_id,
        group,
    )
    logger.info(f'Begin creating or updating the job {job_id}')

    # The create operation is done once the group is provisioned (Succeeded, Canceled or
    # Failed); the container runs after that. The group was deleted at that point, which
    # stopped the block, and a failed block counted as a success.
    try:
        with sigterm_raises():
            while result.done() is False:
                time.sleep(5)

            container_group = aci_client.container_groups.get(resource_group_name, job_id)
            if str(container_group.provisioning_state).lower() != 'succeeded':
                raise Exception(
                    f'Container group {job_id} was not provisioned: '
                    f'{container_group.provisioning_state}.',
                )

            state = container_state(container_group)
            while state is None or str(state.state).lower() != 'terminated':
                time.sleep(5)
                container_group = aci_client.container_groups.get(resource_group_name, job_id)
                state = container_state(container_group)

            if state.exit_code != 0:
                raise Exception(
                    f'Job {job_id} failed with exit code {state.exit_code}'
                    + (f': {state.detail_status}' if state.detail_status else '.'),
                )
            logger.info(f'Executing job {job_id} succeeded.')
    finally:
        # Also when the block run is stopped (SIGTERM) or the wait fails: the container
        # group kept running otherwise.
        try:
            aci_client.container_groups.begin_delete(resource_group_name, job_id)
        except Exception:
            traceback.print_exc()


def container_state(container_group):
    """The state of the group's container: Waiting, Running or Terminated, and exit code."""
    containers = container_group.containers or []
    if not containers or containers[0].instance_view is None:
        return None
    return containers[0].instance_view.current_state
