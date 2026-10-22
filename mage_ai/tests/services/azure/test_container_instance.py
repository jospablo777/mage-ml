import os
import signal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from mage_ai.services.azure.container_instance import container_instance
from mage_ai.services.azure.container_instance.config import ContainerInstanceConfig
from mage_ai.tests.base_test import TestCase


def group(provisioning_state='Succeeded', state=None, exit_code=None, detail=None):
    current = None if state is None else SimpleNamespace(
        state=state, exit_code=exit_code, detail_status=detail,
    )
    return SimpleNamespace(
        provisioning_state=provisioning_state,
        containers=[SimpleNamespace(
            instance_view=None if current is None else SimpleNamespace(current_state=current),
        )],
    )


class RunJobTest(TestCase):
    def run_job(self, groups, sleep=None):
        client = MagicMock()
        client.container_groups.begin_create_or_update.return_value.done.return_value = True
        template = SimpleNamespace(
            containers=[SimpleNamespace(image='mage', environment_variables=[],
                                        volume_mounts=[])],
            location='westus', os_type='Linux', volumes=[],
        )
        client.container_groups.get.side_effect = [template] + list(groups)
        with patch.object(container_instance, 'ContainerInstanceManagementClient',
                          return_value=client), \
                patch.object(container_instance, 'DefaultAzureCredential'), \
                patch.object(container_instance.time, 'sleep', side_effect=sleep), \
                patch.dict(os.environ, {
                    'AZURE_SUBSCRIPTION_ID': 's', 'AZURE_RESOURCE_GROUP_NAME': 'rg',
                    'AZURE_CONTAINER_GROUP_NAME': 'mage',
                }):
            try:
                container_instance.run_job('mage run', 'job-1', ContainerInstanceConfig())
            finally:
                self.deleted = client.container_groups.begin_delete.call_args_list
        return client

    def test_waits_for_the_container_to_finish(self):
        """The group was deleted once provisioned, which stopped the block."""
        client = self.run_job([
            group(state='Waiting'),
            group(state='Running'),
            group(state='Terminated', exit_code=0),
        ])

        self.assertEqual(client.container_groups.get.call_count, 4)
        self.assertEqual(len(self.deleted), 1)

    def test_a_failed_container_fails_the_block(self):
        """A non-zero exit code counted as a success."""
        with self.assertRaisesRegex(Exception, 'exit code 1: Error'):
            self.run_job([group(state='Terminated', exit_code=1, detail='Error')])

        self.assertEqual(len(self.deleted), 1)

    def test_a_group_not_provisioned_fails_the_block(self):
        with self.assertRaisesRegex(Exception, 'was not provisioned: Failed'):
            self.run_job([group(provisioning_state='Failed')])

        self.assertEqual(len(self.deleted), 1)

    def test_a_job_stopped_with_sigterm_deletes_the_group(self):
        with self.assertRaises(SystemExit):
            self.run_job(
                [group(state='Running'), group(state='Running')],
                sleep=lambda _seconds: os.kill(os.getpid(), signal.SIGTERM),
            )

        self.assertEqual(len(self.deleted), 1)
