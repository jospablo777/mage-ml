from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from botocore.exceptions import WaiterError

from mage_ai.services.aws.ecs import ecs
from mage_ai.tests.base_test import TestCase

ARN = 'arn:aws:ecs:us-east-1:1:task/c/abc'


class RunTaskTest(TestCase):
    def run_task(self, client, wait_timeout=60):
        config = SimpleNamespace(
            cluster='c', container_name='mage', wait_timeout=wait_timeout,
            get_task_config=lambda command: dict(command=command),
        )
        with patch.object(ecs, 'get_aws_boto3_client', return_value=client), \
                patch.object(ecs, 'print'):
            return ecs.run_task('mage run', config)

    def client(self):
        client = MagicMock()
        client.run_task.return_value = {'tasks': [{'taskArn': ARN}]}
        return client

    def test_a_task_that_outlives_the_wait_is_stopped(self):
        """It kept running after the block failed, and a retry ran it twice (#4655)."""
        client = self.client()
        client.get_waiter.return_value.wait.side_effect = WaiterError(
            'TasksStopped', 'Max attempts exceeded', {},
        )

        with self.assertRaisesRegex(Exception, 'did not finish within the wait timeout'):
            self.run_task(client)

        client.stop_task.assert_called_once()
        self.assertEqual(client.stop_task.call_args.kwargs['task'], ARN)

    def test_a_cancelled_run_stops_the_task(self):
        client = self.client()
        client.get_waiter.return_value.wait.side_effect = KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            self.run_task(client)

        client.stop_task.assert_called_once()

    def test_a_container_that_never_started_gives_its_reason(self):
        client = self.client()
        client.describe_tasks.return_value = {'tasks': [{
            'containers': [{'name': 'mage', 'reason': 'CannotPullContainerError'}],
            'stoppedReason': 'Essential container in task exited',
        }]}

        with self.assertRaisesRegex(Exception, 'CannotPullContainerError'):
            self.run_task(client)

    def test_a_task_ecs_did_not_start_reports_the_failures(self):
        client = self.client()
        client.run_task.return_value = {'tasks': [], 'failures': [{'reason': 'RESOURCE:CPU'}]}

        with self.assertRaisesRegex(Exception, 'RESOURCE:CPU'):
            self.run_task(client)
