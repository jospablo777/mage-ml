import os
import signal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from mage_ai.services.gcp.cloud_run import cloud_run
from mage_ai.services.gcp.cloud_run.config import CloudRunConfig
from mage_ai.tests.base_test import TestCase

EXECUTION = 'projects/p/locations/r/jobs/job-1/executions/job-1-abc'


class RunJobTest(TestCase):
    def run_job(self, result):
        jobs = MagicMock()
        operation = MagicMock(metadata=SimpleNamespace(name=EXECUTION))
        operation.result.side_effect = result
        jobs.run_job.return_value = operation
        jobs.delete_job.return_value.result.return_value = {}
        executions = MagicMock()
        services = MagicMock()
        services.get_service.return_value.template = MagicMock(containers=[MagicMock()])
        run_v2 = MagicMock()
        run_v2.JobsClient.return_value = jobs
        run_v2.ServicesClient.return_value = services
        run_v2.ExecutionsClient.return_value = executions
        with patch.object(cloud_run, 'run_v2', run_v2), \
                patch.object(cloud_run, 'Duration'), \
                patch.object(cloud_run.json, 'dumps', return_value=''):
            try:
                cloud_run.run_job(
                    'mage run', 'job-1', CloudRunConfig(project_id='p', region='r'),
                )
            finally:
                self.jobs, self.executions, self.run_v2 = jobs, executions, run_v2

    def test_the_job_is_deleted_after_it_completes(self):
        self.run_job(lambda timeout=None: {'succeeded_count': 1})

        self.jobs.delete_job.assert_called_once()
        self.executions.cancel_execution.assert_not_called()

    def test_a_wait_that_times_out_cancels_the_execution(self):
        """The execution kept running and the job stayed; a retry ran the block twice."""
        with self.assertRaises(TimeoutError):
            self.run_job(TimeoutError('deadline'))

        self.run_v2.CancelExecutionRequest.assert_called_once_with(name=EXECUTION)
        self.executions.cancel_execution.assert_called_once()
        self.jobs.delete_job.assert_called_once()

    def test_a_job_stopped_with_sigterm_cancels_the_execution(self):
        with self.assertRaises(SystemExit):
            self.run_job(lambda timeout=None: os.kill(os.getpid(), signal.SIGTERM))

        self.executions.cancel_execution.assert_called_once()
        self.jobs.delete_job.assert_called_once()
