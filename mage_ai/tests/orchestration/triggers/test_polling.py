import unittest
from types import SimpleNamespace
from unittest.mock import patch

from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.orchestration.triggers.utils import check_pipeline_run_status


class TriggerPollingTest(unittest.TestCase):
    def setUp(self):
        self.pipeline_run = SimpleNamespace(
            id=1, pipeline_uuid='polling', status=PipelineRun.PipelineRunStatus.RUNNING,
        )

    @patch('mage_ai.orchestration.triggers.utils.db_connection')
    def test_timeout_limits_sleep_duration(self, connection):
        clock = [10.0]

        def sleep(duration):
            self.assertLessEqual(duration, 0.25)
            clock[0] += duration

        with patch('mage_ai.orchestration.triggers.utils.monotonic',
                   side_effect=lambda: clock[0]), \
                patch('mage_ai.orchestration.triggers.utils.sleep', side_effect=sleep):
            with self.assertRaisesRegex(TimeoutError, 'time out'):
                check_pipeline_run_status(self.pipeline_run, poll_interval=20, poll_timeout=0.25)

    @patch('mage_ai.orchestration.triggers.utils.db_connection')
    def test_zero_timeout_does_not_wait(self, connection):
        with patch('mage_ai.orchestration.triggers.utils.sleep') as sleep:
            sleep.side_effect = AssertionError('Polling exceeded its timeout')
            with self.assertRaises(TimeoutError):
                check_pipeline_run_status(self.pipeline_run, poll_timeout=0)
            sleep.assert_not_called()

    @patch('mage_ai.orchestration.triggers.utils.db_connection')
    def test_completed_run_returns_at_zero_timeout(self, connection):
        self.pipeline_run.status = PipelineRun.PipelineRunStatus.COMPLETED
        self.assertIs(
            check_pipeline_run_status(self.pipeline_run, poll_timeout=0), self.pipeline_run,
        )

    def test_negative_polling_arguments_are_rejected(self):
        for options in ({'poll_interval': -1}, {'poll_timeout': -1}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                check_pipeline_run_status(self.pipeline_run, **options)
