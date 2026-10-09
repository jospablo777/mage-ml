from unittest.mock import MagicMock, patch

from mage_ai.orchestration.triggers import loop_time_trigger
from mage_ai.orchestration.triggers.loop_time_trigger import LoopTimeTrigger
from mage_ai.tests.base_test import TestCase


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class LoopTimeTriggerTests(TestCase):
    @patch('mage_ai.orchestration.triggers.time_trigger.check_sla')
    @patch('mage_ai.orchestration.triggers.time_trigger.schedule_all')
    def test_run(self, mock_schedule_all, mock_check_sla):
        trigger = LoopTimeTrigger()
        trigger.run()
        mock_schedule_all.assert_called_once()
        mock_check_sla.assert_called_once()

    def wait(self, finished_at=None):
        """Waits from a run at 100 s; a job finishes at finished_at. Returns the end."""
        clock = FakeClock()
        job_manager = MagicMock()
        job_manager.jobs_finished.side_effect = lambda: (
            finished_at is not None and clock.now >= finished_at
        )
        trigger = LoopTimeTrigger(trigger_interval=10)
        trigger.last_run_time = clock.now
        with patch.object(loop_time_trigger.time, 'monotonic', clock.monotonic), \
                patch.object(loop_time_trigger.time, 'sleep', clock.sleep), \
                patch('mage_ai.orchestration.job_manager.get_job_manager',
                      return_value=job_manager):
            trigger.wait()
        return clock.now

    def test_waits_for_the_interval_without_finished_jobs(self):
        self.assertEqual(self.wait(), 110)

    def test_a_finished_job_ends_the_wait(self):
        self.assertAlmostEqual(self.wait(finished_at=103), 103, delta=0.25)

    def test_runs_are_at_least_a_second_apart(self):
        self.assertEqual(self.wait(finished_at=100), 101)
