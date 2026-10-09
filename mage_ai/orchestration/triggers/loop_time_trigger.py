import time

from mage_ai.orchestration.triggers.time_trigger import TimeTrigger
from mage_ai.settings import SCHEDULER_TRIGGER_INTERVAL

# Seconds between scheduler runs at least, when jobs finish often.
MIN_TRIGGER_INTERVAL = 1
# Seconds between checks for finished jobs while the scheduler waits.
FINISHED_JOBS_POLL_INTERVAL = 0.25


class LoopTimeTrigger(TimeTrigger):
    def __init__(self, trigger_interval=SCHEDULER_TRIGGER_INTERVAL) -> None:
        super().__init__(trigger_interval)

    def start(self) -> None:
        while True:
            self.last_run_time = time.monotonic()
            self.run()
            self.wait()

    def wait(self) -> None:
        """
        Waits for the next scheduler run, which comes early when a job finishes: the
        block runs downstream of a finished one start then. They waited for the next
        run, up to SCHEDULER_TRIGGER_INTERVAL (10 seconds), so a pipeline of 5 blocks
        spent most of a minute waiting.
        """
        from mage_ai.orchestration.job_manager import get_job_manager

        job_manager = get_job_manager()
        deadline = self.last_run_time + self.trigger_interval
        earliest = self.last_run_time + MIN_TRIGGER_INTERVAL
        while True:
            now = time.monotonic()
            if now >= deadline:
                return
            if now >= earliest and job_manager is not None and job_manager.jobs_finished():
                return
            time.sleep(min(FINISHED_JOBS_POLL_INTERVAL, deadline - now))
