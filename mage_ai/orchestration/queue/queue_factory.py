import os

from mage_ai.data_preparation.repo_manager import get_repo_config
from mage_ai.orchestration.queue.config import QueueConfig, QueueType


def job_engine_requested() -> bool:
    """MAGE_JOB_ENGINE=on runs the scheduler's jobs on the Rust run engine."""
    return (os.getenv('MAGE_JOB_ENGINE') or '').strip().lower() in ('on', '1', 'true', 'yes')


class QueueFactory:
    queue = None

    @classmethod
    def get_queue(self):
        if self.queue is not None:
            return self.queue

        queue_config = QueueConfig.load(config=get_repo_config().queue_config or dict())
        if queue_config.queue_type == QueueType.CELERY:
            from mage_ai.orchestration.queue.celery_queue import CeleryQueue
            self.queue = CeleryQueue(queue_config)
            return self.queue
        if job_engine_requested():
            self.queue = _engine_queue(queue_config)
            if self.queue is not None:
                return self.queue
        from mage_ai.orchestration.queue.process_queue import ProcessQueue
        self.queue = ProcessQueue(queue_config)
        return self.queue


def _engine_queue(queue_config: QueueConfig):
    """The engine queue, or None with the reason printed when the engine cannot run jobs."""
    from mage_ai.orchestration.queue.engine_queue import (
        EngineQueue,
        engine_supports_jobs,
    )
    from mage_ai.pipeline_services.engine import EngineNotFound, engine_path

    try:
        engine = engine_path()
    except EngineNotFound as error:
        print(f'[QueueFactory] MAGE_JOB_ENGINE is on, but {error} Jobs run in processes.')
        return None
    if not engine_supports_jobs(engine):
        print(f'[QueueFactory] MAGE_JOB_ENGINE is on, but {engine} has no `jobs` command '
              '(upgrade mageml-engine). Jobs run in processes.')
        return None
    return EngineQueue(queue_config, engine)
