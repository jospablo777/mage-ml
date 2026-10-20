import os

from mage_ai.data_preparation.repo_manager import get_repo_config
from mage_ai.orchestration.queue.config import QueueConfig, QueueType


def job_engine_mode() -> str:
    """
    Where the scheduler's jobs run, by MAGE_JOB_ENGINE:
    - auto (unset): in the warm workers of the Rust engine when one is installed (the
      mageml-engine wheel, MAGE_ENGINE_PATH or mage-service on PATH), in processes otherwise;
    - on: on the engine, an engine cargo built in a source checkout too; a warning and
      processes when there is none;
    - off: in a process per job, as before the engine.
    """
    value = (os.getenv('MAGE_JOB_ENGINE') or '').strip().lower()
    if value in ('on', '1', 'true', 'yes'):
        return 'on'
    if value in ('off', '0', 'false', 'no'):
        return 'off'
    return 'auto'


def job_engine_requested() -> bool:
    """MAGE_JOB_ENGINE=on asks for the engine explicitly."""
    return job_engine_mode() == 'on'


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
        mode = job_engine_mode()
        if mode != 'off':
            self.queue = _engine_queue(queue_config, quiet=mode == 'auto')
            if self.queue is not None:
                return self.queue
        from mage_ai.orchestration.queue.process_queue import ProcessQueue
        self.queue = ProcessQueue(queue_config)
        return self.queue


def _engine_queue(queue_config: QueueConfig, quiet: bool = False):
    """
    The engine queue, or None when no engine can run jobs: with the reason printed when the
    engine was asked for, and quietly when it was only used because it is there.
    """
    from mage_ai.orchestration.queue.engine_queue import (
        EngineQueue,
        engine_supports_jobs,
    )
    from mage_ai.pipeline_services.engine import (
        EngineNotFound,
        engine_path,
        installed_engine_path,
    )

    try:
        # Without MAGE_JOB_ENGINE, only an installed engine: a source checkout's cargo build
        # can be stale, or a debug build.
        engine = installed_engine_path() if quiet else engine_path()
    except EngineNotFound as error:
        if not quiet:
            print(f'[QueueFactory] MAGE_JOB_ENGINE is on, but {error} Jobs run in processes.')
        return None
    if engine is None:
        return None
    if not engine_supports_jobs(engine):
        if not quiet:
            print(f'[QueueFactory] MAGE_JOB_ENGINE is on, but {engine} has no `jobs` command '
                  '(upgrade mageml-engine). Jobs run in processes.')
        return None
    if quiet:
        print(f'[QueueFactory] Jobs run in warm workers of {engine} '
              '(MAGE_JOB_ENGINE=off runs each in a process).')
    return EngineQueue(queue_config, engine)
