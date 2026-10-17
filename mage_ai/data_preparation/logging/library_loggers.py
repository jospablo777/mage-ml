"""
Records of libraries' loggers in the logs of the block that logs them.

A block's log has the block logger's records and its prints. Logs of libraries, such as
urllib3 at DEBUG, did not reach it, also when a block attached the block logger's handlers
to them (upstream #5766). The project's logging_config lists them:

    logging_config:
      loggers:
        urllib3: DEBUG
        botocore: INFO

and while a block runs, their records go to its log.
"""
import logging
import threading
from contextlib import contextmanager
from typing import Dict, Iterator, Optional

_forwarding = threading.local()


class _BlockLogHandler(logging.Handler):
    def __init__(self, target, tags: Dict):
        super().__init__()
        self.target = target
        self.tags = tags

    def emit(self, record: logging.LogRecord) -> None:
        # The block logger writes through a logger of its own: a listed root logger would
        # bring its records back here.
        if getattr(_forwarding, 'active', False):
            return
        _forwarding.active = True
        # A listed logger's level is the one wanted in the block log: urllib3 at DEBUG
        # was dropped by the block logger's INFO.
        block_logger = getattr(self.target, 'logger', None)
        lowered = []
        if isinstance(block_logger, logging.Logger):
            for item in [block_logger] + list(block_logger.handlers):
                if record.levelno < item.level:
                    lowered.append((item, item.level))
                    item.setLevel(record.levelno)
        try:
            method = getattr(self.target, record.levelname.lower(), None) or self.target.info
            method(f'[{record.name}] {record.getMessage()}', **self.tags)
        except Exception:
            self.handleError(record)
        finally:
            for item, level in lowered:
                item.setLevel(level)
            _forwarding.active = False


@contextmanager
def forward_library_logs(
    loggers: Optional[Dict[str, str]],
    target,
    tags: Optional[Dict] = None,
) -> Iterator[None]:
    """Sends the records of the listed loggers, at their levels, to target while open."""
    attached = []
    for name, level in (loggers or {}).items():
        logger = logging.getLogger(name)
        handler = _BlockLogHandler(target, tags or {})
        previous = logger.level
        logger.setLevel(str(level).upper())
        logger.addHandler(handler)
        attached.append((logger, handler, previous))
    try:
        yield
    finally:
        for logger, handler, previous in attached:
            logger.removeHandler(handler)
            logger.setLevel(previous)
