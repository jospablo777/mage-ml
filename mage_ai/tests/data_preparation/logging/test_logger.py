"""Log entries carry the real time as a UTC epoch, whatever the machine's time zone."""
import json
import logging
import os
import time
import unittest

from mage_ai.data_preparation.logging.logger import DictLogger


class CapturingHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


class LogTimestampTest(unittest.TestCase):
    def setUp(self):
        # A machine west of UTC, where a naive UTC time read as local was hours off.
        previous = os.environ.get('TZ')
        os.environ['TZ'] = 'America/Costa_Rica'
        time.tzset()

        def restore():
            if previous is None:
                os.environ.pop('TZ', None)
            else:
                os.environ['TZ'] = previous
            time.tzset()

        self.addCleanup(restore)

    def test_the_timestamp_is_now_in_utc(self):
        logger = logging.getLogger('mage_test_log_timestamp')
        handler = CapturingHandler()
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        self.addCleanup(logger.removeHandler, handler)

        DictLogger(logger).info('started')

        entry = json.loads(handler.messages[-1])
        self.assertAlmostEqual(entry['timestamp'], time.time(), delta=5)
