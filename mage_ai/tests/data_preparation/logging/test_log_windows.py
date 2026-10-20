"""
A run log read over its rotated segments, a window of lines at a time (I-218): no line
printed before a rotation is hidden, and none past 220 MB is deleted.
"""
import asyncio
import logging
import logging.handlers
import os
import tempfile
from pathlib import Path

from mage_ai.api.resources.LogResource import LogResource
from mage_ai.data_preparation.logging import log_windows
from mage_ai.data_preparation.logging.logger_manager import (
    LOG_FILE_SEGMENTS,
    pipeline_logs_dir,
)
from mage_ai.data_preparation.models.file import FileNotInProjectError
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.tests.base_test import DBTestCase, TestCase


def rotated_log(directory: str, lines: int, max_bytes: int) -> str:
    """A log written through the handler block runs use, rotated every max_bytes."""
    path = os.path.join(directory, 'block.log')
    logger = logging.getLogger(f'test_log_windows_{id(directory)}')
    handler = logging.handlers.RotatingFileHandler(
        path, backupCount=LOG_FILE_SEGMENTS, maxBytes=max_bytes,
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        for index in range(lines):
            logger.info(f'printed line {index}')
    finally:
        logger.removeHandler(handler)
        handler.close()
    return path


class LogWindowsTest(TestCase):
    def setUp(self):
        super().setUp()
        self.directory = tempfile.mkdtemp()

    def test_segments_are_read_oldest_first(self):
        path = rotated_log(self.directory, 2000, 4096)
        segments = log_windows.segment_paths(path)
        self.assertGreater(len(segments), 3)
        self.assertEqual(segments[-1], path)
        self.assertEqual(segments[-2], f'{path}.1')
        self.assertEqual(Path(segments[0]).read_text().splitlines()[0], 'printed line 0')

    def test_every_line_counts_and_the_newest_come_first(self):
        path = rotated_log(self.directory, 2000, 4096)
        item = log_windows.window(path, lines=100)
        self.assertEqual(item['total_lines'], 2000)
        self.assertEqual(item['first_line'], 1900)
        self.assertEqual(item['content'].splitlines()[-1], 'printed line 1999')
        self.assertEqual(item['size'], sum(
            os.path.getsize(p) for p in log_windows.segment_paths(path)
        ))

    def test_the_lines_before_a_window_come_back_in_order(self):
        path = rotated_log(self.directory, 2000, 4096)
        earliest = log_windows.window(path, before_line=150, lines=150)
        self.assertEqual(earliest['first_line'], 0)
        self.assertEqual(
            earliest['content'].splitlines(), [f'printed line {i}' for i in range(150)],
        )

    def test_search_reads_every_segment(self):
        path = rotated_log(self.directory, 2000, 4096)
        # Line 19 is in the oldest segment, line 1999 in the newest.
        found = log_windows.search(path, 'PRINTED LINE 19')
        lines = found['content'].splitlines()
        self.assertIn('printed line 19', lines)
        self.assertIn('printed line 1999', lines)
        self.assertEqual(found['matched_lines'][0], 19)
        self.assertIsNone(log_windows.search(path, 'absent'))

    def test_a_rotated_log_keeps_its_segments(self):
        # Ten segments were kept before: everything older was deleted.
        self.assertGreaterEqual(LOG_FILE_SEGMENTS, 10_000)


class EarlierLinesApiTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)

    def earlier(self, path, before_line):
        result = asyncio.run(LogResource.collection(
            {'log_path': [path], 'before_line': [str(before_line)], 'lines': ['50']}, {},
            None, parent_model=self.pipeline,
        ))
        return result[0].model['block_run_logs'][0]

    def test_a_log_of_the_pipeline_gives_its_earlier_lines(self):
        directory = os.path.join(pipeline_logs_dir(self.pipeline.uuid, self.repo_path), '1', 'p')
        os.makedirs(directory, exist_ok=True)
        path = rotated_log(directory, 500, 2048)
        item = self.earlier(path, 100)
        self.assertEqual(item['first_line'], 50)
        self.assertEqual(item['content'].splitlines()[0], 'printed line 50')

    def test_a_path_outside_the_pipeline_logs_is_refused(self):
        outside = rotated_log(tempfile.mkdtemp(), 10, 2048)
        with self.assertRaises(FileNotInProjectError):
            self.earlier(outside, 5)
        traversal = os.path.join(
            pipeline_logs_dir(self.pipeline.uuid, self.repo_path), '..', '..', 'other', 'x.log',
        )
        with self.assertRaises(FileNotInProjectError):
            self.earlier(traversal, 5)
