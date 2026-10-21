import logging
from contextlib import redirect_stdout
from unittest import TestCase

from mage_ai.data_preparation.shared.stream import MAX_LINE, StreamToLogger


class Recorder:
    def __init__(self):
        self.messages = []

    def log(self, level, message, **tags):
        self.messages.append((level, message, tags))


class StreamToLoggerTest(TestCase):
    def stream(self):
        recorder = Recorder()
        return recorder, StreamToLogger(recorder, logging_tags=dict(block_uuid='load'))

    def test_a_print_with_several_values_is_one_message(self):
        recorder, stream = self.stream()
        with redirect_stdout(stream):
            print('loading for', '2026-10-21', 3)
        self.assertEqual(recorder.messages,
                         [(logging.INFO, 'loading for 2026-10-21 3', dict(block_uuid='load'))])

    def test_blank_lines_and_spaces_are_kept(self):
        recorder, stream = self.stream()
        with redirect_stdout(stream):
            print('  indented  ')
            print()
            print('a\nb')
        self.assertEqual([m[1] for m in recorder.messages], ['  indented  ', '', 'a', 'b'])

    def test_a_line_without_its_end_is_logged_on_flush(self):
        recorder, stream = self.stream()
        with redirect_stdout(stream):
            print('progress', end='')
        self.assertEqual(recorder.messages, [])
        stream.flush()
        self.assertEqual([m[1] for m in recorder.messages], ['progress'])
        stream.flush()
        self.assertEqual(len(recorder.messages), 1)

    def test_carriage_returns_end_lines_and_long_lines_come_in_parts(self):
        recorder, stream = self.stream()
        stream.write('10%\r20%\r\n')
        stream.write('x' * (MAX_LINE + 5))
        self.assertEqual([m[1] for m in recorder.messages[:2]], ['10%', '20%'])
        self.assertEqual(len(recorder.messages[2][1]), MAX_LINE)
        stream.flush()
        self.assertEqual(recorder.messages[3][1], 'xxxxx')

    def test_a_block_run_logs_its_last_line_without_a_newline(self):
        from mage_ai.data_preparation.models.block import Block

        recorder = Recorder()
        block = Block('printer', 'printer', 'data_loader')
        with block._redirect_streams(logger=recorder, logging_tags=dict(block_uuid='printer')):
            print('first', 'line')
            print('no newline', end='')
        self.assertEqual([m[1] for m in recorder.messages], ['first line', 'no newline'])
