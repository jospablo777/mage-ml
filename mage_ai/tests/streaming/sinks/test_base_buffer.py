import os
import tempfile
import unittest

from mage_ai.streaming.sinks.base import BaseSink


class BufferedSink(BaseSink):
    def init_client(self):
        pass

    def batch_write(self, messages):
        pass


class ReadBufferTest(unittest.TestCase):
    def test_a_line_that_does_not_read_is_skipped_not_the_rest(self):
        """Reading stopped at a bad line, and every message buffered after it was lost."""
        folder = tempfile.mkdtemp()
        path = os.path.join(folder, 'buffer')
        with open(path, 'w') as file:
            file.write('{"id": 1}\n{"id": \n\n{"id": 3}\n{"id": 4')

        sink = BufferedSink({}, buffer_path=path)

        self.assertEqual(sink.buffer, [{'id': 1}, {'id': 3}])

    def test_no_buffer_file(self):
        sink = BufferedSink({}, buffer_path='/nonexistent/buffer')
        self.assertEqual(sink.buffer, [])
