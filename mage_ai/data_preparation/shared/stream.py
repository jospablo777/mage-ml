import logging
import re
import sys
import threading
from typing import Dict

# A line too long to hold until its end is logged in parts of this many characters.
MAX_LINE = 64 * 1024
LINE_END = re.compile(r'\r\n|\n|\r')


class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance, one log message
    per line: print('a', 'b') writes four times and logs 'a b' once. Blank lines are logged
    too. What follows the last line end waits for the next write or for flush(), which the
    code that redirects to this stream calls when the block ends.
    """
    def __init__(self, logger, log_level=logging.INFO, logging_tags: Dict = dict()):
        self.logger = logger
        self.log_level = log_level
        self.logging_tags = logging_tags or dict()
        self.linebuf = ''
        self.terminal = sys.stdout
        self._lock = threading.Lock()

    def __getattr__(self, attr):
        return getattr(self.terminal, attr)

    def write(self, buf):
        with self._lock:
            self.linebuf += buf
            *lines, self.linebuf = LINE_END.split(self.linebuf)
            while len(self.linebuf) > MAX_LINE:
                lines.append(self.linebuf[:MAX_LINE])
                self.linebuf = self.linebuf[MAX_LINE:]
        for line in lines:
            self.logger.log(self.log_level, line, **self.logging_tags)
        return len(buf)

    def flush(self):
        with self._lock:
            rest, self.linebuf = self.linebuf, ''
        if rest:
            self.logger.log(self.log_level, rest, **self.logging_tags)
