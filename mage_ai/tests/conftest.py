import os
import re

import pytest

os.environ['ENV'] = 'test_mage'

# What a test run itself keeps in the working directory: the test database (test.db, or
# test<worker>.db with xdist) and pytest's and coverage's own files.
_RUN_FILES = re.compile(r'^(test(gw\d+)?\.db(-journal|-wal|-shm)?|\.pytest_cache|\.coverage.*)$')
_entries_at_start = set()


def pytest_sessionstart(session):
    _entries_at_start.update(os.listdir())


def pytest_sessionfinish(session, exitstatus):
    """
    Fails a run whose tests left files in the working directory, the checkout in a test
    run: block files, MLflow stores and logs were left there.
    """
    report = session.config.getoption('xmlpath', None)
    left = sorted(
        name for name in set(os.listdir()) - _entries_at_start
        if not _RUN_FILES.match(name)
        and not (report and os.path.abspath(name) == os.path.abspath(report))
    )
    if left:
        reporter = session.config.pluginmanager.get_plugin('terminalreporter')
        if reporter is not None:
            reporter.ensure_newline()
            reporter.write_line(
                f'Tests left files in the working directory {os.getcwd()}: {", ".join(left)}',
                red=True,
            )
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
