import os
import re
import shutil
import tempfile

import pytest

os.environ['ENV'] = 'test_mage'

# With no tracking URI, MLflow writes sqlite:///mlflow.db in the working directory, the
# checkout in a test run. Tests that need MLflow point it at their own store and come back
# to this one, so a reset or a block that runs after its test ends writes here instead.
MLFLOW_FOLDER = tempfile.mkdtemp(prefix='mage-test-mlflow-')
os.environ.setdefault('MLFLOW_TRACKING_URI', f'sqlite:///{MLFLOW_FOLDER}/mlflow.db')

# What a test run itself keeps in the working directory: the test database (test.db, or
# test<worker>.db with xdist), pytest's and coverage's own files, and the folder view
# settings macOS Finder writes.
_RUN_FILES = re.compile(
    r'^(test(gw\d+)?\.db(-journal|-wal|-shm)?|\.pytest_cache|\.coverage.*|\.DS_Store)$'
)
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
    shutil.rmtree(MLFLOW_FOLDER, ignore_errors=True)
