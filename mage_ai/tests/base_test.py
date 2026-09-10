import logging
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from faker import Faker

from mage_ai.data_preparation.repo_manager import init_project_uuid
from mage_ai.orchestration.db import TEST_DB, db_connection, engine
from mage_ai.orchestration.db.database_manager import database_manager
from mage_ai.settings.repo import set_repo_path


def remove_test_directory(directory):
    if Path(directory).is_symlink():
        raise ValueError(f'Refusing to remove a symlinked test directory: {directory}')
    directory = Path(directory).resolve()
    checkout = Path(__file__).resolve().parents[2]
    if directory == checkout or directory in checkout.parents or (directory / '.git').exists():
        raise ValueError(f'Refusing to remove a repository directory: {directory}')
    if directory.exists():
        shutil.rmtree(directory)


def drop_test_db():
    """Close pooled connections before removing the test database."""
    engine.dispose()
    if Path(TEST_DB).is_file():
        Path(TEST_DB).unlink()


def set_up_test_repo(test_case):
    test_case._test_directory = tempfile.mkdtemp(prefix='mage-tests-')
    test_case.repo_path = os.path.join(test_case._test_directory, 'test')
    Path(test_case.repo_path).mkdir()
    set_repo_path(test_case.repo_path)
    init_project_uuid()


class AsyncDBTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.faker = Faker()

    def tearDown(self):
        pass

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_up_test_repo(cls)
        database_manager.run_migrations(log_level=logging.ERROR)
        db_connection.start_session(force=True)

    @classmethod
    def tearDownClass(cls):
        db_connection.close_session()
        drop_test_db()
        remove_test_directory(cls._test_directory)
        super().tearDownClass()


class DBTestCase(unittest.TestCase):
    def setUp(self):
        self.faker = Faker()

    def tearDown(self):
        pass

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        set_up_test_repo(self)
        database_manager.run_migrations(log_level=logging.ERROR)
        db_connection.start_session(force=True)

    @classmethod
    def tearDownClass(self):
        db_connection.close_session()
        drop_test_db()
        remove_test_directory(self._test_directory)

        super().tearDownClass()


class TestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        set_up_test_repo(self)

    @classmethod
    def tearDownClass(self):
        remove_test_directory(self._test_directory)
        super().tearDownClass()
