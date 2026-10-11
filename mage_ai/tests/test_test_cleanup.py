import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mage_ai.settings.repo import get_repo_path, set_repo_path
from mage_ai.tests.base_test import TestCase, remove_test_directory


class TestDirectoryCleanup(unittest.TestCase):
    def test_setup_allocates_a_temporary_project(self):
        previous_repo_path = get_repo_path()

        class ExampleTestCase(TestCase):
            pass

        try:
            ExampleTestCase.setUpClass()
            project = Path(ExampleTestCase.repo_path)
            self.assertEqual(project.name, 'test')
            self.assertNotIn(Path(__file__).resolve().parents[2], project.resolve().parents)
            (project / '.git').mkdir()
            ExampleTestCase.tearDownClass()
            self.assertFalse(project.exists())
        finally:
            set_repo_path(previous_repo_path)

    def test_cleanup_uses_the_directory_allocated_during_setup(self):
        previous_repo_path = get_repo_path()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository = root / 'repository'
            repository.mkdir()
            marker = repository / 'keep.txt'
            marker.write_text('repository data')
            test_project = root / 'test'
            test_project.mkdir()

            class ExampleTestCase(TestCase):
                _test_directory = str(test_project)
                repo_path = str(repository)

            try:
                set_repo_path(str(repository))
                ExampleTestCase.tearDownClass()
            finally:
                set_repo_path(previous_repo_path)

            self.assertFalse(test_project.exists())
            self.assertEqual(marker.read_text(), 'repository data')

    def test_checkout_and_ancestors_are_rejected(self):
        checkout = Path(__file__).resolve().parents[2]
        for directory in [checkout, checkout.parent]:
            with self.subTest(directory=directory), patch('shutil.rmtree') as remove:
                with self.assertRaises(ValueError):
                    remove_test_directory(directory)
                remove.assert_not_called()

    def test_git_repositories_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / '.git').mkdir()
            with self.assertRaises(ValueError):
                remove_test_directory(directory)

    def test_symlink_targets_are_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / 'project'
            target.mkdir()
            marker = target / 'keep.txt'
            marker.write_text('project data')
            link = root / 'test'
            link.symlink_to(target, target_is_directory=True)

            with self.assertRaises(ValueError):
                remove_test_directory(link)

            self.assertEqual(marker.read_text(), 'project data')


class CollectionTest(unittest.TestCase):
    def test_importing_the_predicate_tests_writes_nothing_in_the_working_directory(self):
        """
        test_predicates built its tests when imported and created 701 data loader files in
        data_loaders/ of the working directory, the checkout in a test run.
        """
        with tempfile.TemporaryDirectory() as directory:
            subprocess.run(
                [
                    sys.executable, '-c',
                    'import mage_ai.tests.data_preparation.models.global_hooks.test_predicates',
                ],
                cwd=directory,
                # As pytest starts: the project path comes from the working directory.
                env={k: v for k, v in os.environ.items() if k != 'MAGE_REPO_PATH'},
                check=True,
                capture_output=True, timeout=300,
            )
            self.assertFalse(Path(directory, 'data_loaders').exists())
