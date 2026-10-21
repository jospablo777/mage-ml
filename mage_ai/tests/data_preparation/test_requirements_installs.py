import os
from datetime import datetime

import pytz
from freezegun import freeze_time

from mage_ai.data_preparation import requirements_installs
from mage_ai.tests.base_test import TestCase


class RequirementsInstallsTest(TestCase):
    """When MageML last installed requirements.txt, which restarts streaming runs."""

    def test_an_install_is_recorded_with_its_time(self):
        self.assertIsNone(requirements_installs.installed_at(self.repo_path))
        requirements = os.path.join(self.repo_path, 'requirements.txt')
        with open(requirements, 'w') as file:
            file.write('requests==2.31.0\n')
        self.addCleanup(os.remove, requirements)
        marker = requirements_installs._path(self.repo_path)
        self.addCleanup(lambda: os.remove(marker) if os.path.exists(marker) else None)

        with freeze_time('2026-10-21 12:00:00'):
            requirements_installs.record_install(self.repo_path, requirements)

        self.assertEqual(requirements_installs.installed_at(self.repo_path),
                         datetime(2026, 10, 21, 12, 0, tzinfo=pytz.UTC))

    def test_an_unreadable_record_is_no_install(self):
        marker = requirements_installs._path(self.repo_path)
        os.makedirs(os.path.dirname(marker), exist_ok=True)
        with open(marker, 'w') as file:
            file.write('{"installed_at": "not a time"}')
        self.addCleanup(os.remove, marker)
        self.assertIsNone(requirements_installs.installed_at(self.repo_path))


class GitInstallTest(TestCase):
    """A git pull or reset that installs requirements records the install when it succeeds."""

    def install(self, run):
        from types import SimpleNamespace
        from unittest.mock import patch

        from mage_ai.data_preparation.git import Git

        requirements = os.path.join(self.repo_path, 'requirements.txt')
        with open(requirements, 'w') as file:
            file.write('requests==2.31.0\n')
        self.addCleanup(os.remove, requirements)
        marker = requirements_installs._path(self.repo_path)
        self.addCleanup(lambda: os.remove(marker) if os.path.exists(marker) else None)
        git = Git.__new__(Git)
        git.repo = SimpleNamespace(working_dir=self.repo_path)
        git.project_repo_path = self.repo_path
        with patch('mage_ai.data_preparation.git.subprocess.run', side_effect=run):
            git._Git__pip_install()

    def test_a_successful_install_is_recorded(self):
        self.install(lambda *args, **kwargs: None)
        self.assertIsNotNone(requirements_installs.installed_at(self.repo_path))

    def test_a_failed_install_is_not(self):
        import subprocess

        def fail(*args, **kwargs):
            raise subprocess.CalledProcessError(1, 'uv')

        self.install(fail)
        self.assertIsNone(requirements_installs.installed_at(self.repo_path))
