import os
import shutil
import tempfile
from unittest.mock import patch

from mage_ai.data_preparation.git import Git
from mage_ai.tests.api.endpoints.mixins import BaseAPIEndpointTest
from mage_ai.tests.shared.mixins import ProjectPlatformMixin


class GitTest(BaseAPIEndpointTest):
    def test_repo_path(self):
        self.assertEqual(Git().repo_path, os.getcwd())


class GitProjectPlatformTest(ProjectPlatformMixin):
    def test_repo_path(self):
        # Git() creates its relative path in the working directory: run where that is a
        # temporary folder, not the checkout.
        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder, True)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder)
        git_settings = dict(
            path='mage_custom_path',
        )
        with patch('mage_ai.data_preparation.git.project_platform_activated', lambda: True):
            with patch(
                'mage_ai.data_preparation.git.git_settings',
                lambda user=None, **kwargs: git_settings,
            ):
                self.assertEqual(Git().repo_path, 'mage_custom_path')
