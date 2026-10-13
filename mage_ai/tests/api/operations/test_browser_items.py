"""
The browser_items API reads, writes, moves and deletes files only in the project. It took any
path, so a viewer could read any file the server can read and an editor write any file.
"""
import os
import shutil
import tempfile
import urllib.parse
from pathlib import Path
from unittest.mock import patch

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.errors import FileNotInProjectError
from mage_ai.data_preparation.models.file import ensure_file_is_in_project
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user


class BrowserItemsStayInTheProjectTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        self.outside = tempfile.mkdtemp(prefix='mage-outside-')
        self.addCleanup(shutil.rmtree, self.outside, True)
        self.secret = Path(self.outside) / 'secret.txt'
        self.secret.write_text('secret')
        # The tests share the project folder, so each one has its own.
        self.inside = Path(tempfile.mkdtemp(dir=self.repo_path)) / 'readme.txt'
        self.addCleanup(shutil.rmtree, self.inside.parent, True)
        self.inside.write_text('inside')
        self.owner = create_user(_owner=True)

    async def operation(self, action, pk=None, payload=None, query=None, user=None):
        return await BaseOperation(
            action=action,
            pk=urllib.parse.quote(str(pk), safe='') if pk is not None else None,
            payload=payload,
            query=query,
            resource='browser_items',
            user=user or self.owner,
        ).execute()

    def assertRefused(self, response):
        self.assertIn('error', response, response)
        # The operation rewords a 403 for a user without roles, as this test's user is.
        self.assertEqual(response['error']['code'], 403, response)

    async def test_a_file_in_the_project_is_read(self):
        response = await self.operation(OperationType.DETAIL, pk=self.inside)

        self.assertEqual(response['browser_item']['content'], 'inside')

    async def test_a_file_outside_the_project_is_not_read(self):
        # The check is in the resource, so it holds for every role; the owner has the most.
        response = await self.operation(OperationType.DETAIL, pk=self.secret)

        self.assertRefused(response)
        self.assertNotIn('secret', str(response))

    async def test_a_symlink_out_of_the_project_is_not_followed(self):
        link = self.inside.parent / 'link.txt'
        link.symlink_to(self.secret)

        self.assertRefused(await self.operation(OperationType.DETAIL, pk=link))

    async def test_a_listing_outside_the_project_is_refused(self):
        self.assertRefused(await self.operation(
            OperationType.LIST, query=dict(directory=[self.outside]),
        ))
        self.assertRefused(await self.operation(
            OperationType.LIST, query=dict(paths=[self.outside]),
        ))
        traversal = os.path.join(self.repo_path, '..', '..')
        self.assertRefused(await self.operation(
            OperationType.LIST, query=dict(directory=[traversal]),
        ))

    async def test_no_file_is_created_outside_the_project(self):
        target = Path(self.outside) / 'created.txt'

        response = await self.operation(
            OperationType.CREATE,
            payload=dict(browser_item=dict(path=str(target), content='x')),
        )

        self.assertRefused(response)
        self.assertFalse(target.exists())

    async def test_no_file_is_moved_or_renamed_out_of_the_project(self):
        target = Path(self.outside) / 'moved.txt'

        self.assertRefused(await self.operation(
            OperationType.UPDATE, pk=self.inside,
            payload=dict(browser_item=dict(path=str(target))),
        ))
        self.assertRefused(await self.operation(
            OperationType.UPDATE, pk=self.inside,
            payload=dict(browser_item=dict(name=os.path.join('..', '..', '..', 'x.txt'))),
        ))
        self.assertTrue(self.inside.exists())
        self.assertFalse(target.exists())

    async def test_a_file_outside_the_project_is_not_deleted(self):
        self.assertRefused(await self.operation(OperationType.DELETE, pk=self.secret))
        self.assertTrue(self.secret.exists())

    async def test_a_rename_keeps_the_file_in_its_folder(self):
        """
        A new path in the same folder renames the file; the new name was passed as a path,
        so the file moved to the server's working folder.
        """
        response = await self.operation(
            OperationType.UPDATE, pk=self.inside,
            payload=dict(browser_item=dict(path=str(self.inside.parent / 'renamed.txt'))),
        )

        self.assertNotIn('error', response, response)
        self.assertTrue((self.inside.parent / 'renamed.txt').exists())
        self.assertFalse(self.inside.exists())
        self.assertFalse(Path('renamed.txt').exists())

    async def test_a_file_with_saved_versions_can_be_renamed(self):
        """A debugging raise in File.rename_async failed the rename of such a file."""
        from mage_ai.data_preparation.models.file import File

        versions = Path(File.file_path_versions_dir(
            self.repo_path, str(self.inside.parent), self.inside.name,
        ))
        versions.mkdir(parents=True)
        (versions / '1').write_text('old')
        target = self.inside.parent / 'renamed.txt'

        response = await self.operation(
            OperationType.UPDATE, pk=self.inside,
            payload=dict(browser_item=dict(path=str(target))),
        )

        self.assertNotIn('error', response, response)
        self.assertTrue(target.exists())
        moved = Path(File.file_path_versions_dir(
            self.repo_path, str(target.parent), target.name,
        ))
        self.assertEqual((moved / '1').read_text(), 'old')

    def test_the_project_check_also_runs_with_the_project_platform(self):
        """With the project platform on, ensure_file_is_in_project let any path through."""
        with patch(
            'mage_ai.data_preparation.models.file.project_platform_activated',
            lambda: True,
        ):
            ensure_file_is_in_project(str(self.inside))
            with self.assertRaises(FileNotInProjectError):
                ensure_file_is_in_project(str(self.secret))
