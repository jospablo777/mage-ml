"""Block names place files inside the project, whatever the name."""
import os

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.block.utils import clean_name
from mage_ai.tests.base_test import DBTestCase


class BlockNameTest(DBTestCase):
    def test_names_stay_relative_to_the_block_folder(self):
        self.assertEqual(clean_name('/tmp/abs_block'), 'tmp/abs_block')
        self.assertEqual(clean_name('../../up'), '__/__/up')
        self.assertEqual(clean_name('folder//my block/'), 'folder/my_block')

    def test_an_absolute_name_creates_the_block_in_the_project(self):
        outside = '/tmp/mage_block_name_test_abs.py'
        if os.path.exists(outside):
            os.remove(outside)
        block = Block.create('/tmp/mage_block_name_test_abs', 'data_loader', self.repo_path)
        self.addCleanup(lambda: os.path.exists(block.file_path) and os.remove(block.file_path))

        self.assertFalse(os.path.exists(outside))
        self.assertTrue(
            os.path.realpath(block.file_path).startswith(os.path.realpath(self.repo_path)),
            block.file_path,
        )
