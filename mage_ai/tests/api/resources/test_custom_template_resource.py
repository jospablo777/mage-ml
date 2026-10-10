"""Custom block templates through the API: a deleted template's files are removed."""
import os

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.custom_templates.constants import (
    DIRECTORY_FOR_BLOCK_TEMPLATES,
)
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user


class CustomTemplateResourceTest(AsyncDBTestCase):
    async def test_deleting_a_block_template_removes_it(self):
        owner = create_user(_owner=True)
        created = (await BaseOperation(
            action=OperationType.CREATE,
            payload=dict(custom_template=dict(
                object_type=DIRECTORY_FOR_BLOCK_TEMPLATES,
                template_uuid='to_delete',
                block_type='data_loader',
                language='python',
                name='To delete',
            )),
            resource='custom_templates',
            user=owner,
        ).execute())['custom_template']
        folder = os.path.join(self.repo_path, created['uuid'])
        self.assertTrue(os.path.isdir(folder))

        await BaseOperation(
            action=OperationType.DELETE,
            pk='to_delete',
            query={'object_type': [DIRECTORY_FOR_BLOCK_TEMPLATES]},
            resource='custom_templates',
            user=owner,
        ).execute()
        # The model's delete was referenced and not called, so nothing was removed.
        self.assertFalse(os.path.exists(folder))
