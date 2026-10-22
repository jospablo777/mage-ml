"""
The logs page's requests as a user who is not an owner: its text search and the lines
before those shown were refused, their query names missing from LogPolicy.
"""
import os
from unittest.mock import patch

from mage_ai.api.operations import constants
from mage_ai.data_preparation.logging.logger_manager import pipeline_logs_dir
from mage_ai.orchestration.db.models.oauth import Role
from mage_ai.tests.api.operations.test_base import BaseApiTestCase
from mage_ai.tests.factory import create_pipeline, create_user


@patch('mage_ai.api.policies.BasePolicy.REQUIRE_USER_AUTHENTICATION', 1)
class LogOperationTests(BaseApiTestCase):
    async def test_a_viewer_searches_and_reads_earlier_lines(self):
        Role.create_default_roles()
        viewer = create_user(roles_new=[Role.get_role('Viewer')])
        pipeline = create_pipeline(self.faker.unique.name(), self.repo_path)
        log = os.path.join(pipeline_logs_dir(pipeline.uuid, self.repo_path), '1', 'block.log')
        for query in [
            dict(text=['needle']),
            dict(log_path=[log], before_line=['10'], lines=['5']),
        ]:
            with self.subTest(query=list(query)):
                response = await self.build_operation(
                    action=constants.LIST,
                    resource='logs',
                    resource_parent='pipelines',
                    resource_parent_id=pipeline.uuid,
                    query=query,
                    user=viewer,
                ).execute()
                self.assertNotIn('error', response, response)
