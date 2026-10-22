"""
A block output's CSV download (GET /api/pipelines/<p>/block_outputs/<b>/downloads): a
signed-in user with no access to the pipeline downloaded it, and refusals were 500s.
"""
import json
from unittest.mock import patch

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from mage_ai.authentication.oauth2 import generate_access_token
from mage_ai.orchestration.db.models.oauth import Oauth2Application, Role
from mage_ai.server.api.downloads import ApiDownloadHandler
from mage_ai.tests.base_test import DBTestCase
from mage_ai.tests.factory import create_pipeline_with_blocks, create_user


class BlockOutputDownloadTest(AsyncHTTPTestCase, DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)
        AsyncHTTPTestCase.setUp(self)
        for target in ('mage_ai.server.api.downloads.REQUIRE_USER_AUTHENTICATION',
                       'mage_ai.api.utils.REQUIRE_USER_AUTHENTICATION'):
            patcher = patch(target, True)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = Oauth2Application.create(client_id='downloads', name='downloads')
        self.pipeline = create_pipeline_with_blocks(self.faker.unique.name(), self.repo_path)
        self.block = list(self.pipeline.blocks_by_uuid)[0]

    def get_app(self):
        return Application([(
            r'/api/pipelines/(?P<pipeline_uuid>\w+)/block_outputs/'
            r'(?P<block_uuid>[\w\-\%2f\.(/.*)?]+)/downloads',
            ApiDownloadHandler,
        )])

    def download(self, user=None):
        query = ''
        if user is not None:
            token = generate_access_token(user, self.client)
            query = f'?api_key=downloads&token={token.token}'
        return self.fetch(
            f'/api/pipelines/{self.pipeline.uuid}/block_outputs/{self.block}/downloads{query}',
        )

    def test_only_users_who_may_read_the_pipeline_download(self):
        Role.create_default_roles()
        nobody = create_user()
        viewer = create_user(roles_new=[Role.get_role('Viewer')])

        with patch('mage_ai.server.api.downloads.get_repo_path', return_value=self.repo_path):
            unsigned = self.download()
            refused = self.download(nobody)
            allowed = self.download(viewer)

        self.assertEqual(unsigned.code, 401)
        self.assertEqual(refused.code, 403)
        self.assertEqual(json.loads(refused.body)['error']['code'], 403)
        self.assertEqual(allowed.code, 200, allowed.body)
