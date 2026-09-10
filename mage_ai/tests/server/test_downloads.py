import os
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import jwt
from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from mage_ai.server.api.downloads import ApiResourceDownloadHandler
from mage_ai.settings import JWT_DOWNLOAD_SECRET


class ResourceDownloadTest(AsyncHTTPTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = os.path.join(self.directory.name, 'project')
        os.mkdir(self.root)
        self.repo_patch = patch(
            'mage_ai.server.api.downloads.get_repo_path', return_value=self.root,
        )
        self.repo_patch.start()
        self.addCleanup(self.repo_patch.stop)
        super().setUp()

    def get_app(self):
        return Application([(r'/download/(.*)', ApiResourceDownloadHandler)])

    def download(self, path, **kwargs):
        payload = {
            'file_name': os.path.basename(path),
            'file_list': [path],
            'ignore_folder_structure': False,
            'exp': datetime.now(timezone.utc) + timedelta(minutes=1),
        }
        payload.update(kwargs)
        token = jwt.encode(payload, JWT_DOWNLOAD_SECRET, algorithm='HS256')
        return self.fetch(f'/download/{token}')

    def test_binary_download(self):
        path = os.path.join(self.root, 'output.parquet')
        contents = b'PAR1\x00\xff\x80\r\nPAR1'
        with open(path, 'wb') as output:
            output.write(contents)

        response = self.download(path)

        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, contents)

    def test_missing_file_returns_client_error(self):
        response = self.download(os.path.join(self.root, 'missing.py'))

        self.assertEqual(response.code, 400)

    def test_symlink_outside_project_is_rejected(self):
        target = os.path.join(self.directory.name, 'private.txt')
        with open(target, 'w') as output:
            output.write('private data')
        link = os.path.join(self.root, 'linked.txt')
        os.symlink(target, link)

        response = self.download(link)

        self.assertEqual(response.code, 400)
        self.assertNotIn(b'private data', response.body)

    def test_future_token_is_rejected(self):
        response = self.download(
            os.path.join(self.root, 'missing.py'),
            nbf=datetime.now(timezone.utc) + timedelta(hours=1),
        )

        self.assertEqual(response.code, 400)
        self.assertIn(b'invalid', response.body)

    def test_expired_token_is_rejected(self):
        response = self.download(
            os.path.join(self.root, 'missing.py'),
            exp=datetime.now(timezone.utc) - timedelta(minutes=1),
        )

        self.assertEqual(response.code, 400)
        self.assertIn(b'expired', response.body)
