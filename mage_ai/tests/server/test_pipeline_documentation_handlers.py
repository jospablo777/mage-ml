"""The documentation image route serves images from the pipeline folder only, and keeps an
SVG opened directly from running scripts."""
from pathlib import Path
from unittest.mock import patch

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.server.api.pipeline_documentation import (
    PipelineDocumentationImageHandler,
    PipelineDocumentationRenderHandler,
)
from mage_ai.tests.base_test import DBTestCase

SVG = b'<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>'


class DocumentationImageTest(AsyncHTTPTestCase, DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)
        AsyncHTTPTestCase.setUp(self)
        self.pipeline = Pipeline.create('documented', repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)
        (Path(self.pipeline.dir_path) / 'flow.svg').write_bytes(SVG)
        patcher = patch(
            'mage_ai.server.api.pipeline_documentation.get_repo_path',
            return_value=self.repo_path,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        auth = patch(
            'mage_ai.server.api.pipeline_documentation.REQUIRE_USER_AUTHENTICATION', False,
        )
        auth.start()
        self.addCleanup(auth.stop)
        middleware = patch('mage_ai.api.middleware.REQUIRE_USER_AUTHENTICATION', False)
        middleware.start()
        self.addCleanup(middleware.stop)

    def get_app(self):
        return Application([
            (
                r'/api/pipeline_documentations/(?P<pipeline_uuid>\w+)/images/(?P<path>.+)',
                PipelineDocumentationImageHandler,
            ),
            (
                r'/api/pipeline_documentations/(?P<pipeline_uuid>\w+)/rendered',
                PipelineDocumentationRenderHandler,
            ),
        ])

    def test_an_image_is_served_with_a_policy_that_blocks_scripts(self):
        response = self.fetch('/api/pipeline_documentations/documented/images/flow.svg')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.headers['Content-Type'], 'image/svg+xml')
        self.assertIn('sandbox', response.headers['Content-Security-Policy'])
        self.assertEqual(response.headers['X-Content-Type-Options'], 'nosniff')
        self.assertEqual(response.body, SVG)

    def test_an_uploaded_html_file_is_served_in_a_sandbox(self):
        from mage_ai.data_preparation.models.pipelines import documentation

        documentation.save(self.pipeline, '<h1>Sales</h1><script>alert(1)</script>', 'html')
        response = self.fetch('/api/pipeline_documentations/documented/rendered')
        self.assertEqual(response.code, 200)
        self.assertIn(b'<h1>Sales</h1>', response.body)
        self.assertTrue(response.headers['Content-Security-Policy'].startswith('sandbox'))

    def test_files_that_are_not_images_are_not_served(self):
        for path in ('metadata.yaml', '..%2F..%2Fmetadata.yaml', 'missing.png'):
            with self.subTest(path=path):
                response = self.fetch(f'/api/pipeline_documentations/documented/images/{path}')
                self.assertIn(response.code, (400, 404))
        self.assertEqual(
            self.fetch('/api/pipeline_documentations/nope/images/flow.svg').code, 404,
        )

    def test_images_need_a_signed_in_user_when_authentication_is_on(self):
        with patch('mage_ai.server.api.pipeline_documentation.REQUIRE_USER_AUTHENTICATION', True), \
                patch('mage_ai.api.middleware.REQUIRE_USER_AUTHENTICATION', True):
            response = self.fetch('/api/pipeline_documentations/documented/images/flow.svg')
        self.assertEqual(response.code, 401)
        self.assertNotEqual(response.body, SVG)

    def test_a_user_without_access_to_the_pipeline_gets_nothing(self):
        """Signed in was enough: a user with no role read any pipeline's documentation."""
        from mage_ai.api.middleware import OAuthMiddleware
        from mage_ai.tests.factory import create_user

        nobody = create_user()

        def prepare(handler):
            handler.request.current_user = nobody
            handler.request.error = None

        with patch('mage_ai.server.api.pipeline_documentation.REQUIRE_USER_AUTHENTICATION',
                   True), \
                patch('mage_ai.api.utils.REQUIRE_USER_AUTHENTICATION', True), \
                patch.object(OAuthMiddleware, 'prepare', prepare):
            response = self.fetch('/api/pipeline_documentations/documented/images/flow.svg')
        self.assertEqual(response.code, 403)
        self.assertNotEqual(response.body, SVG)
