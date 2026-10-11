"""
The files a pipeline's documentation shows, which are not JSON: its images, and its Quarto
document rendered to HTML. The documentation page loads them with the session cookie and
the API key in the query, as an <img> or <iframe> cannot send headers.
"""
import os

from mage_ai.data_preparation.models.pipelines import documentation
from mage_ai.server.api.base import BaseApiHandler
from mage_ai.settings import REQUIRE_USER_AUTHENTICATION
from mage_ai.settings.repo import get_repo_path, get_variables_dir

# Opened directly in a tab, an SVG or the rendered document must not run scripts on
# MageML's origin: no scripts, no requests, styles and embedded images only.
DOCUMENT_POLICY = (
    "sandbox; default-src 'none'; img-src data: 'self'; style-src 'unsafe-inline' data:; "
    "font-src data:"
)


class _DocumentationFileHandler(BaseApiHandler):
    def _pipeline(self, pipeline_uuid: str):
        from mage_ai.data_preparation.models.pipeline import Pipeline

        if REQUIRE_USER_AUTHENTICATION and (
            getattr(self.request, 'error', None) or not getattr(self.request, 'current_user', None)
        ):
            self.set_status(401)
            self.finish()
            return None
        repo_path = get_repo_path(user=getattr(self.request, 'current_user', None))
        if not Pipeline.get(pipeline_uuid, repo_path=repo_path, check_if_exists=True):
            self.set_status(404)
            self.finish()
            return None
        return Pipeline.get(pipeline_uuid, repo_path=repo_path)

    def _refuse(self, status: int, message: str):
        self.set_status(status)
        self.set_header('Content-Type', 'text/plain; charset=utf-8')
        self.finish(message)


class PipelineDocumentationImageHandler(_DocumentationFileHandler):
    def get(self, pipeline_uuid: str, path: str):
        pipeline = self._pipeline(pipeline_uuid)
        if pipeline is None:
            return
        try:
            image = documentation.image(pipeline, path)
        except documentation.DocumentationError as error:
            return self._refuse(404, str(error))
        self.set_header('Content-Type', documentation.IMAGE_TYPES[image.suffix.lower()])
        self.set_header('Content-Security-Policy', DOCUMENT_POLICY)
        self.set_header('X-Content-Type-Options', 'nosniff')
        self.set_header('Cache-Control', 'private, max-age=60')
        self.finish(image.read_bytes())


class PipelineDocumentationRenderHandler(_DocumentationFileHandler):
    def get(self, pipeline_uuid: str):
        pipeline = self._pipeline(pipeline_uuid)
        if pipeline is None:
            return
        cache_dir = os.path.join(
            get_variables_dir(repo_path=pipeline.repo_path), 'pipeline_documentation',
            pipeline.uuid,
        )
        try:
            html = documentation.render_quarto(pipeline, cache_dir)
        except documentation.DocumentationError as error:
            return self._refuse(422, str(error))
        self.set_header('Content-Type', 'text/html; charset=utf-8')
        self.set_header('Content-Security-Policy', DOCUMENT_POLICY)
        self.set_header('X-Content-Type-Options', 'nosniff')
        self.set_header('Cache-Control', 'private, no-store')
        self.finish(html)
