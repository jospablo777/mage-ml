from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.server.api.base import BaseHandler, SignedInMixin
from mage_ai.settings.repo import get_repo_path


class ApiPipelineBlockAnalysisHandler(SignedInMixin, BaseHandler):
    def get(self, pipeline_uuid, block_uuid):
        pipeline = Pipeline.get(
            pipeline_uuid,
            repo_path=get_repo_path(user=getattr(self.request, 'current_user', None)),
        )
        block = pipeline.get_block(block_uuid)
        if block is None:
            raise Exception(f'Block {block_uuid} does not exist in pipeline {pipeline_uuid}')
        analyses = block.get_analyses()
        self.write(dict(analyses=analyses))
