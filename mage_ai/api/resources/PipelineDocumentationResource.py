from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.data_preparation.models.pipelines import documentation
from mage_ai.settings.repo import get_repo_path


def _pipeline(pipeline_uuid, user):
    from mage_ai.data_preparation.models.pipeline import Pipeline

    repo_path = get_repo_path(user=user)
    if not isinstance(pipeline_uuid, str) or not Pipeline.get(
        pipeline_uuid, repo_path=repo_path, check_if_exists=True,
    ):
        raise ApiError(dict(code=404, message='The pipeline does not exist.'))
    return Pipeline.get(pipeline_uuid, repo_path=repo_path)


def _model(pipeline) -> Dict:
    try:
        doc = documentation.read(pipeline)
    except documentation.DocumentationError as error:
        raise ApiError(dict(code=400, message=str(error)))
    return dict(
        doc.to_dict(),
        exists=doc.format is not None,
        id=pipeline.uuid,
        pipeline_uuid=pipeline.uuid,
        quarto_available=documentation.quarto_command() is not None,
        template=documentation.template(pipeline),
    )


class PipelineDocumentationResource(GenericResource):
    """
    A pipeline's README (data_preparation/models/pipelines/documentation.py):
    GET /api/pipeline_documentations/<pipeline uuid> reads it, with a template to start one;
    PUT saves it as Markdown or Quarto.
    """

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'PipelineDocumentationResource':
        return cls(_model(_pipeline(pk, user)), user, **kwargs)

    async def update(self, payload: Dict, **kwargs) -> 'PipelineDocumentationResource':
        pipeline = _pipeline(self.model['pipeline_uuid'], self.current_user)
        source = payload.get('source')
        if not isinstance(source, str):
            raise ApiError(dict(code=400, message='source must be the document text.'))
        try:
            documentation.save(pipeline, source, payload.get('format') or 'markdown')
        except documentation.DocumentationError as error:
            raise ApiError(dict(code=400, message=str(error)))
        self.model = _model(pipeline)
        return self
