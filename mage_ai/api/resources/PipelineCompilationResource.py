from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


def _pipeline(pipeline_uuid, user):
    from mage_ai.data_preparation.models.pipeline import Pipeline

    repo_path = get_repo_path(user=user)
    if not isinstance(pipeline_uuid, str) or not Pipeline.get(
        pipeline_uuid, repo_path=repo_path, check_if_exists=True,
    ):
        raise ApiError(dict(code=404, message='The pipeline does not exist.'))
    return Pipeline.get(pipeline_uuid, repo_path=repo_path)


class PipelineCompilationResource(GenericResource):
    """
    Compile a pipeline (orchestration/compilation.py): POST /api/pipeline_compilations
    starts compiling what its production runs need; GET
    /api/pipeline_compilations/<pipeline uuid> returns the progress of the latest
    compilation, or what compiling would build when none ran yet.
    """

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'PipelineCompilationResource':
        from mage_ai.orchestration import compilation

        pipeline = _pipeline(payload.get('pipeline_uuid'), user)
        started = compilation.start(pipeline)
        return cls(dict(started.to_dict(), last_compiled=compilation.last_compiled(pipeline)),
                   user, **kwargs)

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'PipelineCompilationResource':
        from mage_ai.orchestration import compilation

        pipeline = _pipeline(pk, user)
        return cls(compilation.status(pipeline), user, **kwargs)
