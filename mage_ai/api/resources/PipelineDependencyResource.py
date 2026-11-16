from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.orchestration import pipeline_dependencies
from mage_ai.orchestration.db import safe_db_query
from mage_ai.settings.repo import get_repo_path


class PipelineDependencyResource(GenericResource):
    """
    GET /api/pipeline_dependencies/project: the project's pipelines that start or feed other
    pipelines, as nodes and edges (orchestration/pipeline_dependencies.py).
    """

    @classmethod
    @safe_db_query
    async def member(cls, pk, user, **kwargs) -> 'PipelineDependencyResource':
        graph = pipeline_dependencies.build(get_repo_path(user=user))
        return cls(dict(graph, id='project'), user, **kwargs)
