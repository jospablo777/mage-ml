from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.column_atlas.errors import AtlasError
from mage_ai.orchestration.db import safe_db_query
from mage_ai.settings.repo import get_repo_path


class ColumnAtlasQueryResource(GenericResource):
    """
    Queries over a stored block output for ColumnAtlas: metadata, row counts, row windows
    and column summaries. POST /api/column_atlas_queries with a source (pipeline, block,
    output variable, optional block run) and an action. The policy checks the user's access
    to the pipeline; the source is resolved again on every request.
    """

    @classmethod
    @safe_db_query
    async def create(cls, payload: Dict, user, **kwargs) -> 'ColumnAtlasQueryResource':
        from mage_ai.column_atlas.service import get_service
        from mage_ai.column_atlas.sources import parse_source, resolve, source_dict

        try:
            service = get_service()
            source = parse_source(payload.get('source'))
            resolved = resolve(source, get_repo_path(user=user))
            result = await service.query(payload.get('action'), source, resolved, payload)
        except AtlasError as error:
            raise ApiError(dict(
                code=error.status,
                message=str(error),
                type='column_atlas_error',
                errors=[],
            ))
        return cls(dict(action=payload.get('action'), result=result, source=source_dict(source)),
                   user, **kwargs)
