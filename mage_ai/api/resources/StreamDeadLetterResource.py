import asyncio
from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


def _pipeline(kwargs) -> str:
    parent = kwargs.get('parent_model')
    if not (getattr(parent, 'uuid', None) and hasattr(parent, 'blocks_by_uuid')):
        raise ApiError(dict(code=400, message='Dead letters belong to a pipeline: '
                                              '/api/pipelines/<uuid>/stream_dead_letters/current.'))
    return parent.uuid


class StreamDeadLetterResource(GenericResource):
    """
    A stream's dead letters: GET current reads the newest (`limit`), POST opens them as a
    sample (`action: sample`, `name`) or replays them to the source (`action: replay`,
    `confirm_topic`, optional `positions`).
    """

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'StreamDeadLetterResource':
        from mage_ai.streaming import dead_letters

        pipeline = _pipeline(kwargs)
        query = kwargs.get('query') or {}
        limit = int((query.get('limit') or [200])[0])
        try:
            found = await asyncio.to_thread(
                dead_letters.read_dead_letters, get_repo_path(user=user), pipeline, limit)
        except dead_letters.DeadLetterError as error:
            raise ApiError(dict(code=400, message=str(error)))
        except Exception as error:
            raise ApiError(dict(code=400, message=f'Reading the dead letters failed: {error}'))
        return cls(dict(found, pipeline_uuid=pipeline), user, **kwargs)

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'StreamDeadLetterResource':
        from mage_ai.streaming import dead_letters

        pipeline = _pipeline(kwargs)
        repo_path = get_repo_path(user=user)
        action = payload.get('action')
        try:
            if action == 'sample':
                summary = await asyncio.to_thread(
                    dead_letters.open_as_sample, repo_path, pipeline, payload.get('name'))
                return cls(dict(pipeline_uuid=pipeline, sample=summary), user, **kwargs)
            if action == 'replay':
                result = await asyncio.to_thread(
                    dead_letters.replay, repo_path, pipeline, payload.get('confirm_topic'),
                    payload.get('positions'))
                return cls(dict(result, pipeline_uuid=pipeline), user, **kwargs)
        except dead_letters.DeadLetterError as error:
            raise ApiError(dict(code=400, message=str(error)))
        except Exception as error:
            raise ApiError(dict(code=400, message=f'{action} failed: {error}'))
        raise ApiError(dict(code=400, message='action is sample or replay.'))
