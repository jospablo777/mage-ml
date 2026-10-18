import asyncio
from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


def _pipeline(kwargs) -> str:
    parent = kwargs.get('parent_model')
    if not (getattr(parent, 'uuid', None) and hasattr(parent, 'blocks_by_uuid')):
        raise ApiError(dict(code=400, message='A stream run belongs to a pipeline: '
                                              '/api/pipelines/<uuid>/stream_runs/current.'))
    return parent.uuid


def _username(user) -> str:
    return getattr(user, 'username', None)


class StreamRunResource(GenericResource):
    """
    A streaming pipeline on the local engine, from the editor: POST starts it (with
    `confirmed_sinks`, the sink blocks the user named), GET current reads its status, PUT
    current pauses or resumes it (`action`), DELETE current stops it.
    """

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'StreamRunResource':
        from mage_ai.streaming import dev_run

        pipeline = _pipeline(kwargs)
        found = await asyncio.to_thread(dev_run.status, get_repo_path(user=user), pipeline)
        return cls(found, user, **kwargs)

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'StreamRunResource':
        from mage_ai.pipeline_services.engine import EngineNotFound
        from mage_ai.pipeline_services.local_run import LocalRunError
        from mage_ai.streaming import dev_run

        pipeline = _pipeline(kwargs)
        try:
            found = await asyncio.to_thread(
                dev_run.start, get_repo_path(user=user), pipeline,
                payload.get('confirmed_sinks') or [], _username(user),
            )
        except (dev_run.DevRunError, EngineNotFound, LocalRunError) as error:
            raise ApiError(dict(code=400, message=str(error)))
        return cls(found, user, **kwargs)

    async def update(self, payload: Dict, **kwargs):
        from mage_ai.streaming import dev_run

        try:
            self.model = await asyncio.to_thread(
                dev_run.control, get_repo_path(user=self.current_user),
                self.model['pipeline_uuid'], payload.get('action'),
            )
        except dev_run.DevRunError as error:
            raise ApiError(dict(code=400, message=str(error)))

    async def delete(self, **kwargs):
        from mage_ai.streaming import dev_run

        self.model = await asyncio.to_thread(
            dev_run.stop, get_repo_path(user=self.current_user), self.model['pipeline_uuid'],
        )
