import asyncio
from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


def _pipeline(kwargs) -> str:
    from mage_ai.data_preparation.models.pipeline import Pipeline

    parent = kwargs.get('parent_model')
    if not isinstance(parent, Pipeline):
        raise ApiError(dict(code=400, message='Samples belong to a pipeline: '
                                              '/api/pipelines/<uuid>/stream_samples.'))
    return parent.uuid


class StreamSampleResource(GenericResource):
    """
    A streaming pipeline's sample messages: GET lists them, POST captures one from the source
    (`capture`, with `count` and `start`) or saves `messages`, GET <name> reads one, PUT saves
    edited messages, DELETE removes it.
    """

    @classmethod
    async def collection(cls, query, meta, user, **kwargs):
        from mage_ai.streaming.samples import list_samples

        found = list_samples(get_repo_path(user=user), _pipeline(kwargs))
        return cls.build_result_set(found, user, **kwargs)

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'StreamSampleResource':
        from mage_ai.streaming.samples import SampleError, list_samples, read_sample

        repo_path = get_repo_path(user=user)
        pipeline = _pipeline(kwargs)
        try:
            messages = read_sample(repo_path, pipeline, pk)
        except SampleError as error:
            raise ApiError(dict(code=404, message=str(error)))
        summary = next(s for s in list_samples(repo_path, pipeline) if s['name'] == pk)
        return cls(dict(summary, messages=messages, pipeline_uuid=pipeline), user, **kwargs)

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'StreamSampleResource':
        from mage_ai.streaming import samples

        repo_path = get_repo_path(user=user)
        pipeline = _pipeline(kwargs)
        name = payload.get('name')
        try:
            if payload.get('capture'):
                summary = await asyncio.to_thread(
                    samples.capture_sample, repo_path, pipeline, name,
                    count=int(payload.get('count') or 100),
                    start=payload.get('start') or 'latest',
                )
            else:
                summary = samples.write_sample(
                    repo_path, pipeline, name, payload.get('messages') or [],
                )
            if payload.get('keep_out_of_git'):
                samples.ignore_in_git(repo_path, pipeline)
        except samples.SampleError as error:
            raise ApiError(dict(code=400, message=str(error)))
        except Exception as error:
            raise ApiError(dict(code=400, message=f'Capturing failed: {error}'))
        return cls(dict(summary, pipeline_uuid=pipeline), user, **kwargs)

    async def update(self, payload: Dict, **kwargs):
        from mage_ai.streaming.samples import SampleError, write_sample

        repo_path = get_repo_path(user=self.current_user)
        try:
            summary = write_sample(
                repo_path, self.model['pipeline_uuid'], self.model['name'],
                payload.get('messages') or [],
            )
        except SampleError as error:
            raise ApiError(dict(code=400, message=str(error)))
        self.model = dict(summary, messages=payload.get('messages') or [],
                          pipeline_uuid=self.model['pipeline_uuid'])

    async def delete(self, **kwargs):
        from mage_ai.streaming.samples import delete_sample

        delete_sample(get_repo_path(user=self.current_user), self.model['pipeline_uuid'],
                      self.model['name'])
