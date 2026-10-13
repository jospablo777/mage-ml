import asyncio
from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.api.resources.StreamSampleResource import _pipeline
from mage_ai.settings.repo import get_repo_path


class StreamSampleRunResource(GenericResource):
    """
    POST /api/pipelines/<uuid>/stream_sample_runs runs the blocks from the source down to
    `block_uuid` on a sample (`sample`, its name) or on `messages`, with the service engine's
    Python worker, and returns each block's output, time and printed lines, or the error and
    the first message that fails.
    """

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'StreamSampleRunResource':
        from mage_ai.streaming.sample_runner import SampleRunError, run_on_sample
        from mage_ai.streaming.samples import SampleError, read_sample

        repo_path = get_repo_path(user=user)
        pipeline = _pipeline(kwargs)
        block_uuid = payload.get('block_uuid')
        if not block_uuid:
            raise ApiError(dict(code=400, message='Name the block to run: block_uuid.'))
        try:
            if payload.get('sample'):
                messages = read_sample(repo_path, pipeline, payload['sample'])
            else:
                messages = payload.get('messages')
                if not isinstance(messages, list):
                    raise SampleError('Give a sample or a list of messages.')
            result = await asyncio.to_thread(
                run_on_sample, repo_path, pipeline, block_uuid, messages,
                payload.get('variables') or {},
            )
        except (SampleError, SampleRunError) as error:
            raise ApiError(dict(code=400, message=str(error)))
        result['sample'] = payload.get('sample')
        result['messages'] = messages
        return cls(result, user, **kwargs)
