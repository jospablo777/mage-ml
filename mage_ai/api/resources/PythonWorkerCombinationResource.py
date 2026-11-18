from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


class PythonWorkerCombinationResource(GenericResource):
    """
    POST /api/python_worker_combinations with {workers: [a, b]}: whether those Python
    workers can run as one environment (environments.combine_check): same, changes,
    conflict, kept_apart or unchecked, with each worker's size and the bytes combining
    saves. Answers are kept by the workers' locks; a new question resolves with uv.
    """

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'PythonWorkerCombinationResource':
        import asyncio

        from mage_ai.data_preparation import environments

        workers = payload.get('workers')
        if not isinstance(workers, list) or not all(isinstance(w, str) for w in workers):
            raise ApiError(dict(code=400, message='Name the Python workers to combine.'))
        try:
            # uv resolves outside the server's event loop.
            result = await asyncio.to_thread(
                environments.combine_check, get_repo_path(user=user), workers,
            )
        except environments.PipelineEnvironmentError as error:
            raise ApiError(dict(code=400, message=str(error)))
        return cls(dict(result, id='+'.join(result['workers'])), user, **kwargs)
