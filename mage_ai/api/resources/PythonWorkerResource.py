from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


class PythonWorkerResource(GenericResource):
    """
    The project's Python workers (python_workers/<name>/): GET /api/python_workers lists
    them with whether each is prepared; POST prepares one in the background; GET
    /api/python_workers/<name> gives it with its prepare's state and every line uv printed.
    """

    @classmethod
    async def collection(cls, query, meta, user, **kwargs):
        from mage_ai.data_preparation import environments

        try:
            workers = environments.describe_workers(get_repo_path(user=user))
        except environments.PipelineEnvironmentError as error:
            raise ApiError(dict(code=400, message=str(error)))
        return cls.build_result_set(workers, user, **kwargs)

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'PythonWorkerResource':
        from mage_ai.data_preparation import environments

        name = payload.get('name')
        if not isinstance(name, str) or not name.strip():
            raise ApiError(dict(code=400, message='Name the Python worker to prepare.'))
        try:
            job = environments.start_prepare(get_repo_path(user=user), name)
        except environments.PipelineEnvironmentError as error:
            raise ApiError(dict(code=404, message=str(error)))
        return cls(dict(name=job['name'], job=job), user, **kwargs)

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'PythonWorkerResource':
        from mage_ai.data_preparation import environments

        try:
            workers = environments.describe_workers(get_repo_path(user=user))
        except environments.PipelineEnvironmentError as error:
            raise ApiError(dict(code=400, message=str(error)))
        alias = environments.normalized_alias(pk)
        for worker in workers:
            if worker['name'] == alias:
                return cls(worker, user, **kwargs)
        raise ApiError(dict(
            code=404,
            message=f'There is no Python worker {pk}; workers: '
                    f'{", ".join(w["name"] for w in workers) or "none"}.',
        ))
