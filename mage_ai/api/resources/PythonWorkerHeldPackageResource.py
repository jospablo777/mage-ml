from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


class PythonWorkerHeldPackageResource(GenericResource):
    """
    GET /api/python_worker_held_packages/<worker>: the packages the worker lists that uv locked
    below their newest release, each with why the newest does not fit; the last check of its
    lock unless ?refresh=1 (data_preparation/python_workers.py, held_back).
    """

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'PythonWorkerHeldPackageResource':
        import asyncio

        from mage_ai.data_preparation import environments, python_workers

        query = kwargs.get('query') or {}
        refresh = (query.get('refresh') or [False])[0] in (True, '1', 'true')
        try:
            report = await asyncio.to_thread(
                python_workers.held_back, get_repo_path(user=user), pk, refresh=refresh,
            )
        except environments.PipelineEnvironmentError as error:
            code = 404 if str(error).startswith('There is no Python worker') else 400
            raise ApiError(dict(code=code, message=str(error)))
        return cls(dict(report, id=report['name']), user, **kwargs)
