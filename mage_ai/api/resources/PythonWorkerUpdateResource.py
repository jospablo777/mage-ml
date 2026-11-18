from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


class PythonWorkerUpdateResource(GenericResource):
    """
    Updates of a Python worker's packages, previewed before they are applied
    (data_preparation/python_workers.py): POST /api/python_worker_updates with {worker}
    (every package to the newest release its ranges allow), {worker, packages} or {worker,
    fix_vulnerabilities: true} locks it in a copy and answers the packages that would move
    and an id; PUT /api/python_worker_updates/<id> applies that lock and prepares the worker.
    """

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'PythonWorkerUpdateResource':
        import asyncio

        from mage_ai.data_preparation import environments, python_workers

        worker = payload.get('worker')
        packages = payload.get('packages')
        if not isinstance(worker, str) or not worker:
            raise ApiError(dict(code=400, message='Name the Python worker to update.'))
        if packages is not None and (
            not isinstance(packages, list) or not all(isinstance(p, str) for p in packages)
        ):
            raise ApiError(dict(code=400, message='packages is a list of package names.'))
        try:
            result = await asyncio.to_thread(
                python_workers.preview, get_repo_path(user=user), worker,
                packages=packages, fix_vulnerabilities=bool(payload.get('fix_vulnerabilities')),
            )
        except environments.PipelineEnvironmentError as error:
            raise ApiError(dict(code=400, message=str(error)))
        return cls(result, user, **kwargs)

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'PythonWorkerUpdateResource':
        return cls(dict(id=pk), user, **kwargs)

    async def update(self, payload: Dict, **kwargs):
        from mage_ai.data_preparation import environments, python_workers

        try:
            result = python_workers.apply(get_repo_path(user=self.current_user),
                                          self.model['id'])
        except environments.PipelineEnvironmentError as error:
            raise ApiError(dict(code=400, message=str(error)))
        self.model = dict(result, id=self.model['id'])
