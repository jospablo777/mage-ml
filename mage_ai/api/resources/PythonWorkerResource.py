from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


class PythonWorkerResource(GenericResource):
    """
    The project's Python workers (python_workers/<name>/): GET /api/python_workers lists
    them with whether each is prepared; POST prepares one in the background, or with a
    template adds a worker, locks it and prepares it, or with combine and pipeline_uuid
    writes one worker for those workers, moves the pipeline's blocks to it, locks it and
    prepares it; GET
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
        repo_path = get_repo_path(user=user)
        template = payload.get('template')
        combine = payload.get('combine')
        if combine:
            from mage_ai.data_preparation.models.pipeline import Pipeline

            pipeline = Pipeline.get(payload.get('pipeline_uuid') or '', repo_path=repo_path,
                                    check_if_exists=True)
            if pipeline is None or not isinstance(combine, list):
                raise ApiError(dict(code=400, message=(
                    'Name the pipeline (pipeline_uuid) and the workers to combine.')))
            try:
                environments.combine_into(pipeline, combine, name,
                                          label=payload.get('label') or None)
            except environments.PipelineEnvironmentError as error:
                raise ApiError(dict(code=400, message=str(error)))
            template = 'combined'
        elif template:
            try:
                environments.new_worker(
                    repo_path, name, template,
                    label=payload.get('label') or None,
                    description=payload.get('description') or None,
                )
            except environments.PipelineEnvironmentError as error:
                raise ApiError(dict(code=400, message=str(error)))
        try:
            job = environments.start_prepare(repo_path, name, lock=bool(template))
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
