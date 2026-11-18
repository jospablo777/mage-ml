from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path

# Why MageML's own Python is not changed from the app (python_workers.mageml_environment).
BUILTIN_CHANGE = (
    "MageML's own Python is the environment the server runs in: change it with uv in a "
    'terminal (uv add, in the project MageML is installed in), then restart MageML. For the '
    'packages of some blocks, add a worker instead.'
)


class PythonWorkerResource(GenericResource):
    """
    The project's Python workers (python_workers/<name>/): GET /api/python_workers lists
    them with whether each is prepared; POST prepares one in the background, or with a
    template adds a worker, locks it and prepares it, or with combine and pipeline_uuid
    writes one worker for those workers, moves the pipeline's blocks to it, locks it and
    prepares it; GET
    /api/python_workers/<name> gives it with each package it locks, where it is used, its
    last security check, and its prepare's state with every line uv printed; PUT changes
    it and DELETE removes it (data_preparation/python_workers.py).
    """

    @classmethod
    async def collection(cls, query, meta, user, **kwargs):
        from mage_ai.data_preparation import environments, python_workers

        repo_path = get_repo_path(user=user)
        try:
            workers = python_workers.overview(repo_path)
        except environments.PipelineEnvironmentError as error:
            raise ApiError(dict(code=400, message=str(error)))
        # Missing or old security checks run in a thread: the list answers at once.
        python_workers.audit_stale_in_background(repo_path)
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
        elif template or payload.get('dependencies') is not None:
            import asyncio

            from mage_ai.data_preparation import python_workers

            dependencies = payload.get('dependencies')
            if dependencies is not None and (
                not isinstance(dependencies, list) or
                not all(isinstance(d, str) for d in dependencies)
            ):
                raise ApiError(dict(code=400, message='dependencies is a list of requirements.'))
            try:
                created = await asyncio.to_thread(
                    python_workers.create, repo_path, name, template or 'empty',
                    label=payload.get('label') or None,
                    description=payload.get('description') or None,
                    dependencies=dependencies,
                    python=payload.get('python') or None,
                    accept_older=bool(payload.get('accept_older')),
                )
            except environments.PipelineEnvironmentError as error:
                raise ApiError(dict(code=400, message=str(error)))
            # With confirm, nothing was added: the page asks whether to go on.
            return cls(dict(name=created['name'], job=created['job'],
                            confirm=created.get('confirm')), user, **kwargs)
        try:
            job = environments.start_prepare(repo_path, name, lock=bool(template))
        except environments.PipelineEnvironmentError as error:
            raise ApiError(dict(code=404, message=str(error)))
        return cls(dict(name=job['name'], job=job), user, **kwargs)

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'PythonWorkerResource':
        from mage_ai.data_preparation import environments, python_workers

        if pk in environments.BUILTIN_WORKERS:
            # MageML's own Python: shown, changed with uv in a terminal.
            return cls(python_workers.mageml_environment(), user, **kwargs)
        try:
            worker = python_workers.detail(get_repo_path(user=user), pk)
        except environments.PipelineEnvironmentError as error:
            code = 404 if str(error).startswith('There is no Python worker') else 400
            raise ApiError(dict(code=code, message=str(error)))
        return cls(worker, user, **kwargs)

    async def update(self, payload: Dict, **kwargs):
        """
        PUT /api/python_workers/<name>: dependencies (the full list), python, label,
        description, combine. A change of packages or Python is locked in a copy first,
        then prepared; the answer has the packages that moved and the prepare job. A change
        that would lock a package below its newest release changes nothing and answers with
        confirm, unless accept_older.
        """
        import asyncio

        from mage_ai.data_preparation import environments, python_workers

        if self.model.get('builtin'):
            raise ApiError(dict(code=400, message=BUILTIN_CHANGE))
        dependencies = payload.get('dependencies')
        if dependencies is not None and (
            not isinstance(dependencies, list) or
            not all(isinstance(d, str) for d in dependencies)
        ):
            raise ApiError(dict(code=400, message='dependencies is a list of requirements.'))
        repo_path = get_repo_path(user=self.current_user)
        pyproject = payload.get('pyproject')
        if pyproject is not None:
            # The whole file, edited as text.
            if not isinstance(pyproject, str):
                raise ApiError(dict(code=400, message='pyproject is the file\'s text.'))
            try:
                result = await asyncio.to_thread(
                    python_workers.save_pyproject, repo_path, self.model['name'], pyproject,
                    accept_older=bool(payload.get('accept_older')))
                self.model = dict(python_workers.detail(repo_path, self.model['name']), **result)
            except environments.PipelineEnvironmentError as error:
                raise ApiError(dict(code=400, message=str(error)))
            return
        try:
            result = await asyncio.to_thread(
                python_workers.update, repo_path, self.model['name'],
                dependencies=dependencies,
                label=payload.get('label'),
                description=payload.get('description'),
                python=payload.get('python'),
                combine=payload.get('combine'),
                memory=payload.get('memory'),
                accept_older=bool(payload.get('accept_older')),
            )
            self.model = dict(python_workers.detail(repo_path, self.model['name']), **result)
        except environments.PipelineEnvironmentError as error:
            raise ApiError(dict(code=400, message=str(error)))

    async def delete(self, **kwargs):
        """DELETE /api/python_workers/<name>: refused while a block runs in it."""
        from mage_ai.data_preparation import environments, python_workers

        if self.model.get('builtin'):
            raise ApiError(dict(code=400, message=BUILTIN_CHANGE))
        try:
            python_workers.delete(get_repo_path(user=self.current_user), self.model['name'])
        except environments.PipelineEnvironmentError as error:
            raise ApiError(dict(code=400, message=str(error)))
