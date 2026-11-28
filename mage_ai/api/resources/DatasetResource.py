import asyncio
from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


def _first(query, key):
    value = (query or {}).get(key)
    return value[0] if isinstance(value, list) and value else value


class DatasetResource(GenericResource):
    """
    Datasets (data_preparation/datasets.py, ADR-0046): GET /api/datasets?query= lists the
    matching ones with their status (drafts=1: global data products' outputs to register;
    pipeline_uuid= and block_uuid=: those a block publishes, for its output in the editor);
    GET /api/datasets/<id> returns one with its columns, runs, dependencies and, for a
    partitioned pipeline, its partitions' coverage; POST
    registers one in datasets.yaml; DELETE removes it.
    """

    @classmethod
    async def collection(cls, query, meta, user, **kwargs):
        from mage_ai.data_preparation import datasets

        repo_path = get_repo_path(user=user)
        try:
            if _first(query, 'drafts') in ('1', 'true'):
                rows = await asyncio.to_thread(datasets.suggestions, repo_path)
            elif _first(query, 'pipeline_uuid'):
                found = datasets.published_by(datasets.load(repo_path),
                                              _first(query, 'pipeline_uuid'),
                                              _first(query, 'block_uuid'))
                rows = await asyncio.to_thread(
                    lambda: [datasets.summary(repo_path, e) for e in found],
                )
            else:
                found = datasets.search(datasets.load(repo_path), _first(query, 'query'))
                rows = await asyncio.to_thread(
                    lambda: [datasets.summary(repo_path, e) for e in found],
                )
        except datasets.DatasetError as error:
            raise ApiError(dict(code=400, message=str(error)))
        return cls.build_result_set(rows, user, **kwargs)

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'DatasetResource':
        from mage_ai.data_preparation import datasets

        repo_path = get_repo_path(user=user)

        def detail():
            entries = datasets.load(repo_path)
            entry = next((e for e in entries if e.get('id') == pk), None)
            if entry is None:
                return None
            return dict(entry, **datasets.status(repo_path, entry),
                        dependencies=datasets.dependencies(repo_path, entries, entry),
                        coverage=datasets.coverage(repo_path, entry))

        try:
            found = await asyncio.to_thread(detail)
        except datasets.DatasetError as error:
            raise ApiError(dict(code=400, message=str(error)))
        if found is None:
            raise ApiError(dict(code=404, message='Unknown dataset.'))
        return cls(found, user, **kwargs)

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'DatasetResource':
        from mage_ai.data_preparation import datasets

        try:
            entry = await asyncio.to_thread(datasets.register, get_repo_path(user=user),
                                            dict(payload))
        except datasets.DatasetError as error:
            raise ApiError(dict(code=400, message=str(error)))
        return cls(entry, user, **kwargs)

    async def delete(self, **kwargs):
        from mage_ai.data_preparation import datasets

        try:
            datasets.unregister(get_repo_path(user=self.current_user), self.model['id'])
        except datasets.DatasetError as error:
            raise ApiError(dict(code=404, message=str(error)))
