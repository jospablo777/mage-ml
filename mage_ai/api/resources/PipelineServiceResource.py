from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path

FIELDS = ('description', 'include', 'max_runs', 'pipelines')


def _with_export(repo_path: str, name: str, value: Dict, exports_by_name: Dict = None) -> Dict:
    """A saved service with its latest export from the app, if any."""
    from mage_ai.pipeline_services.exports import list_exports

    if exports_by_name is None:
        exports_by_name = {e['name']: e for e in list_exports(repo_path)}
    latest = exports_by_name.get(name)
    return dict(
        name=name,
        **value,
        exported_at=latest['exported_at'] if latest else None,
    )


def _definitions():
    from mage_ai.pipeline_services import definitions

    return definitions


class PipelineServiceResource(GenericResource):
    """
    Services saved in the project (pipeline_services.yaml, pipeline_services/definitions.py):
    GET /api/pipeline_services lists them with their latest export; POST saves a new one;
    PUT /api/pipeline_services/<name> changes or renames one; DELETE removes it. Their
    exports go through /api/pipeline_service_exports.
    """

    @classmethod
    async def collection(cls, query, meta, user, **kwargs):
        from mage_ai.pipeline_services.exports import list_exports

        definitions = _definitions()
        repo_path = get_repo_path(user=user)
        try:
            saved = definitions.load_definitions(repo_path)
        except definitions.DefinitionError as error:
            raise ApiError(dict(code=400, message=str(error)))
        exports_by_name = {e['name']: e for e in list_exports(repo_path)}
        return cls.build_result_set(
            [_with_export(repo_path, n, v, exports_by_name) for n, v in sorted(saved.items())],
            user,
            **kwargs,
        )

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'PipelineServiceResource':
        definitions = _definitions()
        repo_path = get_repo_path(user=user)
        try:
            value = definitions.load_definition(repo_path, pk)
        except definitions.DefinitionError as error:
            raise ApiError(dict(code=404, message=str(error)))
        return cls(_with_export(repo_path, pk, value), user, **kwargs)

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'PipelineServiceResource':
        definitions = _definitions()
        repo_path = get_repo_path(user=user)
        name = payload.get('name')
        if name in definitions.load_definitions(repo_path):
            raise ApiError(dict(code=400, message=f'A service named {name} already exists.'))
        try:
            value = definitions.save_definition(
                repo_path, name, {k: payload.get(k) for k in FIELDS},
            )
        except definitions.DefinitionError as error:
            raise ApiError(dict(code=400, message=str(error)))
        return cls(_with_export(repo_path, name, value), user, **kwargs)

    async def update(self, payload: Dict, **kwargs):
        definitions = _definitions()
        repo_path = get_repo_path(user=self.current_user)
        previous = self.model['name']
        name = payload.get('name') or previous
        merged = {k: payload[k] if k in payload else self.model.get(k) for k in FIELDS}
        try:
            value = definitions.save_definition(repo_path, name, merged, previous_name=previous)
        except definitions.DefinitionError as error:
            raise ApiError(dict(code=400, message=str(error)))
        self.model = _with_export(repo_path, name, value)

    async def delete(self, **kwargs):
        _definitions().delete_definition(
            get_repo_path(user=self.current_user), self.model['name'],
        )
