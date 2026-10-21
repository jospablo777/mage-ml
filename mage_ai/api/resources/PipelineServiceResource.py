from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path

FIELDS = ('alerts', 'description', 'include', 'max_runs', 'model_registry', 'models',
          'pipelines', 'platforms')
# A model's settings the Services page does not edit: an update without them keeps them.
MODEL_SETTINGS = ('challengers', 'shadow', 'audit')


def _keep_model_settings(models, previous_models):
    """The models with the settings their earlier versions had and the payload left out."""
    if not isinstance(models, list):
        return models
    earlier = {m.get('name'): m for m in previous_models or [] if isinstance(m, dict)}
    kept = []
    for model in models:
        if isinstance(model, dict):
            model = dict(model)
            for key in MODEL_SETTINGS:
                if key not in model and key in earlier.get(model.get('name'), {}):
                    model[key] = earlier[model['name']][key]
        kept.append(model)
    return kept


def _with_export(
    repo_path: str,
    name: str,
    value: Dict,
    exports_by_name: Dict = None,
    deployments: Dict = None,
) -> Dict:
    """A saved service with its latest export from the app, if any, and its deployments."""
    from mage_ai.pipeline_services.exports import list_exports

    if exports_by_name is None:
        exports_by_name = {e['name']: e for e in list_exports(repo_path)}
    latest = exports_by_name.get(name)
    return dict(
        name=name,
        **value,
        deployments=(deployments or {}).get(name, []),
        exported_at=latest['exported_at'] if latest else None,
        path=_definitions().location(repo_path, name),
    )


def deployments_by_service(repo_path: str) -> Dict[str, list]:
    """
    Each service's deployments (Docker, and Kubernetes environments asked in parallel with a
    short timeout, so an unreachable cluster delays the list by seconds at most) with their
    drift from the repository's lock, for the Services list.
    """
    from mage_ai.pipeline_services import deploy, lifecycle

    try:
        found, _ = deploy.deployments(repo_path, request_timeout='3s')
    except (deploy.DeployError, OSError, ValueError):
        return {}
    locks = {}
    result = {}
    for item in found:
        if item.service not in locks:
            locks[item.service] = (lifecycle.read_lock(repo_path, item.service) or {}).get(
                'manifest_sha256',
            )
        result.setdefault(item.service, []).append(dict(
            commit=item.commit,
            drift=item.drift(locks[item.service]),
            environment=item.environment,
            image=item.image,
            port=item.port,
            state=item.state,
        ))
    return result


def _definitions():
    from mage_ai.pipeline_services import definitions

    return definitions


class PipelineServiceResource(GenericResource):
    """
    Services saved in the project (services/<name>/service.yaml, or the earlier
    pipeline_services.yaml; pipeline_services/definitions.py):
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
        # ?pipeline=<uuid>: the services that run that pipeline, for its pages.
        pipeline = (query.get('pipeline') or [None])[0]
        if pipeline:
            saved = {n: v for n, v in saved.items() if pipeline in v['pipelines']}
        exports_by_name = {e['name']: e for e in list_exports(repo_path)}
        # Docker is asked off the event loop: a slow daemon held every request.
        import asyncio

        deployments = await asyncio.to_thread(deployments_by_service, repo_path)
        return cls.build_result_set(
            [_with_export(repo_path, n, v, exports_by_name, deployments)
             for n, v in sorted(saved.items())],
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
        merged['models'] = _keep_model_settings(merged.get('models'), self.model.get('models'))
        try:
            value = definitions.save_definition(repo_path, name, merged, previous_name=previous)
        except definitions.DefinitionError as error:
            raise ApiError(dict(code=400, message=str(error)))
        self.model = _with_export(repo_path, name, value)

    async def delete(self, **kwargs):
        _definitions().delete_definition(
            get_repo_path(user=self.current_user), self.model['name'],
        )
