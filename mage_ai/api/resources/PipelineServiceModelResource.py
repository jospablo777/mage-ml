from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path

# Promotions the page shows, the latest last.
SHOWN_PROMOTIONS = 10


def _state(repo_path: str, name: str) -> Dict:
    from mage_ai.pipeline_services import lifecycle

    return dict(
        name=name,
        models=lifecycle.models_status(repo_path, name),
        promotions=lifecycle.promotions(repo_path, name)[-SHOWN_PROMOTIONS:],
    )


class PipelineServiceModelResource(GenericResource):
    """
    A service's models in the registry, for the Services page: GET
    /api/pipeline_service_models/<service> gives each model's built version, the version
    its uri points to now (moved when they differ), its challengers' versions and the last
    promotions; POST promotes a version (a challenger's, the moved alias's, or a version),
    as `mage service promote` does, recorded with the signed-in user. Building and shipping
    the image is the next step.
    """

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'PipelineServiceModelResource':
        from mage_ai.pipeline_services import definitions

        try:
            state = _state(get_repo_path(user=user), pk)
        except definitions.DefinitionError as error:
            raise ApiError(dict(code=404, message=str(error)))
        return cls(state, user, **kwargs)

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'PipelineServiceModelResource':
        from mage_ai.pipeline_services import definitions, lifecycle

        name = payload.get('name')
        model = payload.get('model')
        if not isinstance(name, str) or not isinstance(model, str):
            raise ApiError(dict(code=400, message='Name the service and its model.'))
        version = payload.get('version')
        repo_path = get_repo_path(user=user)
        try:
            record = lifecycle.promote(
                repo_path,
                name,
                model,
                challenger=payload.get('challenger') or None,
                version=str(version) if version not in (None, '') else None,
                move_alias=bool(payload.get('move_alias')),
                reason=(payload.get('reason') or '').strip() or None,
                by=(getattr(user, 'email', None) or getattr(user, 'username', None)),
            )
        except lifecycle.PromoteError as error:
            raise ApiError(dict(code=400, message=str(error)))
        except definitions.DefinitionError as error:
            raise ApiError(dict(code=404, message=str(error)))
        return cls(dict(_state(repo_path, name), promoted=record), user, **kwargs)
