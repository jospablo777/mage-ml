from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


def _actor(user) -> str:
    return getattr(user, 'email', None) or getattr(user, 'username', None) or 'unknown user'


class ModelReleaseResource(GenericResource):
    """
    Releases of a registered model (orchestration/releases.py):
    GET /api/model_releases/<model> returns its policy, the version holding its alias and
    its release log; POST /api/model_releases promotes a version (action promote, with
    the champion it was evaluated against) or rolls the alias back (action rollback).
    """

    @classmethod
    def member(cls, pk, user, **kwargs) -> 'ModelReleaseResource':
        from mage_ai.orchestration import releases

        repo_path = get_repo_path(user=user)
        try:
            policy = releases.load_policy(pk, repo_path)
        except releases.ReleaseError as error:
            raise ApiError(dict(code=400, message=str(error)))
        if policy is None:
            raise ApiError(dict(code=404, message=f'{pk} has no release policy.'))
        try:
            champion = releases.champion_of(releases._client(), policy)
        except Exception as error:
            raise ApiError(dict(code=502, message=f'MLflow could not be reached: {error}'))
        return cls(dict(
            id=pk,
            model=pk,
            alias=policy.alias,
            approval=policy.approval,
            champion=str(champion.version) if champion else None,
            log=releases.release_log(repo_path, pk),
        ), user, **kwargs)

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'ModelReleaseResource':
        from mage_ai.orchestration import releases

        repo_path = get_repo_path(user=user)
        model = payload.get('model')
        action = payload.get('action') or 'promote'
        try:
            if action == 'promote':
                version = payload.get('version')
                if not version:
                    raise ApiError(dict(code=400, message='Promoting needs a version.'))
                releases.promote(
                    repo_path, model, str(version), payload.get('expected_champion'),
                    _actor(user),
                )
            elif action == 'rollback':
                releases.rollback(repo_path, model, _actor(user))
            else:
                raise ApiError(dict(code=400, message='The action is promote or rollback.'))
        except releases.ReleaseError as error:
            raise ApiError(dict(code=409, message=str(error)))
        return cls.member(model, user, **kwargs)
