from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.FusionVerificationPresenter import FusionVerificationPresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class FusionVerificationPolicy(BasePolicy):
    @property
    def entity(self):
        """The pipeline, so pipeline permissions apply."""
        payload = (self.options.get('payload') or {}).get('fusion_verification') or {}
        pipeline_uuid = payload.get('pipeline_uuid') if isinstance(payload, dict) else None
        if not isinstance(pipeline_uuid, str) and isinstance(self.resource, object):
            model = getattr(self.resource, 'model', None)
            pipeline_uuid = model.get('pipeline_uuid') if isinstance(model, dict) else None
        if isinstance(pipeline_uuid, str):
            return Entity.PIPELINE, pipeline_uuid
        return Entity.PROJECT, get_project_uuid()


# A verification runs the pipeline twice, exporters included, as running it does.
FusionVerificationPolicy.allow_actions(
    [
        constants.CREATE,
        constants.DELETE,
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

FusionVerificationPolicy.allow_actions(
    [
        constants.DETAIL,
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

FusionVerificationPolicy.allow_read(
    FusionVerificationPresenter.default_attributes,
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    on_action=[
        constants.CREATE,
        constants.DELETE,
        constants.DETAIL,
    ],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

FusionVerificationPolicy.allow_write(
    [
        'exact',
        'pipeline_uuid',
        'variables',
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    on_action=[
        constants.CREATE,
    ],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
