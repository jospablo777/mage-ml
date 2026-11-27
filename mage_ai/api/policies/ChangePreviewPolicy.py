from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.ChangePreviewPresenter import ChangePreviewPresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class ChangePreviewPolicy(BasePolicy):
    @property
    def entity(self):
        """The pipeline, so pipeline permissions apply."""
        payload = (self.options.get('payload') or {}).get('change_preview') or {}
        pipeline_uuid = payload.get('pipeline_uuid') if isinstance(payload, dict) else None
        if not isinstance(pipeline_uuid, str):
            query = (self.options.get('query') or {}).get('pipeline_uuid')
            pipeline_uuid = query[0] if isinstance(query, list) and query else query
        if not isinstance(pipeline_uuid, str):
            model = getattr(self.resource, 'model', None)
            pipeline_uuid = model.get('pipeline_uuid') if isinstance(model, dict) else None
        if isinstance(pipeline_uuid, str):
            return Entity.PIPELINE, pipeline_uuid
        return Entity.PROJECT, get_project_uuid()


# A preview runs the pipeline's blocks: editors, as running blocks.
ChangePreviewPolicy.allow_actions(
    [constants.CREATE],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

ChangePreviewPolicy.allow_actions(
    [constants.DETAIL, constants.LIST],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

ChangePreviewPolicy.allow_read(
    ChangePreviewPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE, constants.DETAIL, constants.LIST],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

ChangePreviewPolicy.allow_write(
    ['baseline_run_id', 'keys', 'pipeline_uuid'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

ChangePreviewPolicy.allow_query(
    ['pipeline_uuid'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.DETAIL, constants.LIST],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)
