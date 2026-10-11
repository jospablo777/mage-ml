from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.PipelineDocumentationPresenter import (
    PipelineDocumentationPresenter,
)
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class PipelineDocumentationPolicy(BasePolicy):
    @property
    def entity(self):
        """The pipeline, so pipeline permissions apply."""
        model = getattr(self.resource, 'model', None)
        pipeline_uuid = model.get('pipeline_uuid') if isinstance(model, dict) else None
        if isinstance(pipeline_uuid, str):
            return Entity.PIPELINE, pipeline_uuid
        return Entity.PROJECT, get_project_uuid()


PipelineDocumentationPolicy.allow_actions(
    [constants.DETAIL],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

PipelineDocumentationPolicy.allow_actions(
    [constants.UPDATE],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

PipelineDocumentationPolicy.allow_read(
    PipelineDocumentationPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.DETAIL, constants.UPDATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

PipelineDocumentationPolicy.allow_write(
    ['format', 'source'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.UPDATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
