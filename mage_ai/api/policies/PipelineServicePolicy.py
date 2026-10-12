from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.PipelineServicePresenter import PipelineServicePresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class PipelineServicePolicy(BasePolicy):
    @property
    def entity(self):
        return Entity.PROJECT, get_project_uuid()


PipelineServicePolicy.allow_actions(
    [constants.DETAIL, constants.LIST],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

# Saving changes pipeline_services.yaml in the project.
PipelineServicePolicy.allow_actions(
    [constants.CREATE, constants.DELETE, constants.UPDATE],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

PipelineServicePolicy.allow_read(
    PipelineServicePresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[
        constants.CREATE, constants.DELETE, constants.DETAIL, constants.LIST, constants.UPDATE,
    ],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

PipelineServicePolicy.allow_write(
    ['description', 'include', 'max_runs', 'model_registry', 'models', 'name', 'pipelines'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE, constants.UPDATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
