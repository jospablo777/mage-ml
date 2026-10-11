from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.PipelineServiceBuildPresenter import (
    PipelineServiceBuildPresenter,
)
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class PipelineServiceBuildPolicy(BasePolicy):
    @property
    def entity(self):
        return Entity.PROJECT, get_project_uuid()


# Building runs docker on the server and can push to registries it is logged in to.
PipelineServiceBuildPolicy.allow_actions(
    [constants.CREATE, constants.DETAIL, constants.LIST],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

PipelineServiceBuildPolicy.allow_read(
    PipelineServiceBuildPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE, constants.DETAIL, constants.LIST],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

PipelineServiceBuildPolicy.allow_write(
    ['image', 'name', 'platforms', 'push'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
