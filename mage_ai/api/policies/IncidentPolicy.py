from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.IncidentPresenter import IncidentPresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class IncidentPolicy(BasePolicy):
    @property
    def entity(self):
        return Entity.PROJECT, get_project_uuid()


IncidentPolicy.allow_actions(
    [constants.LIST],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

IncidentPolicy.allow_read(
    IncidentPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.LIST],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

IncidentPolicy.allow_query(
    ['days'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.LIST],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)
