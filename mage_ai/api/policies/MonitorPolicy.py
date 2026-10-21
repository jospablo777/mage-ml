from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.MonitorPresenter import MonitorPresenter


class MonitorPolicy(BasePolicy):
    pass


MonitorPolicy.allow_actions(
    [constants.DETAIL, constants.LIST],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

MonitorPolicy.allow_read(
    MonitorPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.DETAIL, constants.LIST],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

# Silencing a monitor's notifications changes who hears about failures: editors only.
MonitorPolicy.allow_actions(
    [constants.UPDATE],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

MonitorPolicy.allow_read(
    MonitorPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.UPDATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

MonitorPolicy.allow_write(
    ['silence'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.UPDATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
