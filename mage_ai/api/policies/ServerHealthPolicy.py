from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.ServerHealthPresenter import ServerHealthPresenter


class ServerHealthPolicy(BasePolicy):
    pass


# Owners and admins: the processes and paths of the server.
ServerHealthPolicy.allow_actions(
    [constants.DETAIL],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_admin_role(),
)

ServerHealthPolicy.allow_read(
    ServerHealthPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.DETAIL],
    condition=lambda policy: policy.has_at_least_admin_role(),
)
