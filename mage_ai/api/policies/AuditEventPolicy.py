from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.AuditEventPresenter import AuditEventPresenter


class AuditEventPolicy(BasePolicy):
    pass


# Owners and admins: the log names every user and what they changed.
AuditEventPolicy.allow_actions(
    [constants.LIST],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_admin_role(),
)

AuditEventPolicy.allow_read(
    AuditEventPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.LIST],
    condition=lambda policy: policy.has_at_least_admin_role(),
)

AuditEventPolicy.allow_query(
    ['action', 'outcome', 'object_id', 'object_type', 'since', 'until', 'user_name'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.LIST],
    condition=lambda policy: policy.has_at_least_admin_role(),
)
