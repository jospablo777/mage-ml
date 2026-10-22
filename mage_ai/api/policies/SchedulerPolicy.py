from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.SchedulerPresenter import SchedulerPresenter


class SchedulerPolicy(BasePolicy):
    pass


SchedulerPolicy.allow_actions([
    constants.LIST,
], scopes=[
    OauthScope.CLIENT_PRIVATE,
], condition=lambda policy: policy.has_at_least_viewer_role())

# Starting or stopping the scheduler (action_type start or stop) starts or stops every
# scheduled run: a viewer could stop it.
SchedulerPolicy.allow_actions([
    constants.CREATE,
], scopes=[
    OauthScope.CLIENT_PRIVATE,
], condition=lambda policy: policy.has_at_least_editor_role())

SchedulerPolicy.allow_read(SchedulerPresenter.default_attributes, scopes=[
    OauthScope.CLIENT_PRIVATE,
], on_action=[
    constants.CREATE,
    constants.LIST,
], condition=lambda policy: policy.has_at_least_viewer_role())

SchedulerPolicy.allow_write(['action_type'], scopes=[
    OauthScope.CLIENT_PRIVATE,
], on_action=[
    constants.CREATE,
], condition=lambda policy: policy.has_at_least_editor_role())
