from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.PipelineDependencyPresenter import (
    PipelineDependencyPresenter,
)


class PipelineDependencyPolicy(BasePolicy):
    pass


PipelineDependencyPolicy.allow_actions(
    [constants.DETAIL],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

PipelineDependencyPolicy.allow_read(
    PipelineDependencyPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.DETAIL],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)
