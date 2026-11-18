from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.PythonWorkerCombinationPresenter import (
    PythonWorkerCombinationPresenter,
)


class PythonWorkerCombinationPolicy(BasePolicy):
    pass


# It writes nothing, but each new question runs uv: editors, who can act on the answer.
PythonWorkerCombinationPolicy.allow_actions(
    [constants.CREATE],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

PythonWorkerCombinationPolicy.allow_read(
    PythonWorkerCombinationPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

PythonWorkerCombinationPolicy.allow_write(
    ['workers'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
