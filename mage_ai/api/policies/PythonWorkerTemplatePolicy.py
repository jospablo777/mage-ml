from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.PythonWorkerTemplatePresenter import (
    PythonWorkerTemplatePresenter,
)


class PythonWorkerTemplatePolicy(BasePolicy):
    pass


PythonWorkerTemplatePolicy.allow_actions(
    [constants.LIST],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

PythonWorkerTemplatePolicy.allow_read(
    PythonWorkerTemplatePresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.LIST],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)
