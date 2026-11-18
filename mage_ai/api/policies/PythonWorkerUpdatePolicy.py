from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.PythonWorkerUpdatePresenter import (
    PythonWorkerUpdatePresenter,
)


class PythonWorkerUpdatePolicy(BasePolicy):
    pass


# A preview runs uv and an update changes the worker's lock: editors.
PythonWorkerUpdatePolicy.allow_actions(
    [constants.CREATE, constants.UPDATE],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

PythonWorkerUpdatePolicy.allow_read(
    PythonWorkerUpdatePresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE, constants.UPDATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

PythonWorkerUpdatePolicy.allow_write(
    ['fix_vulnerabilities', 'packages', 'worker'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
