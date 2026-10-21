from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.PythonWorkerPresenter import PythonWorkerPresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class PythonWorkerPolicy(BasePolicy):
    @property
    def entity(self):
        return Entity.PROJECT, get_project_uuid()


PythonWorkerPolicy.allow_actions(
    [constants.DETAIL, constants.LIST],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

# Preparing installs packages on the server.
PythonWorkerPolicy.allow_actions(
    [constants.CREATE],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

PythonWorkerPolicy.allow_read(
    PythonWorkerPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE, constants.DETAIL, constants.LIST],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

PythonWorkerPolicy.allow_write(
    ['name'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
