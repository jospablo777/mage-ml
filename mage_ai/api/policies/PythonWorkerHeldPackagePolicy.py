from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.PythonWorkerHeldPackagePresenter import (
    PythonWorkerHeldPackagePresenter,
)
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class PythonWorkerHeldPackagePolicy(BasePolicy):
    @property
    def entity(self):
        return Entity.PROJECT, get_project_uuid()


# Which packages are held back is for everyone who sees the workers; changing them is an
# update.
PythonWorkerHeldPackagePolicy.allow_actions(
    [constants.DETAIL],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

PythonWorkerHeldPackagePolicy.allow_read(
    PythonWorkerHeldPackagePresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.DETAIL],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

PythonWorkerHeldPackagePolicy.allow_query(
    ['refresh'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.DETAIL],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)
