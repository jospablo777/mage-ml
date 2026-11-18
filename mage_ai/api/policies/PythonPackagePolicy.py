from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.PythonPackagePresenter import PythonPackagePresenter


class PythonPackagePolicy(BasePolicy):
    pass


# It reads the package index for whoever edits workers' packages.
PythonPackagePolicy.allow_actions(
    [constants.DETAIL],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

PythonPackagePolicy.allow_read(
    PythonPackagePresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.DETAIL],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
