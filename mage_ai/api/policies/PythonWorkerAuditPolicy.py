from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.PythonWorkerAuditPresenter import PythonWorkerAuditPresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class PythonWorkerAuditPolicy(BasePolicy):
    @property
    def entity(self):
        return Entity.PROJECT, get_project_uuid()


# What is vulnerable is for everyone who sees the workers; fixing it is an update.
PythonWorkerAuditPolicy.allow_actions(
    [constants.DETAIL],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

PythonWorkerAuditPolicy.allow_read(
    PythonWorkerAuditPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.DETAIL],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

PythonWorkerAuditPolicy.allow_query(
    ['refresh'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.DETAIL],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)
