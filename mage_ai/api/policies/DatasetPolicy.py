from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.DatasetPresenter import DatasetPresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class DatasetPolicy(BasePolicy):
    @property
    def entity(self):
        return Entity.PROJECT, get_project_uuid()


DatasetPolicy.allow_actions(
    [constants.DETAIL, constants.LIST],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

# Registering edits datasets.yaml, a project file: editors.
DatasetPolicy.allow_actions(
    [constants.CREATE, constants.DELETE],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

DatasetPolicy.allow_read(
    DatasetPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE, constants.DELETE, constants.DETAIL, constants.LIST],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

DatasetPolicy.allow_write(
    ['block', 'description', 'id', 'max_age', 'name', 'output', 'owner', 'pipeline',
     'time_column'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

DatasetPolicy.allow_query(
    ['drafts', 'query'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.LIST],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)
