from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.PipelineServiceExportPresenter import (
    PipelineServiceExportPresenter,
)
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class PipelineServiceExportPolicy(BasePolicy):
    @property
    def entity(self):
        return Entity.PROJECT, get_project_uuid()


# An export copies the pipelines' code and the project files they read, and reports the
# environment variables they need: editors only.
PipelineServiceExportPolicy.allow_actions(
    [constants.CREATE, constants.DETAIL, constants.LIST],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

PipelineServiceExportPolicy.allow_read(
    PipelineServiceExportPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE, constants.DETAIL, constants.LIST],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

PipelineServiceExportPolicy.allow_write(
    ['include', 'max_runs', 'model_registry', 'models', 'name', 'pipeline_uuids', 'preview'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
