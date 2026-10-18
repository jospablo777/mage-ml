from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.StreamRunPresenter import StreamRunPresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity

ACTIONS = [constants.CREATE, constants.DELETE, constants.DETAIL, constants.UPDATE]


class StreamRunPolicy(BasePolicy):
    @property
    def entity(self):
        return Entity.PROJECT, get_project_uuid()


# Runs the pipeline's code against its source and sinks: editors only.
StreamRunPolicy.allow_actions(
    ACTIONS,
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

StreamRunPolicy.allow_read(
    StreamRunPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=ACTIONS,
    condition=lambda policy: policy.has_at_least_editor_role(),
)

StreamRunPolicy.allow_write(
    ['action', 'confirmed_sinks'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=ACTIONS,
    condition=lambda policy: policy.has_at_least_editor_role(),
)
