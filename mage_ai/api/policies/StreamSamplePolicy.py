from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.StreamSamplePresenter import StreamSamplePresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity

ACTIONS = [constants.CREATE, constants.DELETE, constants.DETAIL, constants.LIST, constants.UPDATE]


class StreamSamplePolicy(BasePolicy):
    @property
    def entity(self):
        return Entity.PROJECT, get_project_uuid()


# Samples hold messages from the stream's source, which can be real data: editors only.
StreamSamplePolicy.allow_actions(
    ACTIONS,
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

StreamSamplePolicy.allow_read(
    StreamSamplePresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=ACTIONS,
    condition=lambda policy: policy.has_at_least_editor_role(),
)

StreamSamplePolicy.allow_write(
    ['capture', 'count', 'keep_out_of_git', 'messages', 'name', 'start'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=ACTIONS,
    condition=lambda policy: policy.has_at_least_editor_role(),
)
