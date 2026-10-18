from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.StreamDeadLetterPresenter import StreamDeadLetterPresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity

ACTIONS = [constants.CREATE, constants.DETAIL]


class StreamDeadLetterPolicy(BasePolicy):
    @property
    def entity(self):
        return Entity.PROJECT, get_project_uuid()


# Dead letters hold messages from the source, and replay writes to it: editors only.
StreamDeadLetterPolicy.allow_actions(
    ACTIONS,
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

StreamDeadLetterPolicy.allow_read(
    StreamDeadLetterPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=ACTIONS,
    condition=lambda policy: policy.has_at_least_editor_role(),
)

StreamDeadLetterPolicy.allow_write(
    ['action', 'confirm_topic', 'name', 'positions'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=ACTIONS,
    condition=lambda policy: policy.has_at_least_editor_role(),
)
