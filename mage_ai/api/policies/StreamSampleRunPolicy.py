from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.StreamSampleRunPresenter import StreamSampleRunPresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class StreamSampleRunPolicy(BasePolicy):
    @property
    def entity(self):
        return Entity.PROJECT, get_project_uuid()


# Runs the pipeline's code on messages: editors only.
StreamSampleRunPolicy.allow_actions(
    [constants.CREATE],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

StreamSampleRunPolicy.allow_read(
    StreamSampleRunPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

StreamSampleRunPolicy.allow_write(
    ['block_uuid', 'messages', 'sample', 'variables'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
