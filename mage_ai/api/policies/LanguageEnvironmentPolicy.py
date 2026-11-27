from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.LanguageEnvironmentPresenter import (
    LanguageEnvironmentPresenter,
)
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class LanguageEnvironmentPolicy(BasePolicy):
    @property
    def entity(self):
        return Entity.PROJECT, get_project_uuid()


LanguageEnvironmentPolicy.allow_actions(
    [constants.DETAIL, constants.LIST],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

# Changing the packages changes the project's files; syncing installs on the server.
LanguageEnvironmentPolicy.allow_actions(
    [constants.CREATE, constants.DELETE, constants.UPDATE],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

LanguageEnvironmentPolicy.allow_read(
    LanguageEnvironmentPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE, constants.DELETE, constants.DETAIL, constants.LIST,
               constants.UPDATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

LanguageEnvironmentPolicy.allow_write(
    ['init', 'manifest_text', 'sync'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.UPDATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

LanguageEnvironmentPolicy.allow_write(
    ['language', 'name', 'r_version', 'start_from'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
