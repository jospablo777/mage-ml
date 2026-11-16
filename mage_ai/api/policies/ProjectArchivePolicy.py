from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.ProjectArchivePresenter import ProjectArchivePresenter


class ProjectArchivePolicy(BasePolicy):
    pass


# Editors: an import writes pipelines and block code, as editing them does.
ProjectArchivePolicy.allow_actions(
    [constants.CREATE],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

ProjectArchivePolicy.allow_read(
    ProjectArchivePresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

ProjectArchivePolicy.allow_write(
    ['action', 'content', 'mode', 'pipeline_uuids'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
