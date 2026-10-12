from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.LanguageSetupPresenter import LanguageSetupPresenter


class LanguageSetupPolicy(BasePolicy):
    pass


# Anyone who can add blocks can learn what their language needs.
LanguageSetupPolicy.allow_actions(
    [
        constants.DETAIL,
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

LanguageSetupPolicy.allow_read(
    LanguageSetupPresenter.default_attributes,
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    on_action=[
        constants.DETAIL,
    ],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
