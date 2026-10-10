from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.ModelReleasePresenter import ModelReleasePresenter


class ModelReleasePolicy(BasePolicy):
    pass


ModelReleasePolicy.allow_actions(
    [
        constants.DETAIL,
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

# Moving a model's alias changes what its consumers load, as deploying does.
ModelReleasePolicy.allow_actions(
    [
        constants.CREATE,
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

ModelReleasePolicy.allow_read(
    ModelReleasePresenter.default_attributes,
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    on_action=[
        constants.CREATE,
        constants.DETAIL,
    ],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

ModelReleasePolicy.allow_write(
    [
        'action',
        'expected_champion',
        'model',
        'version',
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    on_action=[
        constants.CREATE,
    ],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
