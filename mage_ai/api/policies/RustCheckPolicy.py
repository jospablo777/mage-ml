from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.RustCheckPresenter import RustCheckPresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class RustCheckPolicy(BasePolicy):
    @property
    def entity(self):
        """The pipeline of the block, so pipeline permissions apply."""
        payload = (self.options.get('payload') or {}).get('rust_check') or {}
        pipeline_uuid = payload.get('pipeline_uuid') if isinstance(payload, dict) else None
        if isinstance(pipeline_uuid, str):
            return Entity.PIPELINE, pipeline_uuid
        return Entity.PROJECT, get_project_uuid()


# A check compiles the code in the project's Rust workspace, as editing the block does.
RustCheckPolicy.allow_actions(
    [
        constants.CREATE,
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

RustCheckPolicy.allow_read(
    RustCheckPresenter.default_attributes,
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    on_action=[
        constants.CREATE,
    ],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

RustCheckPolicy.allow_write(
    [
        'block_uuid',
        'content',
        'pipeline_uuid',
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    on_action=[
        constants.CREATE,
    ],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
