from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.ColumnAtlasQueryPresenter import ColumnAtlasQueryPresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class ColumnAtlasQueryPolicy(BasePolicy):
    @property
    def entity(self):
        """The pipeline the query reads, so pipeline permissions apply to its outputs."""
        payload = (self.options.get('payload') or {}).get('column_atlas_query') or {}
        source = payload.get('source') if isinstance(payload, dict) else None
        if isinstance(source, dict) and isinstance(source.get('pipeline_uuid'), str):
            return Entity.PIPELINE, source['pipeline_uuid']
        return Entity.PROJECT, get_project_uuid()


# A query reads an output and changes nothing, so it needs the viewer role on the pipeline.
ColumnAtlasQueryPolicy.allow_actions(
    [
        constants.CREATE,
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

ColumnAtlasQueryPolicy.allow_read(
    ColumnAtlasQueryPresenter.default_attributes,
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    on_action=[
        constants.CREATE,
    ],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

ColumnAtlasQueryPolicy.allow_write(
    [
        'action',
        'bins',
        'columns',
        'limit',
        'offset',
        'source',
        'view',
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    on_action=[
        constants.CREATE,
    ],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)
