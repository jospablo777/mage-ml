from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.PipelinePartitionPresenter import PipelinePartitionPresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class PipelinePartitionPolicy(BasePolicy):
    @property
    def entity(self):
        """The pipeline, so pipeline permissions apply."""
        payload = (self.options.get('payload') or {}).get('pipeline_partition') or {}
        pipeline_uuid = payload.get('pipeline_uuid') if isinstance(payload, dict) else None
        if not isinstance(pipeline_uuid, str):
            model = getattr(self.resource, 'model', None)
            pipeline_uuid = model.get('pipeline_uuid') if isinstance(model, dict) else None
        if isinstance(pipeline_uuid, str):
            return Entity.PIPELINE, pipeline_uuid
        return Entity.PROJECT, get_project_uuid()


# Running partitions starts pipeline runs: editors, as triggering a pipeline.
PipelinePartitionPolicy.allow_actions(
    [constants.CREATE],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

PipelinePartitionPolicy.allow_actions(
    [constants.DETAIL],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

PipelinePartitionPolicy.allow_read(
    PipelinePartitionPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE, constants.DETAIL],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

PipelinePartitionPolicy.allow_write(
    ['action', 'pipeline_uuid', 'select'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.CREATE],
    condition=lambda policy: policy.has_at_least_editor_role(),
)
