from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.RunRecordPresenter import RunRecordPresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class RunRecordPolicy(BasePolicy):
    @property
    def entity(self):
        """The pipeline, so pipeline permissions apply."""
        model = getattr(self.resource, 'model', None)
        pipeline_uuid = model.get('pipeline_uuid') if isinstance(model, dict) else None
        payload = (self.options.get('payload') or {}).get('run_record') or {}
        if pipeline_uuid is None and isinstance(payload, dict) and payload.get('pipeline_run_id'):
            from mage_ai.orchestration.db.models.schedules import PipelineRun

            try:
                run = PipelineRun.get(int(payload['pipeline_run_id']))
            except (TypeError, ValueError):
                run = None
            pipeline_uuid = run.pipeline_uuid if run else None
        if isinstance(pipeline_uuid, str):
            return Entity.PIPELINE, pipeline_uuid
        return Entity.PROJECT, get_project_uuid()


RunRecordPolicy.allow_actions(
    [
        constants.DETAIL,
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

# A reproduction runs the pipeline again, exporters included, as running it does.
RunRecordPolicy.allow_actions(
    [
        constants.CREATE,
        constants.DELETE,
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

RunRecordPolicy.allow_read(
    RunRecordPresenter.default_attributes,
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    on_action=[
        constants.CREATE,
        constants.DELETE,
        constants.DETAIL,
    ],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

RunRecordPolicy.allow_write(
    [
        'pipeline_run_id',
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    on_action=[
        constants.CREATE,
    ],
    condition=lambda policy: policy.has_at_least_editor_role(),
)

RunRecordPolicy.allow_query(
    [
        'compare_with',
    ],
    scopes=[
        OauthScope.CLIENT_PRIVATE,
    ],
    on_action=[
        constants.DETAIL,
    ],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)
