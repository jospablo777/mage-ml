from mage_ai.api.oauth_scope import OauthScope
from mage_ai.api.operations import constants
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.api.presenters.MonitorReportPresenter import MonitorReportPresenter
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity


class MonitorReportPolicy(BasePolicy):
    @property
    def entity(self):
        """The monitored pipeline, so pipeline permissions apply."""
        model = getattr(self.resource, 'model', None)
        pipeline_uuid = model.get('pipeline_uuid') if isinstance(model, dict) else None
        if isinstance(pipeline_uuid, str):
            return Entity.PIPELINE, pipeline_uuid
        return Entity.PROJECT, get_project_uuid()


MonitorReportPolicy.allow_actions(
    [constants.DETAIL],
    scopes=[OauthScope.CLIENT_PRIVATE],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

MonitorReportPolicy.allow_read(
    MonitorReportPresenter.default_attributes,
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.DETAIL],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)

MonitorReportPolicy.allow_query(
    ['pipeline_run_id', 'report'],
    scopes=[OauthScope.CLIENT_PRIVATE],
    on_action=[constants.DETAIL],
    condition=lambda policy: policy.has_at_least_viewer_role(),
)
