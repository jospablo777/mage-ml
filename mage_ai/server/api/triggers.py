import hmac
import json

from mage_ai.api.errors import ApiError
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.data_preparation.models.triggers import ScheduleType
from mage_ai.orchestration.db import safe_db_query
from mage_ai.orchestration.db.models.schedules import PipelineRun, PipelineSchedule
from mage_ai.orchestration.triggers.utils import create_and_start_pipeline_run
from mage_ai.server.api.base import BaseHandler
from mage_ai.server.api.errors import UnauthenticatedRequestException
from mage_ai.shared.requests import get_bearer_auth_token_from_headers


class ApiTriggerPipelineHandler(BaseHandler):
    model_class = PipelineRun

    @safe_db_query
    def post(self, pipeline_schedule_id, token: str = None):
        pipeline_schedule = PipelineSchedule.get_by_id(int(pipeline_schedule_id))
        if not pipeline_schedule:
            raise ApiError(ApiError.RESOURCE_NOT_FOUND)

        if token is None:
            token = get_bearer_auth_token_from_headers(self.request.headers)

        if ScheduleType.API != pipeline_schedule.schedule_type:
            raise UnauthenticatedRequestException(
                'This endpoint is only supported for API trigger.',
            )
        if not pipeline_schedule.token:
            raise UnauthenticatedRequestException(
                'The token of the API trigger cannot be empty.',
            )

        if not hmac.compare_digest(str(pipeline_schedule.token), str(token or '')):
            raise UnauthenticatedRequestException(
                f'Invalid token for pipeline schedule ID {pipeline_schedule_id}.',
            )

        payload = {}
        if self.request.body:
            try:
                body = json.loads(self.request.body)
            except ValueError as error:
                raise ApiError(dict(
                    ApiError.RESOURCE_INVALID,
                    message=f'The request body is not JSON: {error}.',
                ))
            if isinstance(body, dict):
                payload = body.get('pipeline_run') or {}
                payload['event_variables'] = {
                    k: v for k, v in body.items() if k != 'pipeline_run'
                }
            else:
                # Webhooks such as HubSpot's send a list; it failed with a 500. Blocks get
                # it as kwargs['body'] (upstream #5414).
                payload['event_variables'] = dict(body=body)
        if 'variables' not in payload:
            payload['variables'] = {}

        pipeline = Pipeline.get(
            pipeline_schedule.pipeline_uuid, repo_path=pipeline_schedule.repo_path
        )
        pipeline_run = create_and_start_pipeline_run(
            pipeline,
            pipeline_schedule,
            payload,
            should_schedule=False,
        )

        self.write(dict(pipeline_run=pipeline_run.to_dict()))
