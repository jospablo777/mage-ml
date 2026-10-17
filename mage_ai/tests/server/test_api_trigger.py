import json
from types import SimpleNamespace
from unittest.mock import patch

import tornado.web
from tornado.testing import AsyncHTTPTestCase

from mage_ai.data_preparation.models.triggers import ScheduleType
from mage_ai.server.api.triggers import ApiTriggerPipelineHandler


class ApiTriggerBodyTest(AsyncHTTPTestCase):
    def get_app(self):
        return tornado.web.Application([
            (r'/api/pipeline_schedules/(?P<pipeline_schedule_id>\w+)/api_trigger',
             ApiTriggerPipelineHandler),
        ])

    def trigger(self, body, token='secret'):
        schedule = SimpleNamespace(
            schedule_type=ScheduleType.API, token='secret', pipeline_uuid='p', repo_path='r',
        )
        run = SimpleNamespace(to_dict=lambda: {'id': 1})
        with patch('mage_ai.server.api.triggers.PipelineSchedule.get_by_id',
                   return_value=schedule), \
                patch('mage_ai.server.api.triggers.Pipeline.get'), \
                patch('mage_ai.server.api.triggers.create_and_start_pipeline_run',
                      return_value=run) as start:
            response = self.fetch(
                '/api/pipeline_schedules/1/api_trigger', method='POST', body=body,
                headers={'Authorization': f'Bearer {token}'},
            )
        payload = start.call_args.args[2] if start.called else None
        return response, json.loads(response.body), payload

    def test_an_array_body_reaches_blocks_as_body(self):
        """A list body, as HubSpot webhooks send, failed with a 500 (upstream #5414)."""
        _, answer, payload = self.trigger(json.dumps([{'id': 1}, {'id': 2}]))

        self.assertEqual(answer, {'pipeline_run': {'id': 1}})
        self.assertEqual(payload['event_variables'], {'body': [{'id': 1}, {'id': 2}]})
        self.assertEqual(payload['variables'], {})

    def test_an_object_body_gives_variables_as_before(self):
        _, _, payload = self.trigger(json.dumps({
            'pipeline_run': {'variables': {'target': 'staging'}}, 'source': 'crm',
        }))

        self.assertEqual(payload['variables'], {'target': 'staging'})
        self.assertEqual(payload['event_variables'], {'source': 'crm'})

    def test_a_body_that_is_not_json_is_refused_with_the_reason(self):
        _, answer, payload = self.trigger('{not json')

        self.assertIsNone(payload)
        self.assertEqual(answer['error']['code'], 400)
        self.assertIn('not JSON', answer['error']['message'])

    def test_a_wrong_token_starts_no_run(self):
        _, _, payload = self.trigger('{}', token='other')

        self.assertIsNone(payload)
