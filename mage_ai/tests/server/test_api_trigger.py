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

    def trigger(self, body, token='secret', skip=False, running=0):
        from mage_ai.data_preparation.models.triggers import SettingsConfig

        schedule = SimpleNamespace(
            schedule_type=ScheduleType.API, token='secret', pipeline_uuid='p', repo_path='r',
            get_settings=lambda: SettingsConfig(skip_if_previous_running=skip),
            running_pipeline_run_count=running, initial_pipeline_runs=[],
        )
        run = SimpleNamespace(to_dict=lambda: {'id': 1})
        self.cancelled = None
        with patch('mage_ai.server.api.triggers.PipelineSchedule.get_by_id',
                   return_value=schedule), \
                patch('mage_ai.server.api.triggers.Pipeline.get'), \
                patch('mage_ai.server.api.triggers.create_and_cancel_pipeline_run',
                      return_value=SimpleNamespace(to_dict=lambda: {'id': 2})) as cancel, \
                patch('mage_ai.server.api.triggers.create_and_start_pipeline_run',
                      return_value=run) as start:
            response = self.fetch(
                '/api/pipeline_schedules/1/api_trigger', method='POST', body=body,
                headers={'Authorization': f'Bearer {token}'},
            )
        self.cancelled = cancel.called
        payload = start.call_args.args[2] if start.called else None
        return response, json.loads(response.body), payload

    def test_a_trigger_that_skips_while_a_run_runs_starts_no_run(self):
        """Skip if the previous run is still running applied to schedules only (PR 5961)."""
        _, answer, payload = self.trigger('{}', skip=True, running=1)

        self.assertIsNone(payload)
        self.assertTrue(self.cancelled)
        self.assertEqual(answer, {'pipeline_run': {'id': 2}})

        _, _, payload = self.trigger('{}', skip=True, running=0)
        self.assertIsNotNone(payload)
        self.assertFalse(self.cancelled)

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

    def test_a_caller_sets_variables_and_the_execution_date_only(self):
        """Every key of pipeline_run reached PipelineRun.create: id, status, backfill."""
        _, _, payload = self.trigger(json.dumps({'pipeline_run': {
            'variables': {'target': 'staging'},
            'execution_date': '2026-01-01T00:00:00',
            'id': 7, 'status': 'completed', 'backfill_id': 3, 'metrics': {'x': 1},
        }}))

        self.assertEqual(
            {k: v for k, v in payload.items() if k != 'event_variables'},
            {'variables': {'target': 'staging'}, 'execution_date': '2026-01-01T00:00:00'},
        )

    def test_a_pipeline_run_that_is_not_an_object_is_refused(self):
        for body in ({'pipeline_run': [1]}, {'pipeline_run': {'variables': [1]}}):
            with self.subTest(body=body):
                _, answer, payload = self.trigger(json.dumps(body))
                self.assertIsNone(payload)
                self.assertEqual(answer['error']['code'], 400)

    def test_a_body_that_is_not_json_is_refused_with_the_reason(self):
        _, answer, payload = self.trigger('{not json')

        self.assertIsNone(payload)
        self.assertEqual(answer['error']['code'], 400)
        self.assertIn('not JSON', answer['error']['message'])

    def test_a_wrong_token_starts_no_run(self):
        _, _, payload = self.trigger('{}', token='other')

        self.assertIsNone(payload)
