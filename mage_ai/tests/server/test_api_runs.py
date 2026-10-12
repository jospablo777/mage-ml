"""What a call to an API trigger's run URL answers when its token is missing or unknown."""
import json

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from mage_ai.server.api.runs import ApiRunHandler
from mage_ai.tests.base_test import DBTestCase


class ApiRunTokenTest(AsyncHTTPTestCase, DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)
        AsyncHTTPTestCase.setUp(self)

    def get_app(self):
        return Application([
            (r'/api/runs', ApiRunHandler),
            (r'/api/runs/(?P<token>\w+)', ApiRunHandler),
        ])

    def post(self, path):
        response = self.fetch(path, method='POST', body=json.dumps({'run': {}}))
        return json.loads(response.body)['error']

    def test_an_unknown_token_says_where_to_find_the_right_one(self):
        error = self.post('/api/runs/ffffffffffffffff')

        self.assertIn('No API trigger has this token', error['exception'])
        self.assertNotIn('NoResultFound', error['message'])

    def test_a_missing_token_says_how_to_send_one(self):
        error = self.post('/api/runs')

        self.assertIn('Authorization: Bearer <token>', error['exception'])

    def test_a_valid_token_reaches_the_pipeline(self):
        from mage_ai.data_preparation.models.triggers import (
            ScheduleStatus,
            ScheduleType,
        )
        from mage_ai.orchestration.db.models.schedules import PipelineSchedule

        PipelineSchedule.create(
            name='api', pipeline_uuid='orders', schedule_type=ScheduleType.API,
            status=ScheduleStatus.ACTIVE, token='0123456789abcdef',
        )

        response = self.fetch('/api/runs/0123456789abcdef', method='POST', body=json.dumps(
            {'run': {'pipeline_uuid': 'no_such_pipeline', 'block_uuid': 'load'}}))

        # The token was found; before, every call failed on a loader option.
        error = json.loads(response.body)['error']
        self.assertIn('Pipeline no_such_pipeline does not exist', error['exception'])
