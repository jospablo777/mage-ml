"""Handlers outside the API resources answer only signed-in users when authentication is on."""
import json
from unittest.mock import patch

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from mage_ai.orchestration.db.models.schedules import EventMatcher
from mage_ai.server.api.blocks import ApiPipelineBlockAnalysisHandler
from mage_ai.server.api.events import (
    ApiEventHandler,
    ApiEventMatcherDetailHandler,
    ApiEventMatcherListHandler,
)
from mage_ai.tests.base_test import DBTestCase


class LegacyHandlerAuthTest(AsyncHTTPTestCase, DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)
        AsyncHTTPTestCase.setUp(self)
        for target in ('mage_ai.settings.REQUIRE_USER_AUTHENTICATION',
                       'mage_ai.api.middleware.REQUIRE_USER_AUTHENTICATION'):
            patcher = patch(target, True)
            patcher.start()
            self.addCleanup(patcher.stop)

    def get_app(self):
        return Application([
            (r'/api/events', ApiEventHandler),
            (r'/api/event_matchers', ApiEventMatcherListHandler),
            (r'/api/event_matchers/(?P<event_matcher_id>\w+)', ApiEventMatcherDetailHandler),
            (
                r'/api/pipelines/(?P<pipeline_uuid>\w+)/blocks/(?P<block_uuid>[\w\-\%2f\.]+)'
                r'/analyses',
                ApiPipelineBlockAnalysisHandler,
            ),
        ])

    def test_events_and_event_matchers_need_a_signed_in_user(self):
        matcher = EventMatcher.create(name='orders_landed', pattern={'source': ['shop']})
        with patch('mage_ai.server.api.events.EventTrigger') as trigger:
            response = self.fetch('/api/events', method='POST',
                                  body=json.dumps({'source': 'shop'}))
        self.assertEqual(response.code, 401)
        trigger.assert_not_called()

        self.assertEqual(self.fetch('/api/event_matchers').code, 401)
        created = self.fetch('/api/event_matchers', method='POST', body=json.dumps(
            {'event_matcher': {'name': 'any', 'pattern': {}}}))
        self.assertEqual(created.code, 401)
        self.assertEqual(EventMatcher.query.count(), 1)
        deleted = self.fetch(f'/api/event_matchers/{matcher.id}', method='DELETE')
        self.assertEqual(deleted.code, 401)
        self.assertIsNotNone(EventMatcher.get_by_id(matcher.id))

    def test_block_output_statistics_need_a_signed_in_user(self):
        response = self.fetch('/api/pipelines/orders/blocks/load_orders/analyses')
        self.assertEqual(response.code, 401)
        self.assertNotIn(b'analyses', response.body)

    def test_without_authentication_the_handlers_answer(self):
        with patch('mage_ai.settings.REQUIRE_USER_AUTHENTICATION', False), \
                patch('mage_ai.api.middleware.REQUIRE_USER_AUTHENTICATION', False):
            self.assertEqual(self.fetch('/api/event_matchers').code, 200)

    def test_viewers_read_and_only_editors_change(self):
        """A signed-in viewer triggered event pipelines and changed event matchers."""
        from mage_ai.api.middleware import OAuthMiddleware
        from mage_ai.orchestration.db.models.oauth import Role
        from mage_ai.tests.factory import create_user

        Role.create_default_roles()
        viewer = create_user(roles_new=[Role.get_role('Viewer')])
        editor = create_user(roles_new=[Role.get_role('Editor')])
        matcher = EventMatcher.create(name='orders_landed', pattern={'source': ['shop']})

        def as_user(user):
            def prepare(handler):
                handler.request.current_user = user
                handler.request.error = None
            return patch.object(OAuthMiddleware, 'prepare', prepare)

        with patch('mage_ai.api.utils.REQUIRE_USER_AUTHENTICATION', True), \
                patch('mage_ai.server.api.events.EventTrigger') as trigger:
            with as_user(viewer):
                self.assertEqual(self.fetch('/api/event_matchers').code, 200)
                event = self.fetch('/api/events', method='POST',
                                   body=json.dumps({'source': 'shop'}))
                self.assertEqual(event.code, 403)
                trigger.assert_not_called()
                changed = self.fetch(f'/api/event_matchers/{matcher.id}', method='PUT',
                                     body=json.dumps({'event_matcher': {'name': 'taken'}}))
                self.assertEqual(changed.code, 403)
            with as_user(editor):
                changed = self.fetch(f'/api/event_matchers/{matcher.id}', method='PUT',
                                     body=json.dumps({'event_matcher': {
                                         'name': 'renamed', 'id': 999,
                                     }}))
                self.assertEqual(changed.code, 200, changed.body)
        found = EventMatcher.get_by_id(matcher.id)
        # Its own fields only: the id stays.
        self.assertEqual(found.name, 'renamed')

    def test_block_statistics_need_access_to_the_pipeline(self):
        """Signed in was enough: a user with no role read any pipeline's statistics."""
        from mage_ai.api.middleware import OAuthMiddleware
        from mage_ai.tests.factory import create_user

        nobody = create_user()

        def prepare(handler):
            handler.request.current_user = nobody
            handler.request.error = None

        with patch('mage_ai.api.utils.REQUIRE_USER_AUTHENTICATION', True), \
                patch.object(OAuthMiddleware, 'prepare', prepare):
            response = self.fetch('/api/pipelines/orders/blocks/load_orders/analyses')
        self.assertEqual(response.code, 403)
        self.assertNotIn(b'analyses', response.body)


class EventStreamAuthTest(AsyncHTTPTestCase, DBTestCase):
    """The editor kernel's execution results answered anyone who could reach the server."""

    def setUp(self):
        DBTestCase.setUp(self)
        AsyncHTTPTestCase.setUp(self)
        for target in ('mage_ai.settings.REQUIRE_USER_AUTHENTICATION',
                       'mage_ai.api.middleware.REQUIRE_USER_AUTHENTICATION',
                       'mage_ai.api.utils.REQUIRE_USER_AUTHENTICATION'):
            patcher = patch(target, True)
            patcher.start()
            self.addCleanup(patcher.stop)

    def get_app(self):
        from mage_ai.server.events.stream import EventStreamHandler

        return Application([(r'/event-streams/(?P<uuid>[\w\-\%2f\.]+)', EventStreamHandler)])

    def test_only_an_editor_reads_the_stream(self):
        from mage_ai.api.middleware import OAuthMiddleware
        from mage_ai.orchestration.db.models.oauth import Role
        from mage_ai.tests.factory import create_user

        self.assertEqual(self.fetch('/event-streams/abc').code, 401)

        Role.create_default_roles()
        viewer = create_user(roles_new=[Role.get_role('Viewer')])

        def prepare(handler):
            handler.request.current_user = viewer
            handler.request.error = None

        with patch.object(OAuthMiddleware, 'prepare', prepare):
            self.assertEqual(self.fetch('/event-streams/abc').code, 403)
