"""
The Services page's model promotion (item 82): a service's models in the registry, and a
promotion recorded with the signed-in user, as mage service promote does it.
"""
from unittest.mock import patch

from mage_ai.api.operations import constants
from mage_ai.orchestration.db.models.oauth import Role
from mage_ai.tests.api.operations.test_base import BaseApiTestCase
from mage_ai.tests.factory import create_user

STATUS = [dict(name='churn', uri='models:/churn@champion', built='3', registry='5',
               status='moved', challengers=[])]


@patch('mage_ai.api.policies.BasePolicy.REQUIRE_USER_AUTHENTICATION', 1)
class PipelineServiceModelTests(BaseApiTestCase):
    def setUp(self):
        super().setUp()
        Role.create_default_roles()
        self.viewer = create_user(roles_new=[Role.get_role('Viewer')])
        self.editor = create_user(roles_new=[Role.get_role('Editor')])

    async def call(self, action, user, pk=None, **payload):
        return await self.build_operation(
            action=action, resource='pipeline_service_models', pk=pk, user=user,
            payload=dict(pipeline_service_model=payload) if payload else {},
        ).execute()

    @patch('mage_ai.pipeline_services.lifecycle.promotions', return_value=[])
    @patch('mage_ai.pipeline_services.lifecycle.models_status', return_value=STATUS)
    async def test_a_viewer_sees_the_models_and_an_editor_promotes(self, _status, _history):
        found = await self.call(constants.DETAIL, self.viewer, pk='scoring')
        self.assertNotIn('error', found, found)
        self.assertEqual(found['pipeline_service_model']['models'], STATUS)

        refused = await self.call(
            constants.CREATE, self.viewer, name='scoring', model='churn',
        )
        self.assertEqual(refused['error']['code'], 403, refused)

        record = dict(model='churn', from_version='3', to_version='5', how='alias followed')
        with patch('mage_ai.pipeline_services.lifecycle.promote',
                   return_value=record) as promote:
            promoted = await self.call(
                constants.CREATE, self.editor, name='scoring', model='churn',
                reason='better recall',
            )
        self.assertNotIn('error', promoted, promoted)
        self.assertEqual(promoted['pipeline_service_model']['promoted'], record)
        kwargs = promote.call_args.kwargs
        self.assertEqual(kwargs['by'], self.editor.email)
        self.assertEqual(kwargs['reason'], 'better recall')
        self.assertIsNone(kwargs['challenger'])

    async def test_a_refused_promotion_says_why(self):
        from mage_ai.pipeline_services.lifecycle import PromoteError

        with patch('mage_ai.pipeline_services.lifecycle.promote',
                   side_effect=PromoteError('Service scoring already embeds version 5.')):
            response = await self.call(
                constants.CREATE, self.editor, name='scoring', model='churn',
            )
        self.assertEqual(response['error']['code'], 400)
        self.assertIn('already embeds', response['error']['message'])
