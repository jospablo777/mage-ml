"""
Requests the API recording (scripts/contracts/record.py) found failing with a 500: a nested
resource called without its parent, and members that did not load.
"""
from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.api.resources.ConfigurationOptionResource import (
    ConfigurationOptionResource,
)
from mage_ai.api.resources.OauthAccessTokenResource import OauthAccessTokenResource
from mage_ai.orchestration.db.models.oauth import Oauth2AccessToken, Oauth2Application
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import (
    create_pipeline_run_with_schedule,
    create_pipeline_with_blocks,
    create_user,
)


class RecordedApiFixesTest(AsyncDBTestCase):
    async def call(self, action, resource, **kwargs):
        return await BaseOperation(
            action=action, resource=resource, user=create_user(_owner=True), **kwargs,
        ).execute()

    async def test_nested_resources_without_their_parent_name_the_route(self):
        for action, resource, payload, route in (
            (OperationType.LIST, 'widgets', None, '/api/pipelines/<uuid>/widgets'),
            (OperationType.LIST, 'pipeline_triggers', None,
             '/api/pipelines/<uuid>/pipeline_triggers'),
            (OperationType.LIST, 'variables', None, '/api/pipelines/<uuid>/variables'),
            (OperationType.CREATE, 'backfills', dict(backfill=dict(name='b')),
             '/api/pipelines/<uuid>/backfills'),
            (OperationType.CREATE, 'pipeline_schedules',
             dict(pipeline_schedule=dict(name='t')), '/api/pipelines/<uuid>/pipeline_schedules'),
            (OperationType.LIST, 'version_control_branches', None,
             '/api/version_control_projects/<uuid>/version_control_branches'),
            (OperationType.LIST, 'version_control_files', None,
             '/api/version_control_projects/<uuid>/version_control_files'),
            (OperationType.LIST, 'version_control_remotes', None,
             '/api/version_control_projects/<uuid>/version_control_remotes'),
        ):
            with self.subTest(resource=resource, action=action):
                response = await self.call(action, resource, payload=payload or {})
                error = response['error']
                self.assertEqual(error['code'], 400)
                self.assertIn(route, error['message'])
                self.assertEqual(error['type'], 'parent_required')

    async def test_configuration_options_of_a_type_without_options_are_empty(self):
        response = await self.call(OperationType.LIST, 'configuration_options')
        self.assertEqual(response.get('configuration_options'), [])
        self.assertEqual(
            list(await ConfigurationOptionResource.collection(dict(
                configuration_type=['not_dbt'],
            ), {}, None)),
            [],
        )

    def test_access_tokens_without_a_user_are_the_tokens_that_have_none(self):
        application = Oauth2Application.create(client_id='contracts', name='contracts')
        owned = create_user()
        Oauth2AccessToken.create(oauth2_application_id=application.id, token='owned',
                                 user_id=owned.id)
        Oauth2AccessToken.create(oauth2_application_id=application.id, token='nobody')

        tokens = OauthAccessTokenResource.collection(dict(show_all=['1']), {}, None)

        self.assertEqual([token.token for token in tokens], ['nobody'])

    async def test_an_execution_framework_loads_by_its_uuid(self):
        response = await self.call(OperationType.DETAIL, 'execution_frameworks', pk='rag')
        self.assertNotIn('error', response)
        self.assertEqual(response['execution_framework']['uuid'], 'rag')

    async def test_a_runs_block_loads_by_its_uuid(self):
        pipeline = create_pipeline_with_blocks('recorded_api_run', self.repo_path)
        run = create_pipeline_run_with_schedule(pipeline_uuid=pipeline.uuid)

        response = await self.call(
            OperationType.DETAIL, 'blocks', pk='block1',
            resource_parent='pipeline_runs', resource_parent_id=run.id,
        )
        self.assertNotIn('error', response)
        self.assertEqual(response['block']['uuid'], 'block1')

        missing = await self.call(
            OperationType.DETAIL, 'blocks', pk='not_a_block',
            resource_parent='pipeline_runs', resource_parent_id=run.id,
        )
        self.assertEqual(
            missing['error']['message'], f'Block not_a_block is not in pipeline run {run.id}.',
        )
