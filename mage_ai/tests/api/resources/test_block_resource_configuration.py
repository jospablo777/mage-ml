"""Block settings saved through the API: an integration pipeline transformer's records
per call, and trigger rules."""
from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.constants import PipelineType
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user


class RecordsPerCallTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        self.owner = create_user(_owner=True)
        self.pipeline = Pipeline.create(
            self.faker.unique.name(), pipeline_type=PipelineType.INTEGRATION,
            repo_path=self.repo_path,
        )
        self.block = Block.create(
            f'{self.pipeline.uuid}_shout', 'transformer', self.repo_path, pipeline=self.pipeline,
        )

    async def save(self, records_per_call):
        return await BaseOperation(
            action=OperationType.UPDATE,
            payload=dict(block=dict(configuration=dict(records_per_call=records_per_call))),
            pk=self.block.uuid,
            resource='blocks',
            resource_parent='pipelines',
            resource_parent_id=self.pipeline.uuid,
            user=self.owner,
        ).execute()

    def saved(self):
        pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        return pipeline.get_block(self.block.uuid).configuration.get('records_per_call')

    async def test_a_positive_whole_number_is_saved(self):
        response = await self.save(1)
        self.assertNotIn('error', response)
        self.assertEqual(self.saved(), 1)

        await self.save(None)
        self.assertIsNone(self.saved())

    async def test_other_values_are_refused(self):
        for value in (0, -5, 'many'):
            response = await self.save(value)
            self.assertEqual(response['error']['code'], 400, value)
            self.assertIn('positive whole number', response['error']['message'])
        self.assertIsNone(self.saved())


class TriggerRuleTest(AsyncDBTestCase):
    async def save(self, pipeline_type, language, trigger_rule):
        pipeline = Pipeline.create(
            self.faker.unique.name(), pipeline_type=pipeline_type, repo_path=self.repo_path,
        )
        block = Block.create(
            f'{pipeline.uuid}_step', 'transformer', self.repo_path, language=language,
            pipeline=pipeline,
        )
        return await BaseOperation(
            action=OperationType.UPDATE,
            payload=dict(block=dict(configuration=dict(trigger_rule=trigger_rule))),
            pk=block.uuid,
            resource='blocks',
            resource_parent='pipelines',
            resource_parent_id=pipeline.uuid,
            user=create_user(_owner=True),
        ).execute()

    async def test_a_rule_is_saved_where_it_applies(self):
        response = await self.save(PipelineType.PYTHON, 'python', 'all_done')
        self.assertNotIn('error', response)
        self.assertEqual(response['block']['configuration']['trigger_rule'], 'all_done')

    async def test_unknown_rules_and_blocks_it_does_not_apply_to_are_refused(self):
        for pipeline_type, language, rule, message in (
            (PipelineType.PYTHON, 'python', 'one_success', 'not a trigger rule'),
            (PipelineType.PYTHON, 'sql', 'all_done', 'Python blocks'),
            (PipelineType.STREAMING, 'python', 'all_done', 'batch pipelines'),
        ):
            response = await self.save(pipeline_type, language, rule)
            self.assertEqual(response['error']['code'], 400, rule)
            self.assertIn(message, response['error']['message'])
