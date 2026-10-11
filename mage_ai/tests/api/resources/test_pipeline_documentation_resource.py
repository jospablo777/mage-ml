"""Read and save a pipeline's README through the API."""
from pathlib import Path

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user


class PipelineDocumentationResourceTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        self.owner = create_user(_owner=True)

    async def detail(self):
        return (await BaseOperation(
            action=OperationType.DETAIL, pk=self.pipeline.uuid,
            resource='pipeline_documentations', user=self.owner,
        ).execute())

    async def save(self, **payload):
        return (await BaseOperation(
            action=OperationType.UPDATE, pk=self.pipeline.uuid,
            payload=dict(pipeline_documentation=payload),
            resource='pipeline_documentations', user=self.owner,
        ).execute())

    async def test_a_readme_is_read_and_saved(self):
        empty = (await self.detail())['pipeline_documentation']
        self.assertFalse(empty['exists'])
        self.assertIn('## Inputs', empty['template'])

        saved = (await self.save(source='# Sales\n', format='markdown'))['pipeline_documentation']
        self.assertEqual((saved['exists'], saved['format'], saved['source']),
                         (True, 'markdown', '# Sales\n'))
        self.assertEqual((Path(self.pipeline.dir_path) / 'README.md').read_text(), '# Sales\n')

        detail = (await BaseOperation(
            action=OperationType.DETAIL, pk=self.pipeline.uuid, resource='pipelines',
            user=self.owner,
        ).execute())['pipeline']
        self.assertTrue(detail['has_documentation'])

    async def test_an_invalid_save_is_explained(self):
        response = await self.save(source='# x', format='html')
        self.assertEqual(response['error']['code'], 400)
        self.assertIn('format must be', response['error']['message'])
        response = await self.save(format='markdown')
        self.assertIn('source must be', response['error']['message'])

    async def test_an_unknown_pipeline_is_not_found(self):
        response = await BaseOperation(
            action=OperationType.DETAIL, pk='no_such_pipeline',
            resource='pipeline_documentations', user=self.owner,
        ).execute()
        self.assertEqual(response['error']['code'], 404)
