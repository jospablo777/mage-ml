"""
Data contracts through the API: the list, a draft from a block's stored output saved as a
contract file, and a contract with the block's last report.
"""
import textwrap
from pathlib import Path

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user

LOAD = '''
import pandas as pd


@data_loader
def load(*args, **kwargs):
    return pd.DataFrame({'id': [1, 2, 2], 'amount': [1.5, None, 3.0]})
'''


class DataContractResourceTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        self.owner = create_user(_owner=True)
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        block = Block.create(
            f'{self.pipeline.uuid}_load', 'data_loader', self.repo_path, pipeline=self.pipeline,
        )
        Path(block.file_path).write_text(textwrap.dedent(LOAD))
        self.pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        self.block = self.pipeline.get_block(block.uuid)
        self.folder = Path(self.repo_path) / 'contracts'
        self.addCleanup(lambda: [p.unlink() for p in self.folder.glob('*.yaml')])

    async def call(self, action, **options):
        return await BaseOperation(
            action=action, resource='data_contracts', user=self.owner, **options,
        ).execute()

    async def test_a_contract_is_drafted_listed_and_shown_with_its_report(self):
        self.block.execute_sync()
        name = f'{self.pipeline.uuid}_amounts'

        created = (await self.call(
            OperationType.CREATE,
            payload=dict(data_contract=dict(
                block_uuid=self.block.uuid, name=name, pipeline_uuid=self.pipeline.uuid,
            )),
        ))['data_contract']
        self.assertEqual(created['file_path'], f'contracts/{name}.yaml')
        self.assertIn('amount:', created['content'])

        again = await self.call(
            OperationType.CREATE,
            payload=dict(data_contract=dict(
                block_uuid=self.block.uuid, name=name, pipeline_uuid=self.pipeline.uuid,
            )),
        )
        self.assertIn('exists already', again['error']['message'])

        listed = (await self.call(OperationType.LIST))['data_contracts']
        self.assertIn(name, [c['name'] for c in listed])

        (self.folder / f'{name}.yaml').write_text('columns:\n  id: {type: integer, unique: true}\n')
        self.block.configuration = dict(contract=dict(name=name, enforcement='warn'))
        self.block.run_tests(update_tests=False)
        shown = (await self.call(
            OperationType.DETAIL,
            pk=name,
            query=dict(pipeline_uuid=[self.pipeline.uuid], block_uuid=[self.block.uuid]),
        ))['data_contract']
        self.assertFalse(shown['report']['passed'])
        self.assertEqual(shown['report']['violations'][0]['rule'], 'unique')
        self.assertEqual(shown['report']['enforcement'], 'warn')
