"""Monitors through the API: the list with each latest result, and one monitor's history."""
import textwrap
from pathlib import Path

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration.fusion_verify import run_in_process
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user

LOAD = '''
import pandas as pd


@data_loader
def load(*args, **kwargs):
    return pd.DataFrame({'x': range(int(kwargs.get('rows', 100)))})
'''


class MonitorResourceTest(AsyncDBTestCase):
    async def test_monitors_list_their_latest_result_and_history(self):
        pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        block = Block.create(f'{pipeline.uuid}_load', 'data_loader', self.repo_path,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(LOAD))
        folder = Path(self.repo_path, 'monitors')
        folder.mkdir(exist_ok=True)
        (folder / f'{pipeline.uuid}.yaml').write_text(
            f'pipeline: {pipeline.uuid}\nblock: {block.uuid}\nrows: {{min: 50}}\n',
        )
        # A broken monitor of the same block runs no checks and does not stop the others.
        (folder / f'{pipeline.uuid}_broken.yaml').write_text(f'pipeline: {pipeline.uuid}\n')
        self.addCleanup(lambda: [p.unlink() for p in folder.glob(f'{pipeline.uuid}*.yaml')])
        for rows in (100, 10):
            run_in_process(trigger_pipeline(pipeline.uuid, variables=dict(rows=rows)),
                           log=lambda _l: None)
        owner = create_user(_owner=True)

        listed = {m['name']: m for m in (await BaseOperation(
            action=OperationType.LIST, resource='monitors', user=owner,
        ).execute())['monitors']}
        mine = listed[pipeline.uuid]
        self.assertEqual((mine['runs'], mine['latest']['result']), (2, 'fail'))
        self.assertIn('needs a block', listed[f'{pipeline.uuid}_broken']['error'])

        detail = (await BaseOperation(
            action=OperationType.DETAIL, pk=pipeline.uuid, resource='monitors', user=owner,
        ).execute())['monitor']
        rows = [next(c for c in h['checks'] if c['check'] == 'rows') for h in detail['history']]
        self.assertEqual([(c['value'], c['result']) for c in rows], [(100, 'pass'), (10, 'fail')])

    async def test_an_editor_silences_a_monitor_and_a_viewer_cannot(self):
        pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        block = Block.create(f'{pipeline.uuid}_load', 'data_loader', self.repo_path,
                             pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(LOAD))
        folder = Path(self.repo_path, 'monitors')
        folder.mkdir(exist_ok=True)
        path = folder / f'{pipeline.uuid}.yaml'
        path.write_text(f'pipeline: {pipeline.uuid}\nblock: {block.uuid}\n')
        self.addCleanup(path.unlink)
        owner = create_user(_owner=True)

        updated = (await BaseOperation(
            action=OperationType.UPDATE, pk=pipeline.uuid, resource='monitors', user=owner,
            payload=dict(monitor=dict(silence=dict(hours=2, reason='known outage'))),
        ).execute())['monitor']
        self.assertEqual(updated['silence']['reason'], 'known outage')
        detail = (await BaseOperation(
            action=OperationType.DETAIL, pk=pipeline.uuid, resource='monitors', user=owner,
        ).execute())['monitor']
        self.assertEqual(detail['silence']['until'], updated['silence']['until'])

        # A user who is no editor is refused.
        refused = await BaseOperation(
            action=OperationType.UPDATE, pk=pipeline.uuid, resource='monitors',
            user=create_user(), payload=dict(monitor=dict(silence=None)),
        ).execute()
        self.assertEqual(refused['error']['code'], 403)

        cleared = (await BaseOperation(
            action=OperationType.UPDATE, pk=pipeline.uuid, resource='monitors', user=owner,
            payload=dict(monitor=dict(silence=None)),
        ).execute())['monitor']
        self.assertIsNone(cleared['silence'])
