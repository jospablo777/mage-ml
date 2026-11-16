"""
The project's pipeline dependencies through the API: after-run triggers, global data products
read by a block, and blocks that call trigger_pipeline, each with the pipelines' last runs.
"""
import os
from pathlib import Path

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.global_data_product import GlobalDataProduct
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration.db.models.schedules import PipelineRun, PipelineSchedule
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user

STARTS_REPORT = '''
from mage_ai.orchestration.triggers.api import trigger_pipeline


@data_exporter
def start(*args, **kwargs):
    trigger_pipeline('{report}', variables=dict(day=kwargs['ds']))
'''


class PipelineDependencyResourceTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        self.names = {}
        for name in ('orders', 'cleanup', 'features', 'report', 'alone'):
            uuid = f'{name}_{self.faker.unique.pyint()}'
            self.names[name] = Pipeline.create(uuid, repo_path=self.repo_path).uuid

        # cleanup runs after orders fails.
        PipelineSchedule.create(
            name='after orders', pipeline_uuid=self.names['cleanup'],
            schedule_type='api', status='active', repo_path=self.repo_path,
            settings=dict(after_pipeline=self.names['orders'], after_status='failed'),
        )
        # features reads the product of orders.
        product = GlobalDataProduct(
            object_type='pipeline', object_uuid=self.names['orders'],
            repo_path=self.repo_path, uuid=f'orders_product_{self.faker.unique.pyint()}',
        )
        product.save()
        self.addCleanup(os.remove, GlobalDataProduct.file_path(self.repo_path))
        features = Pipeline.get(self.names['features'], repo_path=self.repo_path)
        Block.create(
            f'{features.uuid}_orders', 'global_data_product', self.repo_path,
            pipeline=features, configuration=dict(global_data_product=dict(uuid=product.uuid)),
        )
        # features starts report from a block.
        features = Pipeline.get(self.names['features'], repo_path=self.repo_path)
        starter = Block.create(
            f'{features.uuid}_start', 'data_exporter', self.repo_path, pipeline=features,
        )
        Path(starter.file_path).write_text(STARTS_REPORT.format(report=self.names['report']))
        PipelineRun.create(pipeline_uuid=self.names['orders'], status='failed')
        self.product = product

    async def test_the_dependencies_and_last_runs(self):
        response = await BaseOperation(
            action=OperationType.DETAIL, pk='project', resource='pipeline_dependencies',
            user=create_user(_owner=True),
        ).execute()
        graph = response['pipeline_dependency']
        mine = set(self.names.values())
        edges = {
            (e['source'], e['target'], e['kind'], e['label'])
            for e in graph['edges'] if e['source'] in mine or e['target'] in mine
        }
        n = self.names

        self.assertEqual(edges, {
            (n['orders'], n['cleanup'], 'after_run', f"after {n['orders']} fails"),
            (n['orders'], n['features'], 'data_product', f'reads product {self.product.uuid}'),
            (n['features'], n['report'], 'api', f"{n['features']}_start starts it"),
        })
        nodes = {node['uuid']: node for node in graph['nodes']}
        self.assertNotIn(n['alone'], nodes)
        self.assertEqual(nodes[n['orders']]['last_run']['status'], 'failed')
        self.assertIsNone(nodes[n['report']]['last_run'])
        self.assertGreaterEqual(graph['pipelines_without_dependencies'], 1)
