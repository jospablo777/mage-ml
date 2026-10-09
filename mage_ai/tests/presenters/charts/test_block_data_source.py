from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.presenters.charts.data_sources.block import ChartDataSourceBlock
from mage_ai.tests.base_test import DBTestCase


class ChartDataSourceBlockTest(DBTestCase):
    def test_each_partition_reads_its_own_output(self):
        pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(pipeline.delete)
        block = Block.create(f'{pipeline.uuid}_load', 'data_loader', self.repo_path)
        pipeline.add_block(block)
        for partition, value in [(None, 'notebook'), ('run_a', 'a'), ('run_b', 'b')]:
            pipeline.variable_manager.add_variable(
                pipeline.uuid, block.uuid, 'output_0', dict(value=value), partition=partition,
            )

        data = ChartDataSourceBlock(
            block_uuid=block.uuid, pipeline_uuid=pipeline.uuid,
        ).load_data(partitions=['run_a', 'run_b'])

        self.assertEqual(data, [dict(value='a'), dict(value='b')])
