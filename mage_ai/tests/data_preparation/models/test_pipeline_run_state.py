"""
metadata.yaml holds the pipeline's definition, not run state or machine paths (I-107): it
carried each block's status and all_upstream_blocks_executed, which changed with every
run, and the project's variables_dir, a path such as /Users/<name>/.mage_data/...
"""
from pathlib import Path

import yaml

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.constants import BlockStatus
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.tests.base_test import DBTestCase


class PipelineRunStateTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)
        self.block = Block.create(f'{self.pipeline.uuid}_load', 'data_loader', self.repo_path)
        self.pipeline.add_block(self.block)

    def metadata(self):
        return yaml.safe_load(Path(self.pipeline.config_path).read_text())

    def test_saved_metadata_has_no_run_state_or_machine_paths(self):
        self.pipeline.get_block(self.block.uuid).status = BlockStatus.EXECUTED
        self.pipeline.save()
        data = self.metadata()
        self.assertNotIn('variables_dir', data)
        self.assertNotIn('remote_variables_dir', data)
        for block in data['blocks']:
            self.assertNotIn('status', block)
            self.assertNotIn('all_upstream_blocks_executed', block)
        self.assertNotIn(self.repo_path, Path(self.pipeline.config_path).read_text())

    def test_a_block_keeps_its_status_on_this_machine(self):
        self.pipeline.get_block(self.block.uuid).status = BlockStatus.EXECUTED
        self.pipeline.save()
        again = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        self.assertEqual(again.get_block(self.block.uuid).status, BlockStatus.EXECUTED)

    def test_older_metadata_with_status_still_loads_it(self):
        data = self.metadata()
        data['blocks'][0]['status'] = 'failed'
        Path(self.pipeline.config_path).write_text(yaml.dump(data))
        again = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        self.assertEqual(again.get_block(self.block.uuid).status, BlockStatus.FAILED)
