"""
Stored outputs of editor and mage run runs are cleaned too (I-104): clean-cached-variables
only removed scheduled runs' old partitions, so outputs CLI runs stored (with the data they
read) stayed on disk.
"""
import os
import textwrap
import time
from pathlib import Path

from mage_ai.data_preparation.executors.pipeline_executor import PipelineExecutor
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.data_preparation.models.variable import VARIABLE_DIR
from mage_ai.data_preparation.variable_manager import VariableManager
from mage_ai.tests.base_test import DBTestCase


class VariableCleanupTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)
        block = Block.create(f'{self.pipeline.uuid}_load', 'data_loader', self.repo_path,
                             language='python')
        Path(block.file_path).write_text(textwrap.dedent('''
            import pandas as pd
            @data_loader
            def load(**kwargs):
                return pd.DataFrame({'email': ['a@example.com']})
        '''))
        self.pipeline.add_block(block)
        self.block = block
        self.manager = VariableManager.get_manager(self.repo_path, self.pipeline.variables_dir)
        self.variables = os.path.join(self.manager.pipeline_path(self.pipeline.uuid), VARIABLE_DIR)

    def run_cli(self, partition=None):
        PipelineExecutor(self.pipeline, execution_partition=partition).execute(
            global_vars={}, update_status=False,
        )

    def entries(self):
        return sorted(os.listdir(self.variables)) if os.path.exists(self.variables) else []

    def clean(self, **kwargs):
        return self.manager.clean_variables(pipeline_uuid=self.pipeline.uuid, **kwargs)

    def test_older_than_zero_removes_cli_outputs_and_a_dry_run_removes_nothing(self):
        self.run_cli()
        self.run_cli(partition='cli_42')
        self.assertEqual(self.entries(), sorted([self.block.uuid, 'cli_42']))
        report = self.clean(older_than='0d', dry_run=True)
        self.assertEqual(len(report['removed']), 2)
        self.assertGreater(report['bytes_removed'], 0)
        self.assertEqual(self.entries(), sorted([self.block.uuid, 'cli_42']))
        report = self.clean(older_than='0d')
        self.assertEqual(report['errors'], [])
        self.assertEqual(self.entries(), [])

    def test_a_duration_keeps_recent_outputs(self):
        self.run_cli()
        self.run_cli(partition='old_run')
        old = time.time() - 10 * 24 * 3600
        for root, _, files in os.walk(os.path.join(self.variables, 'old_run')):
            for name in files:
                os.utime(os.path.join(root, name), (old, old))
        self.clean(older_than='7d')
        self.assertEqual(self.entries(), [self.block.uuid])

    def test_without_a_retention_period_nothing_is_removed_and_the_size_is_reported(self):
        self.run_cli()
        report = self.clean()
        self.assertEqual(report['removed'], [])
        self.assertEqual(len(report['kept']), 1)
        self.assertGreater(report['bytes_kept'], 0)

    def test_scheduled_partitions_older_than_the_limit_are_removed(self):
        for partition in ('20200101T000000', '29990101T000000'):
            os.makedirs(os.path.join(self.variables, '7', partition, 'block'))
        self.clean(older_than='1d')
        self.assertEqual(os.listdir(os.path.join(self.variables, '7')), ['29990101T000000'])

    def test_a_run_removes_the_outputs_it_stored(self):
        self.run_cli(partition='cli_1')
        self.run_cli()
        removed = self.manager.clean_run_outputs(
            self.pipeline.uuid, [self.block.uuid], 'cli_1',
        )
        self.assertEqual(len(removed), 1)
        self.assertEqual(self.entries(), [self.block.uuid])
        self.manager.clean_run_outputs(self.pipeline.uuid, [self.block.uuid])
        self.assertEqual(self.entries(), [])
