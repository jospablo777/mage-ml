"""mage_ai.run, the Python API that the Airflow helper and the Prefect guide call."""
import json
import os

import mage_ai
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.tests.base_test import TestCase


class RunTest(TestCase):
    def pipeline(self, name: str, variables=None) -> str:
        pipeline = Pipeline.create(name, repo_path=self.repo_path)
        block = Block.create(f'{name}_load', 'data_loader', self.repo_path, language='python')
        with open(block.file_path, 'w') as file:
            file.write('''
import json


@data_loader
def load(*args, **kwargs):
    with open(kwargs['out'], 'w') as file:
        json.dump({k: kwargs.get(k) for k in ('region', 'days')}, file)
    return {}
''')
        pipeline.add_block(block)
        if variables:
            pipeline.variables = variables
            pipeline.save()
        return block.uuid

    def test_the_pipelines_variables_reach_its_blocks_under_the_given_ones(self):
        out = os.path.join(self.repo_path, 'run_variables.json')
        block_uuid = self.pipeline('run_variables', dict(region='north', days=7, out=out))
        for kwargs in (dict(), dict(block_uuid=block_uuid)):
            with self.subTest(**kwargs):
                mage_ai.run('run_variables', self.repo_path, days=30, **kwargs)
                with open(out) as file:
                    self.assertEqual(json.load(file), dict(region='north', days=30))

    def test_a_pipeline_without_variables_gets_the_given_ones(self):
        out = os.path.join(self.repo_path, 'run_plain.json')
        self.pipeline('run_plain')
        mage_ai.run('run_plain', self.repo_path, out=out, region='south')
        with open(out) as file:
            self.assertEqual(json.load(file), dict(region='south', days=None))
