"""
Run records: a triggered run records its code, environment and variables, block runs
record their output digests, two runs compare, and a run reproduces after its code
changed.
"""
import textwrap
from pathlib import Path

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import run_records
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.orchestration.fusion_verify import run_in_process
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import DBTestCase

LOAD = '''
import pandas as pd

from {project}.run_records_shared import FACTOR


@data_loader
def load(*args, **kwargs):
    rows = int(kwargs.get('rows', 3))
    return pd.DataFrame({{'id': range(rows), 'value': [i * FACTOR for i in range(rows)]}})
'''

TRANSFORM = '''
@transformer
def transform(frame, *args, **kwargs):
    frame['double'] = frame['value'] * {multiplier}
    return frame
'''


class RunRecordsTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.project = Path(self.repo_path)
        (self.project / 'run_records_shared.py').write_text('FACTOR = 10\n')
        self.addCleanup((self.project / 'run_records_shared.py').unlink)
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        load = Block.create(
            f'{self.pipeline.uuid}_load', 'data_loader', self.repo_path, pipeline=self.pipeline,
        )
        Path(load.file_path).write_text(textwrap.dedent(LOAD.format(project=self.project.name)))
        transform = Block.create(
            f'{self.pipeline.uuid}_transform', 'transformer', self.repo_path,
            pipeline=self.pipeline, upstream_block_uuids=[load.uuid],
        )
        self.transform_path = Path(transform.file_path)
        self.set_multiplier(2)
        self.load_uuid, self.transform_uuid = load.uuid, transform.uuid
        self.pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)

    def set_multiplier(self, multiplier):
        self.transform_path.write_text(textwrap.dedent(TRANSFORM.format(multiplier=multiplier)))

    def run_pipeline(self, **variables) -> PipelineRun:
        run = trigger_pipeline(self.pipeline.uuid, variables=variables)
        return run_in_process(run, log=lambda _line: None)

    def stored(self, run, block_uuid):
        return self.pipeline.variable_manager.get_variable(
            self.pipeline.uuid, block_uuid, 'output_0', partition=run.execution_partition,
        )

    def test_a_run_records_its_code_environment_variables_and_outputs(self):
        run = self.run_pipeline(rows=4)
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)

        recorded = run_records.manifest(self.pipeline, run.id)
        files = recorded['code']['files']
        relative = self.transform_path.relative_to(self.project).as_posix()
        self.assertIn(relative, files)
        self.assertIn('run_records_shared.py', files)
        self.assertIn(f'pipelines/{self.pipeline.uuid}/metadata.yaml', files)
        self.assertFalse(recorded['code']['truncated'])
        self.assertEqual(recorded['variables']['names'], ['rows'])
        self.assertEqual(run.metrics[run_records.METRIC]['code'], recorded['code']['digest'])
        environment = run_records.environment_document(
            self.pipeline, recorded['environment']['digest'],
        )
        self.assertIn('pandas', environment['packages'])
        self.assertEqual(environment['python'], recorded['environment']['python'])

        outputs = {b.block_uuid: (b.metrics or {}).get('outputs') for b in run.block_runs}
        self.assertEqual(set(outputs[self.transform_uuid]), {'output_0'})
        self.assertEqual(len(outputs[self.transform_uuid]['output_0']['sha256']), 64)
        self.assertGreater(outputs[self.transform_uuid]['output_0']['bytes'], 0)

    def test_two_runs_compare_by_code_variables_and_outputs(self):
        first = self.run_pipeline(rows=3)
        self.set_multiplier(3)
        second = self.run_pipeline(rows=3)
        third = self.run_pipeline(rows=5)

        comparison = run_records.compare(self.pipeline, first, second)
        relative = self.transform_path.relative_to(self.project).as_posix()
        self.assertEqual(comparison['code']['changed'], [relative])
        self.assertIn(
            "-    frame['double'] = frame['value'] * 2", comparison['code']['diffs'][relative],
        )
        self.assertTrue(comparison['same_environment'])
        results = {o['block_uuid']: o['result'] for o in comparison['outputs']}
        self.assertEqual(results, {self.load_uuid: 'same', self.transform_uuid: 'differs'})

        later = run_records.compare(self.pipeline, second, third)
        self.assertTrue(later['same_code'])
        self.assertEqual(later['variables']['changed'], ['rows'])
        text = run_records.format_comparison(later)
        self.assertIn('Variables changed: rows', text)
        self.assertIn(f'Output of {self.load_uuid}: differs', text)

    def test_a_run_reproduces_with_its_recorded_code(self):
        original = self.run_pipeline(rows=3)
        self.set_multiplier(7)
        (self.project / 'run_records_shared.py').write_text('FACTOR = 11\n')

        report = run_records.reproduce(self.pipeline, original, log=lambda _line: None)

        self.assertTrue(report['passed'], run_records.format_reproduction(report))
        self.assertTrue(report['code_restored'])
        self.assertEqual(
            {b['block_uuid']: b['result'] for b in report['blocks']},
            {self.load_uuid: 'same', self.transform_uuid: 'same'},
        )
        reproduced = PipelineRun.get(report['reproduced_run_id'])
        self.assertEqual(
            self.stored(reproduced, self.transform_uuid)['double'].tolist(), [0, 20, 40],
        )
        # The project keeps its current code.
        self.assertIn('* 7', self.transform_path.read_text())

    def test_runs_without_a_record_are_explained(self):
        run = self.run_pipeline()
        with self.assertRaises(run_records.RunRecordError) as caught:
            run_records.manifest(self.pipeline, run.id + 1000)
        self.assertIn('has no run record', str(caught.exception))
