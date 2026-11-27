"""
Change previews (ADR-0044): the blocks whose code changed since a completed run, and those
after them, run again on that run's stored inputs in a partition of their own, and their
outputs are compared with the run's. Exporters never run.
"""
import textwrap
from pathlib import Path
from unittest.mock import patch

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import previews
from mage_ai.orchestration.fusion_verify import run_in_process
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import DBTestCase

LOAD = '''
import pandas as pd


@data_loader
def load(*args, **kwargs):
    return pd.DataFrame({'id': range(5), 'value': [i * 10 for i in range(5)]})
'''

TRANSFORM = '''
@transformer
def transform(frame, *args, **kwargs):
    frame['double'] = frame['value'] * {multiplier}
    return frame[frame['id'] < {rows}]
'''

EXPORT = '''
@data_exporter
def export(frame, *args, **kwargs):
    raise Exception('a preview ran an exporter')
'''


class ChangePreviewTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        uuid = self.pipeline.uuid
        load = Block.create(f'{uuid}_load', 'data_loader', self.repo_path, pipeline=self.pipeline)
        Path(load.file_path).write_text(textwrap.dedent(LOAD))
        transform = Block.create(
            f'{uuid}_transform', 'transformer', self.repo_path, pipeline=self.pipeline,
            upstream_block_uuids=[load.uuid],
        )
        self.transform_path = Path(transform.file_path)
        self.set_transform(multiplier=2, rows=5)
        export = Block.create(
            f'{uuid}_export', 'data_exporter', self.repo_path, pipeline=self.pipeline,
            upstream_block_uuids=[transform.uuid],
        )
        Path(export.file_path).write_text(textwrap.dedent(EXPORT.replace('raise', 'return')))
        self.export_path = Path(export.file_path)
        self.load_uuid, self.transform_uuid, self.export_uuid = load.uuid, transform.uuid, \
            export.uuid
        self.pipeline = Pipeline.get(uuid, repo_path=self.repo_path)

    def set_transform(self, multiplier, rows):
        self.transform_path.write_text(
            textwrap.dedent(TRANSFORM.format(multiplier=multiplier, rows=rows)),
        )

    def baseline(self):
        run = trigger_pipeline(self.pipeline.uuid, variables={})
        run = run_in_process(run, log=lambda _line: None)
        self.assertEqual(run.status, 'completed')
        return run

    def preview(self, **kwargs):
        created = previews.create(self.pipeline, **kwargs)
        return previews.run(
            Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path), created['id'],
            log=lambda _line: None,
        )

    def test_a_changed_transformer_reruns_on_the_baseline_inputs_and_its_rows_compare(self):
        baseline = self.baseline()
        # Doubles become triples, and the last row is dropped; the exporter now raises.
        self.set_transform(multiplier=3, rows=4)
        self.export_path.write_text(textwrap.dedent(EXPORT))

        preview = self.preview()

        self.assertEqual(preview['state'], 'done', preview.get('error'))
        self.assertEqual(preview['baseline_run_id'], baseline.id)
        rows = {r['block_uuid']: r for r in preview['blocks']}
        self.assertEqual(rows[self.load_uuid]['status'], 'unchanged')
        self.assertEqual(rows[self.export_uuid]['status'], 'not run')
        self.assertIn('never run exporters', rows[self.export_uuid]['reason'])
        transform = rows[self.transform_uuid]
        self.assertEqual(transform['status'], 'previewed', transform.get('error'))
        self.assertIn('its code changed', transform['reason'])
        diff = transform['outputs']['output_0']['diff']
        self.assertEqual(transform['outputs']['output_0']['result'], 'differs')
        # Keyed by id: row 4 removed; rows 1 to 3 have a new double; row 0 is 0 either way.
        self.assertEqual(diff['keys'], ['id'])
        self.assertEqual(
            (diff['added'], diff['removed'], diff['changed'], diff['unchanged']), (0, 1, 3, 1),
        )
        self.assertEqual(diff['columns'], {'double': 3})
        self.assertEqual(diff['samples']['removed'], [{'id': 4}])
        # The baseline's outputs are untouched.
        stored = self.pipeline.variable_manager.get_variable(
            self.pipeline.uuid, self.transform_uuid, 'output_0',
            partition=baseline.execution_partition,
        )
        self.assertEqual(list(stored['double']), [0, 20, 40, 60, 80])
        self.assertEqual(previews.listed(self.pipeline)[0]['state'], 'done')

    def test_the_same_code_previews_nothing_and_a_failing_block_says_why(self):
        self.baseline()
        preview = self.preview()
        self.assertEqual(
            {r['status'] for r in preview['blocks']}, {'unchanged'},
        )
        self.transform_path.write_text(textwrap.dedent('''
            @transformer
            def transform(frame, *args, **kwargs):
                return frame['missing']
        '''))
        preview = self.preview()
        transform = {r['block_uuid']: r for r in preview['blocks']}[self.transform_uuid]
        self.assertEqual(transform['status'], 'failed')
        self.assertIn("KeyError: 'missing'", transform['error'])

    def test_a_pipeline_without_a_completed_run_cannot_be_previewed(self):
        with self.assertRaisesRegex(previews.PreviewError, 'no completed run'):
            previews.create(self.pipeline)

    def test_the_command_prints_each_block_and_its_rows(self):
        from typer.testing import CliRunner

        from mage_ai.cli.main import app

        self.baseline()
        self.set_transform(multiplier=3, rows=4)
        with patch('mage_ai.cli.main._run_records_setup', return_value=self.pipeline):
            result = CliRunner().invoke(
                app, ['preview', self.repo_path, self.pipeline.uuid,
                      '--key', f'{self.transform_uuid}=id'],
            )
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn(f'{self.transform_uuid}  previewed: its code changed', result.output)
        self.assertIn('0 added, 1 removed, 3 changed (double 3), 1 unchanged, by id',
                      result.output)
        self.assertIn('writes to a destination; previews never run exporters', result.output)
