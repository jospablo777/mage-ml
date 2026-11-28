"""
Change previews (ADR-0044): the blocks whose code changed since a completed run, and those
after them, run again on that run's stored inputs in a partition of their own, and their
outputs are compared with the run's. Exporters never run.
"""
import textwrap
from pathlib import Path
from types import SimpleNamespace
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

        # Both outputs open in ColumnAtlas: the run's by its block run, the preview's by id.
        from mage_ai.column_atlas.sources import parse_source, resolve

        base_source = parse_source(dict(
            pipeline_uuid=self.pipeline.uuid, block_uuid=self.transform_uuid,
            block_run_id=transform['baseline_block_run_id'],
        ))
        preview_source = parse_source(dict(
            pipeline_uuid=self.pipeline.uuid, block_uuid=self.transform_uuid,
            preview_id=preview['id'],
        ))
        self.assertNotEqual(base_source.key, preview_source.key)
        self.assertNotEqual(resolve(base_source, self.repo_path).path,
                            resolve(preview_source, self.repo_path).path)
        self.assertIn(previews.partition(preview['id']),
                      resolve(preview_source, self.repo_path).path)

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

    def test_what_reads_a_change_that_was_not_run_says_so(self):
        def block(uuid, *upstream, language='python'):
            return SimpleNamespace(uuid=uuid, type='transformer', language=language,
                                   configuration={}, upstream_blocks=list(upstream))

        query = block('query', language='sql')
        clean = block('clean', query)
        edited = block('edited', query)
        report = block('report', clean)
        with patch.object(previews, '_ordered', return_value=[query, clean, edited, report]):
            rows = {r['block_uuid']: r for r in previews.plan(
                None, dict(query='its code changed', edited='its code changed'),
            )}
        self.assertEqual(rows['query']['status'], 'not run')
        self.assertEqual((rows['clean']['status'], rows['clean']['reason']),
                         ('not run', 'it reads query, which changed and was not previewed'))
        self.assertEqual(rows['report']['reason'],
                         'it reads clean, which changed and was not previewed')
        # Its own change still runs, on the baseline's output of what it reads.
        self.assertEqual(rows['edited']['status'], 'run')
        self.assertIn("reads query's output from the baseline run", rows['edited']['reason'])

    def test_a_preview_source_must_name_a_preview_of_the_pipeline(self):
        from mage_ai.column_atlas.errors import AtlasError
        from mage_ai.column_atlas.sources import parse_source, resolve

        for bad in ('../x', 'a b', 7):
            with self.assertRaises(AtlasError):
                parse_source(dict(pipeline_uuid='p', block_uuid='b', preview_id=bad))
        with self.assertRaises(AtlasError):
            parse_source(dict(pipeline_uuid='p', block_uuid='b', preview_id='a1',
                              block_run_id=3))
        with self.assertRaisesRegex(AtlasError, 'preview does not exist'):
            resolve(parse_source(dict(pipeline_uuid=self.pipeline.uuid,
                                      block_uuid=self.transform_uuid, preview_id='nope1')),
                    self.repo_path)

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
