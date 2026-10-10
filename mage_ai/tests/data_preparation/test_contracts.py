"""
Data contracts: a block's output checked against a contract file with its tests, in the
notebook and in triggered runs, where a broken contract stops the downstream blocks.
"""
import contextlib
import io
import textwrap
from pathlib import Path
from unittest.mock import patch

import yaml

from mage_ai.data_preparation import contracts
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.tests.base_test import DBTestCase

CONTRACT = '''
version: 1.2.0
owner: ml-engineering
columns:
  customer_id: {type: integer, nullable: false}
  score: {type: float, min: 0, max: 1}
  segment: {type: string, values: [new, active]}
unique: [customer_id]
'''

LOAD = '''
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(*args, **kwargs):
    scores = kwargs.get('scores') or [0.1, 0.5, 0.9]
    return pd.DataFrame({
        'customer_id': pd.Series(range(1, len(scores) + 1), dtype='Int64'),
        'score': scores,
        'segment': ['new', 'active', 'new'][:len(scores)],
    })
'''

LOAD_POLARS = '''
import polars as pl


@data_loader
def load(*args, **kwargs):
    return pl.DataFrame({'customer_id': [1, 1], 'score': [0.2, 0.3], 'segment': ['new'] * 2})
'''

EXPORT = '''
from pathlib import Path


@data_exporter
def export(frame, *args, **kwargs):
    Path(kwargs['marker']).write_text('exported')
'''


class DataContractTest(DBTestCase):
    def setUp(self):
        super().setUp()
        folder = Path(self.repo_path) / contracts.CONTRACTS_FOLDER
        folder.mkdir(exist_ok=True)
        (folder / 'customer_scores.yaml').write_text(CONTRACT)
        self.addCleanup(lambda: (folder / 'customer_scores.yaml').unlink(missing_ok=True))
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)

    def block(self, name, kind, code, contract=None, upstream=()):
        block = Block.create(
            f'{self.pipeline.uuid}_{name}', kind, self.repo_path, pipeline=self.pipeline,
            upstream_block_uuids=[b.uuid for b in upstream],
            configuration=dict(contract=contract) if contract is not None else None,
        )
        Path(block.file_path).write_text(textwrap.dedent(code))
        self.pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        return self.pipeline.get_block(block.uuid)

    def run_in_notebook(self, block, **variables):
        block.execute_sync(global_vars=variables)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            try:
                block.run_tests(global_vars=variables, update_tests=False)
                error = None
            except Exception as caught:
                error = caught
        return stdout.getvalue(), error

    def test_a_broken_contract_fails_the_block_and_lists_every_rule(self):
        block = self.block('load', 'data_loader', LOAD, contract='customer_scores')

        printed, error = self.run_in_notebook(block, scores=[0.1, 1.4, -2.0])

        self.assertIn('broke contract customer_scores', str(error))
        self.assertIn('Contract customer_scores 1.2.0 on output_0: 2 rules broken', printed)
        self.assertIn('score has values above the max 1 (1 of 3 rows); values: 1.4; rows: 1',
                      printed)
        self.assertIn('score has values below the min 0 (1 of 3 rows)', printed)
        report = contracts.read_report(block)
        self.assertFalse(report['passed'])
        self.assertEqual(report['rows'], 3)
        self.assertEqual(report['enforcement'], 'fail')

    def test_a_matching_output_passes_and_warn_only_reports(self):
        block = self.block('load', 'data_loader', LOAD, contract='customer_scores')
        printed, error = self.run_in_notebook(block)
        self.assertIsNone(error)
        self.assertIn('passed (3 rows checked)', printed)

        block.configuration = dict(contract=dict(name='customer_scores', enforcement='warn'))
        printed, error = self.run_in_notebook(block, scores=[0.1, 3.0, 0.2])
        self.assertIsNone(error)
        self.assertIn('WARNING: Contract customer_scores', printed)

        block.configuration = dict(contract=dict(name='customer_scores', enforcement='off'))
        printed, error = self.run_in_notebook(block, scores=[0.1, 3.0, 0.2])
        self.assertIsNone(error)
        self.assertNotIn('Contract', printed)

    def test_polars_outputs_are_checked_with_their_keys(self):
        block = self.block('load', 'data_loader', LOAD_POLARS, contract='customer_scores')
        printed, error = self.run_in_notebook(block)
        self.assertIsNotNone(error)
        self.assertIn('customer_id has values repeated (2 of 2 rows); values: 1; rows: 0, 1',
                      printed)

    def test_a_block_written_for_another_major_version_is_stopped(self):
        block = self.block(
            'load', 'data_loader', LOAD,
            contract=dict(name='customer_scores', version='2'),
        )
        printed, error = self.run_in_notebook(block)
        self.assertIn('expects version 2 of contract customer_scores', str(error))

        block.configuration = dict(contract=dict(name='customer_scores', version='1'))
        _, error = self.run_in_notebook(block)
        self.assertIsNone(error)

    def test_settings_and_contract_files_are_explained(self):
        block = self.block('load', 'data_loader', LOAD, contract='missing_contract')
        _, error = self.run_in_notebook(block)
        self.assertIn('create contracts/missing_contract.yaml', str(error))

        block.configuration = dict(contract=dict(name='customer_scores', enforcment='warn'))
        with self.assertRaises(contracts.ContractError) as caught:
            contracts.binding_of(block)
        self.assertIn('unknown settings: enforcment', str(caught.exception))

        Path(self.repo_path, 'contracts', 'typo.yaml').write_text(
            'columns:\n  a: {type: integer, nulable: false}\n',
        )
        self.addCleanup(Path(self.repo_path, 'contracts', 'typo.yaml').unlink)
        block.configuration = dict(contract='typo')
        _, error = self.run_in_notebook(block)
        self.assertIn('unknown field `nulable`', str(error))
        listed = {c['name']: c for c in contracts.list_contracts(self.repo_path)}
        self.assertEqual(listed['customer_scores']['version'], '1.2.0')
        self.assertIn('nulable', listed['typo']['error'])

    def test_a_draft_comes_from_the_stored_output(self):
        block = self.block('load', 'data_loader', LOAD)
        block.execute_sync()

        document = yaml.safe_load(contracts.draft(block, 'drafted'))

        self.assertEqual(document['name'], 'drafted')
        self.assertEqual(document['columns']['customer_id'], dict(type='integer', nullable=False))
        self.assertEqual(document['columns']['segment'], dict(type='string', nullable=False))

    def test_a_broken_contract_stops_the_exporter_in_a_triggered_run(self):
        from mage_ai.orchestration import pipeline_scheduler_original
        from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun
        from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
        from mage_ai.orchestration.triggers.api import trigger_pipeline

        load = self.block('load', 'data_loader', LOAD, contract='customer_scores')
        exported = self.block('export', 'data_exporter', EXPORT, upstream=[load])
        marker = Path(self.repo_path) / f'{self.pipeline.uuid}.marker'

        run = trigger_pipeline(
            self.pipeline.uuid, variables=dict(scores=[0.1, 7.0, 0.3], marker=str(marker)),
        )
        job_manager = pipeline_scheduler_original.get_job_manager()
        pending = []

        def add_job(job_type, uid, target, *args, **kwargs):
            pending.append((target, args, kwargs))

        with patch.object(job_manager, 'add_job', side_effect=add_job), \
                patch.object(job_manager, 'has_block_run_job', return_value=True):
            for _ in range(10):
                run.refresh()
                scheduler = PipelineScheduler(run)
                if run.status == PipelineRun.PipelineRunStatus.INITIAL:
                    scheduler.start(should_schedule=False)
                scheduler.schedule()
                run.refresh()
                if run.status not in (
                    PipelineRun.PipelineRunStatus.INITIAL, PipelineRun.PipelineRunStatus.RUNNING,
                ) or not pending:
                    break
                while pending:
                    target, args, kwargs = pending.pop(0)
                    try:
                        target(*args, **kwargs)
                    except Exception:
                        pass

        statuses = {b.block_uuid: b.status for b in BlockRun.query.filter(
            BlockRun.pipeline_run_id == run.id,
        )}
        self.assertEqual(statuses[load.uuid], BlockRun.BlockRunStatus.FAILED)
        self.assertNotEqual(statuses[exported.uuid], BlockRun.BlockRunStatus.COMPLETED)
        self.assertFalse(marker.exists())
        report = contracts.read_report(load, execution_partition=run.execution_partition)
        self.assertEqual(report['violations'][0]['rule'], 'max')

    def test_the_cli_drafts_a_contract_and_checks_an_output(self):
        from typer.testing import CliRunner

        from mage_ai.cli.main import app

        block = self.block('load', 'data_loader', LOAD)
        block.execute_sync(global_vars=dict(scores=[0.1, 4.0, 0.3]))
        runner = CliRunner()

        drafted = runner.invoke(app, [
            'contract-draft', self.repo_path, self.pipeline.uuid, block.uuid,
            '--name', 'drafted_scores', '--write',
        ])
        self.assertEqual(drafted.exit_code, 0, drafted.output)
        path = Path(self.repo_path, 'contracts', 'drafted_scores.yaml')
        self.addCleanup(path.unlink, missing_ok=True)
        self.assertIn('customer_id', path.read_text())
        again = runner.invoke(app, [
            'contract-draft', self.repo_path, self.pipeline.uuid, block.uuid,
            '--name', 'drafted_scores', '--write',
        ])
        self.assertEqual(again.exit_code, 2)
        self.assertIn('--force', again.output)

        block.configuration = dict(contract='customer_scores')
        self.pipeline.save()
        checked = runner.invoke(app, ['contract-check', self.repo_path, self.pipeline.uuid,
                                      block.uuid])
        self.assertEqual(checked.exit_code, 1, checked.output)
        self.assertIn('score has values above the max 1', checked.output)

    def test_a_sql_block_output_is_checked(self):
        database = Path(self.repo_path) / f'{self.pipeline.uuid}.duckdb'
        io_config = Path(self.repo_path) / 'io_config.yaml'
        previous = io_config.read_text() if io_config.exists() else None
        io_config.write_text(f'contracts_duckdb:\n  DUCKDB_DATABASE: {database}\n')
        self.addCleanup(
            lambda: io_config.write_text(previous) if previous is not None
            else io_config.unlink(),
        )
        load = self.block('load', 'data_loader', LOAD)
        block = Block.create(
            f'{self.pipeline.uuid}_sql', 'transformer', self.repo_path, pipeline=self.pipeline,
            language='sql', upstream_block_uuids=[load.uuid],
            configuration=dict(
                contract='customer_scores',
                data_provider='duckdb',
                data_provider_profile='contracts_duckdb',
                data_provider_schema='main',
                export_write_policy='replace',
            ),
        )
        Path(block.file_path).write_text(
            'SELECT customer_id, score * 10 AS score, segment FROM {{ df_1 }}',
        )
        self.pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        load = self.pipeline.get_block(load.uuid)
        block = self.pipeline.get_block(block.uuid)
        load.execute_sync()

        printed, error = self.run_in_notebook(block)

        self.assertIn('broke contract customer_scores', str(error), printed)
        self.assertIn('score has values above the max 1 (2 of 3 rows)', printed)
