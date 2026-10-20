"""
`mage run` records its run when it ends (cli_runs.py, I-103): a finished pipeline run of
the "Command line" trigger, with its block runs and run record, which the scheduler leaves
alone.
"""
import textwrap
from pathlib import Path

from mage_ai.data_preparation.executors.pipeline_executor import PipelineExecutor
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import cli_runs, run_records
from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun
from mage_ai.tests.base_test import DBTestCase


class CliRunsTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)

    def block(self, name, kind, code, upstream=()):
        block = Block.create(
            f'{self.pipeline.uuid}_{name}', kind, self.repo_path, language='python',
        )
        Path(block.file_path).write_text(textwrap.dedent(code))
        self.pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    def flow(self, fail: bool):
        load = self.block('load', 'data_loader', '''
            @data_loader
            def load(**kwargs):
                return {'value': 1}
        ''')
        self.block('check', 'transformer', f'''
            @transformer
            def check(data, **kwargs):
                if {fail!r}:
                    raise ValueError('the check failed')
                return data
        ''', upstream=[load])
        self.pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)

    def run_cli(self):
        try:
            PipelineExecutor(self.pipeline).execute(
                global_vars={'region': 'north'}, record_run=True, update_status=False,
            )
        except Exception as error:
            return error
        return None

    def recorded(self) -> PipelineRun:
        runs = PipelineRun.query.filter(
            PipelineRun.pipeline_uuid == self.pipeline.uuid,
        ).all()
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0].pipeline_schedule.name, cli_runs.TRIGGER_NAME)
        self.assertEqual(runs[0].pipeline_schedule.status, 'inactive')
        return runs[0]

    def test_a_run_is_recorded_with_its_blocks_and_run_record(self):
        self.flow(fail=False)
        self.assertIsNone(self.run_cli())
        run = self.recorded()
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        self.assertEqual(run.variables['region'], 'north')
        statuses = {b.block_uuid.split('_')[-1]: b for b in run.block_runs}
        self.assertEqual({k: b.status for k, b in statuses.items()}, {
            'load': BlockRun.BlockRunStatus.COMPLETED,
            'check': BlockRun.BlockRunStatus.COMPLETED,
        })
        self.assertIn('outputs', statuses['load'].metrics)
        self.assertLessEqual(statuses['load'].started_at, statuses['load'].completed_at)
        manifest = run_records.manifest(self.pipeline, run.id)
        self.assertEqual(manifest['pipeline_run_id'], run.id)
        self.assertEqual(manifest['variables']['names'], ['region'])

    def test_a_failed_run_is_recorded_as_failed(self):
        self.flow(fail=True)
        self.assertIsNotNone(self.run_cli())
        run = self.recorded()
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.FAILED)
        statuses = {b.block_uuid.split('_')[-1]: b.status for b in run.block_runs}
        self.assertEqual(statuses['check'], BlockRun.BlockRunStatus.FAILED)

    def test_the_scheduler_does_not_run_a_recorded_run_again(self):
        from mage_ai.orchestration.pipeline_scheduler_original import schedule_all

        self.flow(fail=False)
        self.run_cli()
        run = self.recorded()
        schedule_all()
        run.refresh()
        self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        self.assertTrue(all(
            b.status == BlockRun.BlockRunStatus.COMPLETED for b in run.block_runs
        ))
