import shutil
import textwrap
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import compilation
from mage_ai.tests.base_test import DBTestCase

TRANSFORM = '''
use mage::prelude::*;

fn transform(rows: LazyFrame) -> Result<LazyFrame> {
    Ok(rows.with_column((col("a") * lit(2)).alias("b")))
}
'''
TESTED = TRANSFORM + '''
fn test_rows(output: &DataFrame) -> Result<()> {
    ensure!(output.height() > 0, "no rows");
    Ok(())
}
'''
LOAD = '''
use mage::prelude::*;

fn load_data() -> Result<DataFrame> {
    Ok(df!("a" => [1i64, 2, 3])?)
}
'''


def built(pipeline, unit):
    unit.state = 'compiled'


class CompilationTest(DBTestCase):
    def setUp(self):
        super().setUp()
        compilation._compilations.clear()

    def pipeline(self, blocks):
        pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(pipeline.delete)
        upstream = []
        for name, block_type, language, code in blocks:
            block = Block.create(f'{self._testMethodName}_{name}', block_type, self.repo_path,
                                 language=language)
            if code is not None:
                Path(block.file_path).write_text(textwrap.dedent(code))
            pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
            upstream = [block]
        return Pipeline.get(pipeline.uuid, repo_path=self.repo_path)

    def names(self, units):
        prefix = f'{self._testMethodName}_'
        return [(u.kind, [b.removeprefix(prefix) for b in u.block_uuids]) for u in units]

    def test_units_follow_how_runs_plan_their_chains(self):
        pipeline = self.pipeline([
            ('load', 'data_loader', 'rust', LOAD),
            ('tested', 'transformer', 'rust', TESTED),
            ('double', 'transformer', 'rust', TRANSFORM),
            ('again', 'transformer', 'rust', TRANSFORM),
            ('python', 'transformer', 'python', None),
        ])
        # A block with tests computes its output, so it ends the first chain.
        self.assertEqual(self.names(compilation.units_for(pipeline)), [
            ('python_check', ['python']),
            ('rust_chain', ['load', 'tested']),
            ('rust_chain', ['double', 'again']),
        ])
        summary = compilation.summary_for(pipeline)
        self.assertEqual(summary['languages'], ['python', 'rust'])
        self.assertEqual(summary['language_switches'], 1)
        self.assertTrue(summary['fusion'])

        pipeline.block_fusion = 'off'
        self.assertEqual(self.names(compilation.units_for(pipeline))[1:], [
            ('rust_block', [name]) for name in ('load', 'tested', 'double', 'again')
        ])

    def test_sql_blocks_are_not_compiled(self):
        pipeline = self.pipeline([
            ('sql', 'data_loader', 'sql', 'SELECT 1 AS a'),
            ('notes', 'markdown', 'markdown', '# Notes'),
        ])
        self.assertEqual(compilation.units_for(pipeline), [])
        self.assertEqual(compilation.start(pipeline).state, 'nothing')
        self.assertEqual(compilation.summary_for(pipeline)['languages'], ['sql'])

    def test_a_polyglot_pipeline_compiles_every_language(self):
        pipeline = self.pipeline([
            ('load', 'data_loader', 'python', None),
            ('double', 'transformer', 'rust', TRANSFORM),
            ('again', 'transformer', 'rust', TRANSFORM),
            ('score', 'transformer', 'r', None),
            ('save', 'data_exporter', 'sql', 'SELECT * FROM {{ df_1 }}'),
        ])
        self.assertEqual(self.names(compilation.units_for(pipeline)), [
            ('python_check', ['load']),
            ('r_check', ['score']),
            ('rust_chain', ['double', 'again']),
            ('r_environment', []),
        ])
        summary = compilation.summary_for(pipeline)
        self.assertEqual(summary['languages'], ['python', 'r', 'rust', 'sql'])
        self.assertEqual(summary['language_switches'], 3)
        self.assertEqual(summary['flow'], [
            dict(language='python', blocks=1), dict(language='rust', blocks=2),
            dict(language='r', blocks=1), dict(language='sql', blocks=1),
        ])

    def test_a_python_syntax_error_fails_with_the_block_and_line(self):
        pipeline = self.pipeline([
            ('good', 'data_loader', 'python', None),
            ('bad', 'transformer', 'python', '''
                from mage_ai.data_preparation.decorators import transformer

                @transformer
                def transform(df, *args, **kwargs):
                    return df[
            '''),
        ])
        result = compilation.start(pipeline, wait=True)
        self.assertEqual(result.state, 'failed')
        error = result.units[0].error
        self.assertIn(f'{self._testMethodName}_bad, line ', error)
        self.assertNotIn(f'{self._testMethodName}_good', error)

    def test_a_python_block_without_its_decorator_is_a_warning(self):
        pipeline = self.pipeline([
            ('plain', 'transformer', 'python', '''
                def transform(df, *args, **kwargs):
                    return df
            '''),
        ])
        result = compilation.start(pipeline, wait=True)
        self.assertEqual(result.state, 'completed')
        self.assertEqual(result.units[0].state, 'checked')
        self.assertIn('no function decorated with @transformer', result.units[0].warnings[0])

    @unittest.skipUnless(shutil.which('Rscript'), 'needs R')
    def test_an_r_parse_error_fails_with_the_block(self):
        pipeline = self.pipeline([
            ('good', 'data_loader', 'r', None),
            ('bad', 'transformer', 'r', '''
                transform <- function(df_1, ...) {
                  dplyr::mutate(df_1, x = (1 + )
                }
            '''),
        ])
        unit = compilation.units_for(pipeline)[0]
        self.assertEqual(unit.kind, 'r_check')
        with self.assertRaises(compilation.CompileError) as raised:
            compilation._check_r(pipeline, unit)
        message = str(raised.exception)
        self.assertIn(f'{self._testMethodName}_bad: line', message)
        self.assertIn('unexpected', message)
        self.assertNotIn(f'{self._testMethodName}_good', message)

    @unittest.skipUnless(shutil.which('cargo'), 'needs cargo')
    def test_compiling_builds_the_chains_and_then_reuses_them(self):
        pipeline = self.pipeline([
            ('load', 'data_loader', 'rust', LOAD),
            ('double', 'transformer', 'rust', TRANSFORM),
        ])
        first = compilation.start(pipeline, wait=True)
        self.assertEqual(first.state, 'completed', [u.error for u in first.units])
        self.assertIn(first.units[0].state, ('compiled', 'cached'))
        again = compilation.start(pipeline, wait=True)
        self.assertEqual([u.state for u in again.units], ['cached'])
        report = again.to_dict()
        self.assertEqual((report['done'], report['total']), (1, 1))

    @unittest.skipUnless(shutil.which('cargo'), 'needs cargo')
    def test_a_block_that_does_not_compile_fails_its_unit_with_the_error(self):
        pipeline = self.pipeline([
            ('load', 'data_loader', 'rust', LOAD),
            ('broken', 'transformer', 'rust', TRANSFORM.replace('lit(2)', 'lit(2')),
        ])
        result = compilation.start(pipeline, wait=True)
        self.assertEqual(result.state, 'failed')
        self.assertIn('does not compile', result.units[0].error)
        self.assertIn(f'{self._testMethodName}_broken.rs', result.units[0].error)

    def test_last_compiled_holds_until_the_code_changes(self):
        pipeline = self.pipeline([
            ('load', 'data_loader', 'rust', LOAD),
            ('double', 'transformer', 'rust', TRANSFORM),
        ])
        self.assertIsNone(compilation.last_compiled(pipeline))
        self.assertEqual(compilation.status(pipeline)['state'], 'none')
        with patch.object(compilation, '_compile_unit', built):
            compilation.start(pipeline, wait=True)
        last = compilation.last_compiled(pipeline)
        self.assertEqual((last['state'], last['up_to_date'], last['units']),
                         ('completed', True, 1))
        self.assertEqual(compilation.status(pipeline)['units'][0]['state'], 'compiled')

        # The record outlives the server.
        compilation._compilations.clear()
        self.assertEqual(compilation.status(pipeline)['state'], 'completed')

        block = pipeline.get_block(f'{self._testMethodName}_double')
        Path(block.file_path).write_text(TRANSFORM.replace('lit(2)', 'lit(3)'))
        self.assertFalse(compilation.last_compiled(pipeline)['up_to_date'])
        current = compilation.status(pipeline)
        self.assertEqual(current['state'], 'none')
        self.assertEqual(current['units'][0]['state'], 'pending')
        self.assertFalse(current['last_compiled']['up_to_date'])

    def test_a_failed_compilation_keeps_its_error_while_the_code_is_the_same(self):
        pipeline = self.pipeline([('load', 'data_loader', 'rust', LOAD)])

        def fails(pipeline, unit):
            raise RuntimeError('block load does not compile: expected `;`')

        with patch.object(compilation, '_compile_unit', fails):
            compilation.start(pipeline, wait=True)
        current = compilation.status(pipeline)
        self.assertEqual(current['state'], 'failed')
        self.assertIn('does not compile', current['units'][0]['error'])
        self.assertEqual(current['last_compiled']['state'], 'failed')
        self.assertFalse(current['last_compiled']['up_to_date'])

    def test_a_save_while_compiling_compiles_again_after(self):
        pipeline = self.pipeline([('load', 'data_loader', 'rust', LOAD)])
        release = threading.Event()

        def slow(pipeline, unit):
            release.wait(10)
            unit.state = 'compiled'

        with patch.object(compilation, '_compile_unit', slow), \
                patch.object(compilation, 'request') as request:
            running = compilation.start(pipeline)
            self.assertEqual(running.state, 'running')
            # The same code joins the running compilation.
            self.assertIs(compilation.start(pipeline), running)
            self.assertFalse(running.rerun)
            block = pipeline.get_block(f'{self._testMethodName}_load')
            Path(block.file_path).write_text(LOAD.replace('3]', '4]'))
            self.assertIs(compilation.start(pipeline), running)
            self.assertTrue(running.rerun)
            release.set()
            for _ in range(100):
                if running.state != 'running':
                    break
                time.sleep(0.05)
        self.assertEqual(running.state, 'completed')
        request.assert_called_once_with(pipeline.uuid, pipeline.repo_path)
        # The record is of the code it compiled, which has changed since.
        self.assertFalse(compilation.last_compiled(pipeline)['up_to_date'])

    def test_saves_in_a_row_compile_once(self):
        pipeline = self.pipeline([('load', 'data_loader', 'rust', LOAD)])
        with patch.object(compilation, 'DEBOUNCE_SECONDS', 0.2), \
                patch.object(compilation, 'start') as start:
            for _ in range(5):
                compilation.request_for(pipeline)
            time.sleep(0.6)
        self.assertEqual(start.call_count, 1)
        self.assertTrue(start.call_args.kwargs['automatic'])

    def test_saving_a_pipeline_of_sql_blocks_does_not_compile(self):
        pipeline = self.pipeline([('sql', 'data_loader', 'sql', 'SELECT 1 AS a')])
        with patch.object(compilation, 'request') as request:
            compilation.request_for(pipeline)
        request.assert_not_called()
        self.assertEqual(compilation.status(pipeline)['state'], 'nothing')

    def test_the_crates_compiling_adds_to_cargo_lock_do_not_outdate_it(self):
        from mage_ai.data_preparation.models.block.rust import workspace as ws

        pipeline = self.pipeline([('load', 'data_loader', 'rust', LOAD)])
        lock = ws.workspace_root(pipeline.repo_path) / 'Cargo.lock'
        lock.parent.mkdir(parents=True, exist_ok=True)
        lock.write_text('version = 4\n')
        self.addCleanup(lock.unlink, missing_ok=True)

        def adds_its_crate(pipeline, unit):
            # What cargo does on a block's first build.
            lock.write_text(
                'version = 4\n\n[[package]]\nname = "block_chain_ab12"\nversion = "0.1.0"\n',
            )
            unit.state = 'compiled'

        with patch.object(compilation, '_compile_unit', adds_its_crate):
            compilation.start(pipeline, wait=True)
        self.assertTrue(compilation.last_compiled(pipeline)['up_to_date'])

        # A new dependency is a change the blocks compile against.
        lock.write_text(lock.read_text() + '\n[[package]]\nname = "regex"\nversion = "1.11.0"\n'
                        'source = "registry+https://github.com/rust-lang/crates.io-index"\n')
        self.assertFalse(compilation.last_compiled(pipeline)['up_to_date'])

    def test_server_start_compiles_active_pipelines_that_are_not_compiled(self):
        from mage_ai.data_preparation.models.triggers import ScheduleStatus
        from mage_ai.orchestration.db.models.schedules import PipelineSchedule

        active = self.pipeline([('load', 'data_loader', 'rust', LOAD)])
        PipelineSchedule.create(
            name='hourly', pipeline_uuid=active.uuid, repo_path=self.repo_path,
            schedule_type='time', schedule_interval='@hourly',
            status=ScheduleStatus.ACTIVE,
        )
        inactive = Pipeline.create(f'{self._testMethodName}_off', repo_path=self.repo_path)
        self.addCleanup(inactive.delete)
        PipelineSchedule.create(
            name='off', pipeline_uuid=inactive.uuid, repo_path=self.repo_path,
            schedule_type='time', schedule_interval='@hourly',
            status=ScheduleStatus.INACTIVE,
        )
        with patch.object(compilation, '_compile_unit', built):
            compilation.compile_active_pipelines(self.repo_path).join(30)
            self.assertTrue(compilation.last_compiled(active)['up_to_date'])
            self.assertIsNone(compilation.last_compiled(inactive))
            # A compiled pipeline is left alone the next time.
            with patch.object(compilation, 'start') as start:
                compilation.compile_active_pipelines(self.repo_path).join(30)
            start.assert_not_called()

        with patch.dict('os.environ', {'MAGE_COMPILE_ON_START': 'off'}):
            self.assertIsNone(compilation.compile_active_pipelines(self.repo_path))
