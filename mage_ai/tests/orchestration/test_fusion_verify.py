import math
import textwrap
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import polars as pl

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import fusion, fusion_verify
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.tests.base_test import DBTestCase, TestCase

LOAD = '''
    import pandas as pd
    @data_loader
    def load(**kwargs):
        return pd.DataFrame({
            'id': pd.Series([1, 2, 3], dtype='Int64'),
            'amount': [10.0, 20.0, 30.0],
        })
'''
CLEAN = '''
    @transformer
    def clean(frame, **kwargs):
        return frame[frame['amount'] > 10].reset_index(drop=True)
'''
TO_POLARS = '''
    import polars as pl
    @transformer
    def to_polars(frame, **kwargs):
        return pl.from_pandas(frame).with_columns(total=pl.col('amount') * 2)
'''
# State a block leaves in its process: a separate process per block does not see it, a
# stage does. Fusion restores environment variables, but not module attributes.
MARK = '''
    import json
    @transformer
    def mark(frame, **kwargs):
        json.mage_seen = 'stage'
        return frame
'''
READ_MARK = '''
    import json
    @transformer
    def read_mark(frame, **kwargs):
        return frame.assign(source=getattr(json, 'mage_seen', 'own process'))
'''
FAIL = '''
    @transformer
    def fail(frame, **kwargs):
        raise ValueError('broken on purpose')
'''


class VerifyFusionTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)
        self.logs = []

    def chain(self, *steps):
        upstream = []
        for name, kind, code in steps:
            block = Block.create(
                f'{self.pipeline.uuid}_{name}', kind, self.repo_path, language='python',
            )
            Path(block.file_path).write_text(textwrap.dedent(code))
            self.pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
            upstream = [block]
        return Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)

    def verify(self, pipeline):
        return fusion_verify.verify_fusion(pipeline, log=self.logs.append)

    def results(self, verification):
        return {
            b.block_uuid.rsplit('_', 1)[-1]: b.result for b in verification.blocks
        }

    def test_a_chain_with_the_same_outputs_is_verified(self):
        pipeline = self.chain(
            ('load', 'data_loader', LOAD),
            ('clean', 'transformer', CLEAN),
            ('polars', 'transformer', TO_POLARS),
        )

        verification = self.verify(pipeline)

        self.assertTrue(verification.passed, fusion_verify.format_report(verification))
        self.assertEqual(set(self.results(verification).values()), {'same'})
        self.assertEqual(len(verification.plan), 1)
        self.assertEqual(len(verification.plan[0]), 3)
        report = fusion_verify.format_report(verification)
        self.assertIn('Fusion verified', report)
        self.assertIn(f'Stage 1: {self.pipeline.uuid}_load -> ', report)
        self.assertEqual({b.stage for b in verification.blocks}, {1})
        # The pipeline's own setting stays off; each run carried its mode.
        self.assertIsNone(Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
                          .block_fusion)
        for mode, run_id in verification.runs.items():
            run = PipelineRun.get_by_id(run_id)
            self.assertEqual(run.metrics[fusion.VERIFY_FUSION_METRIC], mode)
            self.assertEqual(run.status, PipelineRun.PipelineRunStatus.COMPLETED)
        runs = [PipelineRun.get_by_id(i) for i in verification.runs.values()]
        self.assertEqual(runs[0].execution_date, runs[1].execution_date)
        self.assertNotEqual(runs[0].execution_partition, runs[1].execution_partition)

    def test_the_first_block_whose_output_differs_is_named(self):
        pipeline = self.chain(
            ('load', 'data_loader', LOAD),
            ('mark', 'transformer', MARK),
            ('readmark', 'transformer', READ_MARK),
        )

        verification = self.verify(pipeline)

        self.assertFalse(verification.passed)
        self.assertEqual(
            self.results(verification),
            {'load': 'same', 'mark': 'same', 'readmark': 'differs'},
        )
        first = verification.first_difference
        self.assertTrue(first.block_uuid.endswith('readmark'))
        self.assertIn('source', first.detail)
        report = fusion_verify.format_report(verification)
        self.assertIn(f'First difference: {first.block_uuid}', report)

    def test_a_block_that_fails_in_both_runs_fails_the_check(self):
        pipeline = self.chain(
            ('load', 'data_loader', LOAD),
            ('fail', 'transformer', FAIL),
        )

        verification = self.verify(pipeline)

        self.assertFalse(verification.passed)
        self.assertEqual(self.results(verification), {'load': 'same', 'fail': 'failed'})
        self.assertEqual(
            set(verification.statuses.values()), {PipelineRun.PipelineRunStatus.FAILED.value},
        )

    def test_runs_left_by_a_stopped_verification_are_cancelled(self):
        pipeline = self.chain(('load', 'data_loader', LOAD))
        left = fusion_verify._create_run(
            pipeline, fusion.BLOCK_FUSION_CHAINS, {}, pd.Timestamp.now(tz='UTC').to_pydatetime(),
        )
        left.update(status=PipelineRun.PipelineRunStatus.RUNNING)

        self.verify(pipeline)

        left.refresh()
        self.assertEqual(left.status, PipelineRun.PipelineRunStatus.CANCELLED)

    def test_schedulers_leave_verification_runs_alone(self):
        from mage_ai.orchestration import pipeline_scheduler_original as scheduler

        pipeline = self.chain(('load', 'data_loader', LOAD))
        run = fusion_verify._create_run(
            pipeline, fusion.BLOCK_FUSION_OFF, {}, pd.Timestamp.now(tz='UTC').to_pydatetime(),
        )
        run.update(status=PipelineRun.PipelineRunStatus.RUNNING)
        scheduled = []

        with patch.object(
            scheduler.PipelineScheduler, 'schedule',
            autospec=True, side_effect=lambda s, *a, **k: scheduled.append(s.pipeline_run.id),
        ):
            scheduler.schedule_all()

        self.assertNotIn(run.id, scheduled)

    def test_the_run_mode_overrides_the_pipeline_setting(self):
        pipeline = self.chain(('load', 'data_loader', LOAD))
        self.assertFalse(fusion.fusion_enabled(pipeline))
        on = fusion_verify._Mode(fusion.BLOCK_FUSION_CHAINS)
        off = fusion_verify._Mode(fusion.BLOCK_FUSION_OFF)
        self.assertTrue(fusion.fusion_enabled(pipeline, on))
        pipeline.block_fusion = fusion.BLOCK_FUSION_CHAINS
        self.assertFalse(fusion.fusion_enabled(pipeline, off))


class CompareValuesTest(TestCase):
    def assertSame(self, left, right):
        self.assertEqual(fusion_verify.compare_values(left, right), ('same', ''))

    def assertDiffers(self, left, right, text=''):
        result, detail = fusion_verify.compare_values(left, right)
        self.assertEqual(result, 'differs', detail)
        self.assertIn(text, detail)

    def test_pandas_frames_compare_values_dtypes_and_index(self):
        frame = pd.DataFrame({'a': pd.Series([1, None], dtype='Int64')})
        self.assertSame(frame, frame.copy())
        self.assertDiffers(frame, frame.astype('float64'), 'a: Int64 block by block, float64 fused')
        self.assertDiffers(frame, frame.set_axis([5, 6]), 'index differs')
        self.assertDiffers(frame, frame.rename(columns={'a': 'b'}))

    def test_polars_frames_and_lazy_frames(self):
        frame = pl.DataFrame({'a': [1, 2], 'b': ['x', None]})
        self.assertSame(frame, frame.lazy())
        self.assertDiffers(
            frame, frame.with_columns(pl.col('a').cast(pl.Int32)), 'a: Int64 block by block',
        )
        self.assertDiffers(frame, frame.select('b', 'a'))

    def test_types_must_match(self):
        self.assertDiffers(
            pd.DataFrame({'a': [1]}), pl.DataFrame({'a': [1]}),
            'pandas.DataFrame block by block, polars.DataFrame fused',
        )

    def test_arrays_json_and_scalars(self):
        self.assertSame(np.array([1.0, np.nan]), np.array([1.0, np.nan]))
        self.assertDiffers(np.array([1, 2]), np.array([1.0, 2.0]), 'int64')
        self.assertSame({'a': [1, {'b': math.nan}]}, {'a': [1, {'b': math.nan}]})
        self.assertDiffers({'a': 1, 'b': 2}, {'b': 2, 'a': 1}, 'keys')
        self.assertDiffers([1, 2], [1, 3], '[1] 2 block by block, 3 fused')
        self.assertSame(None, None)

    def test_float_rounding_is_close_and_larger_differences_are_not(self):
        frame = pl.DataFrame({'revenue': [4020800.13, 3981700.5], 'n': [1, 2]})
        rounded = frame.with_columns(pl.col('revenue') * (1 + 4e-16))
        result, detail = fusion_verify.compare_values(frame, rounded)
        self.assertEqual(result, 'close')
        self.assertIn('revenue: 2 values differ by up to', detail)
        self.assertDiffers(frame, frame.with_columns(pl.col('revenue') + 1), 'revenue')

        pandas_frame = frame.to_pandas()
        self.assertEqual(
            fusion_verify.compare_values(pandas_frame, rounded.to_pandas())[0], 'close',
        )
        self.assertEqual(fusion_verify.compare_values(0.1 + 0.2, 0.3)[0], 'close')
        self.assertDiffers(np.array([1.0, np.nan]), np.array([1.0, 2.0]), 'row 1')

    def test_a_difference_names_the_column_and_first_row(self):
        frame = pl.DataFrame({'id': [1, 2, 3], 'name': ['a', 'b', None]})
        self.assertDiffers(
            frame, frame.with_columns(name=pl.Series(['a', 'x', None])),
            "name: 1 rows differ; row 1 is 'b' block by block, 'x' fused",
        )
        pandas_frame = frame.to_pandas()
        changed = pandas_frame.assign(id=[1, 2, 4])
        self.assertDiffers(pandas_frame, changed, 'id: 1 rows differ; row 2 is 3')

    def test_values_without_equality_are_not_compared(self):
        class Model:
            def __eq__(self, other):
                return np.array([True])

        result, detail = fusion_verify.compare_values(Model(), Model())
        self.assertEqual(result, 'not compared')
        self.assertIn('Model', detail)
