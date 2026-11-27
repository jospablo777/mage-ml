"""
Performance advice from measured runs (orchestration/advisor.py): each advice comes from the
medians of the last completed runs and names them; nothing is advised below the thresholds or
with too few runs.
"""
from datetime import datetime, timedelta, timezone

from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import advisor, compilation
from mage_ai.orchestration.advisor import BlockMeasure, RunMeasure
from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun
from mage_ai.tests.base_test import DBTestCase, TestCase
from mage_ai.tests.factory import create_pipeline_run_with_schedule

LANGUAGES = {'load': 'python', 'model': 'r', 'report': 'python'}
EDGES = [('load', 'model'), ('model', 'report')]


def runs(count=5, load=40.0, model=8.0, passed=0, digest=None):
    return [
        RunMeasure(seconds=load + model + 2, blocks={
            'load': BlockMeasure(seconds=load, output_bytes=passed,
                                 digest=digest if digest else f'run{i}'),
            'model': BlockMeasure(seconds=model, digest=f'm{i}'),
        })
        for i in range(count)
    ]


class AdviseTest(TestCase):
    def test_a_block_holding_most_of_the_run_is_named_with_its_measurement(self):
        [advice] = advisor.advise(runs(), LANGUAGES, EDGES)

        self.assertEqual(advice['kind'], 'bottleneck')
        self.assertEqual(advice['blocks'], ['load'])
        self.assertEqual(
            advice['message'],
            "load holds 80% of the run's time (40.0 s of 50.0 s, median of the last 5 "
            'completed runs): work on it first.',
        )
        self.assertEqual(advice['measurement'], dict(block_seconds=40.0, run_seconds=50.0,
                                                     runs=5))

    def test_a_language_switch_moving_much_data_and_a_repeated_output(self):
        advice = advisor.advise(
            runs(load=31.0, model=40.0, passed=1_288_490_189, digest='same'),
            LANGUAGES, EDGES,
        )

        self.assertEqual([a['kind'] for a in advice],
                         ['bottleneck', 'language_switch', 'repeated_output'])
        self.assertEqual(advice[0]['blocks'], ['model'])
        self.assertEqual(
            advice[1]['message'],
            'The switch from Python (load) to R (model) passes 1.2 GB a run (median of the '
            'last 5 completed runs). One language for both, or model reading the source '
            'itself, skips converting it.',
        )
        self.assertIn('load gave the same output in each of the last 5 completed runs',
                      advice[2]['message'])
        self.assertIn('31.0 s each', advice[2]['message'])

    def test_nothing_below_the_thresholds_or_with_too_few_runs(self):
        # Balanced blocks, little data, outputs that change.
        self.assertEqual(advisor.advise(runs(load=20.0, model=20.0), LANGUAGES, EDGES), [])
        # A short run: its biggest block is not worth a word.
        self.assertEqual(advisor.advise(runs(load=4.0, model=1.0), LANGUAGES, EDGES), [])
        self.assertEqual(advisor.advise(runs(count=2), LANGUAGES, EDGES), [])
        # One slow run does not move the median.
        measured = runs(load=10.0, model=10.0)
        measured[0].blocks['load'].seconds = 500.0
        measured[0].seconds = 512.0
        self.assertEqual(advisor.advise(measured, LANGUAGES, EDGES), [])


class MeasuresTest(DBTestCase):
    def test_runs_and_block_runs_of_the_history_become_measurements(self):
        pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(pipeline.delete)
        start = datetime(2026, 11, 1, tzinfo=timezone.utc)
        for _ in range(4):
            run = create_pipeline_run_with_schedule(pipeline_uuid=pipeline.uuid)
            run.update(
                status=PipelineRun.PipelineRunStatus.COMPLETED,
                started_at=start, completed_at=start + timedelta(seconds=60),
            )
            BlockRun.create(
                pipeline_run_id=run.id, block_uuid='load',
                status=BlockRun.BlockRunStatus.COMPLETED,
                started_at=start, completed_at=start + timedelta(seconds=45),
                metrics=dict(outputs=dict(output_0=dict(sha256='abc', bytes=2048))),
            )
        # A failed run is not measured.
        failed = create_pipeline_run_with_schedule(pipeline_uuid=pipeline.uuid)
        failed.update(status=PipelineRun.PipelineRunStatus.FAILED,
                      started_at=start, completed_at=start + timedelta(seconds=999))

        measured = advisor.measures(pipeline)

        self.assertEqual(len(measured), 4)
        self.assertEqual(measured[0].seconds, 60.0)
        self.assertEqual(measured[0].blocks['load'],
                         BlockMeasure(seconds=45.0, output_bytes=2048,
                                      digest='output_0:abc'))
        advisor._cache.clear()
        compilation._compilations.clear()
        # 45 of 60 s, and the same output four times at 45 s each.
        self.assertEqual([a['kind'] for a in compilation.summary_for(pipeline)['advice']],
                         ['bottleneck', 'repeated_output'])
