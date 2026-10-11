"""
Monitors: a block's output profiled on every run and compared with a baseline run for
drift, missing values, row counts and freshness; a failed monitor notifies, and can fail
the block run.
"""
import json
import math
import textwrap
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import monitors
from mage_ai.orchestration.db.models.schedules import BlockRun
from mage_ai.orchestration.fusion_verify import run_in_process
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import DBTestCase, TestCase

LOAD = '''
import numpy as np
import pandas as pd


@data_loader
def load(*args, **kwargs):
    rng = np.random.default_rng(int(kwargs.get('seed', 0)))
    rows = int(kwargs.get('rows', 2000))
    shift = float(kwargs.get('shift', 0))
    missing = float(kwargs.get('missing', 0))
    score = rng.normal(shift, 1, rows)
    score[rng.random(rows) < missing] = np.nan
    return pd.DataFrame({
        'score': score,
        'segment': rng.choice(['a', 'b', 'c'] if shift == 0 else ['c', 'd'], rows),
        'at': pd.Timestamp(kwargs.get('newest', '2026-10-10T00:00:00Z')),
    })
'''


class CompareTest(TestCase):
    def test_jensen_shannon_bounds(self):
        self.assertEqual(monitors.jensen_shannon([1, 2, 3], [2, 4, 6]), 0)
        self.assertAlmostEqual(monitors.jensen_shannon([1, 0], [0, 1]), math.log(2))
        self.assertIsNone(monitors.jensen_shannon([0, 0], [1, 1]))

    def test_durations_and_monitor_files_are_checked(self):
        self.assertEqual(monitors.parse_duration('6h', 'x'), 21600)
        self.assertEqual(monitors.parse_duration('1.5d', 'x'), 129600)
        with self.assertRaises(monitors.MonitorError):
            monitors.parse_duration('soon', 'x')

    def test_too_little_data_holds_and_a_lost_category_is_drift(self):
        monitor = monitors.Monitor(name='m', pipeline='p', block='b', min_rows=10)
        column = dict(name='c', kind='string', rows=100, missing=0, top=[['a', 50], ['b', 50]],
                      other=0)
        baseline = dict(rows=100, columns=[column])
        same = monitors.compare(monitor, baseline, baseline)
        self.assertEqual(same['result'], 'pass')
        moved = dict(rows=100, columns=[dict(column, top=[['z', 100]], other=0)])
        self.assertEqual(monitors.compare(monitor, moved, baseline)['result'], 'fail')
        small = dict(rows=5, columns=[dict(column, rows=5)])
        self.assertEqual(monitors.compare(monitor, small, baseline)['checks'][0]['result'], 'hold')
        self.assertEqual(monitors.compare(monitor, baseline, None)['result'], 'hold')


    def test_the_summary_names_the_checks_that_passed(self):
        record = dict(monitor='m', result='warn', baseline_run_id=3, checks=[
            dict(check='rows', result='pass', detail='48 rows'),
            dict(check='missing', column='revenue', result='pass', detail='0%'),
            dict(check='drift', column='orders', result='warn', detail='0.12'),
        ])
        self.assertEqual(monitors.summary(record), (
            'Monitor m: warn (against run 3).\n'
            '- drift orders: warn; 0.12\n'
            '- passed: rows, missing revenue'
        ))

class ExpandedCompareTest(TestCase):
    def test_population_stability_and_per_column_thresholds(self):
        self.assertEqual(monitors.population_stability([10, 10], [10, 10]), 0)
        self.assertGreater(monitors.population_stability([90, 10], [10, 90]), 1)
        monitor = monitors.Monitor(
            name='m', pipeline='p', block='b', min_rows=1,
            drift=dict(method='psi', warn=0.1, fail=0.25),
            column_rules=dict(loose=dict(drift=dict(method='psi', warn=5, fail=10),
                                         missing=None)),
        )
        column = dict(kind='numeric', rows=100, missing=0, edges=[0.0], counts=[90, 10])
        moved = dict(column, counts=[10, 90])
        baseline = dict(rows=100, columns=[dict(column, name='x'), dict(column, name='loose')])
        current = dict(rows=100, columns=[dict(moved, name='x'), dict(moved, name='loose')])
        checks = monitors.compare(monitor, current, baseline)['checks']
        results = {c['column']: c['result'] for c in checks}
        self.assertEqual(results, {'x': 'fail', 'loose': 'pass'})

    def test_rolling_baselines_add_up_runs_with_the_same_edges(self):
        def record(run, counts, top):
            return dict(pipeline_run_id=run, profile=dict(rows=sum(counts), columns=[
                dict(name='x', kind='numeric', rows=sum(counts), missing=1, edges=[0.0, 1.0],
                     counts=counts),
                dict(name='s', kind='string', rows=sum(counts), missing=0, top=top, other=0),
            ]))

        merged = monitors._merged([
            record(1, [1, 2, 3], [['a', 6]]), record(2, [3, 2, 1], [['b', 6]]),
        ])
        x, s = merged['profile']['columns']
        self.assertEqual(x['counts'], [4, 4, 4])
        self.assertEqual(x['missing'], 2)
        self.assertEqual(dict(s['top']), {'a': 6, 'b': 6})
        self.assertEqual(merged['baseline_runs'], [1, 2])

    def test_performance_settings_are_checked(self):
        with self.assertRaises(monitors.MonitorError) as caught:
            monitors._performance_settings(
                dict(target='y', prediction='p', metrics=['roc_auc']), 'm',
            )
        self.assertIn('need a score column', str(caught.exception))
        with self.assertRaises(monitors.MonitorError) as caught:
            monitors._performance_settings(
                dict(task='regression', target='y', prediction='p', metrics=['f1']), 'm',
            )
        self.assertIn('cannot compute f1', str(caught.exception))


class MonitorFlowTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        block = Block.create(
            f'{self.pipeline.uuid}_load', 'data_loader', self.repo_path, pipeline=self.pipeline,
        )
        Path(block.file_path).write_text(textwrap.dedent(LOAD))
        self.block_uuid = block.uuid
        self.monitor_path = Path(self.repo_path, 'monitors', f'{self.pipeline.uuid}.yaml')
        self.monitor_path.parent.mkdir(exist_ok=True)
        self.addCleanup(self.monitor_path.unlink, missing_ok=True)

    def set_monitor(self, extra=''):
        self.monitor_path.write_text(textwrap.dedent(f'''
            pipeline: {self.pipeline.uuid}
            block: {self.block_uuid}
            missing:
              max_increase: 0.05
            rows:
              max_change: 0.5
            freshness:
              column: at
              max_age: 2d
        ''') + extra)

    def run_pipeline(self, **variables):
        run = run_in_process(
            trigger_pipeline(self.pipeline.uuid, variables=variables), log=lambda _l: None,
        )
        block_run = next(b for b in run.block_runs if b.block_uuid == self.block_uuid)
        return run, block_run

    def slack_webhook(self) -> list:
        """A local Slack webhook: block runs run in processes of their own."""
        received = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                received.append(json.loads(body))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'ok')

            def log_message(self, *args):
                pass

        server = HTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        self.pipeline.notification_config = dict(slack_config=dict(
            webhook_url=f'http://127.0.0.1:{server.server_port}/hook',
        ))
        self.pipeline.save()
        return received

    def test_drift_missing_values_rows_and_freshness_across_runs(self):
        self.set_monitor()
        received = self.slack_webhook()
        newest = datetime.now(timezone.utc).isoformat()
        _, first = self.run_pipeline(seed=1, newest=newest)
        record = first.metrics['monitors'][0]
        self.assertEqual(record['baseline_run_id'], None)
        drift = [c for c in record['checks'] if c['check'] == 'drift']
        self.assertEqual({c['result'] for c in drift}, {'hold'})

        run, same = self.run_pipeline(seed=2, newest=newest)
        record = same.metrics['monitors'][0]
        self.assertEqual(record['result'], 'pass', monitors.summary(record))
        self.assertIsNotNone(record['baseline_run_id'])
        self.assertEqual(received, [])

        stale = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
        _, shifted = self.run_pipeline(
            seed=3, shift=2, missing=0.2, rows=500, newest=stale,
        )
        record = shifted.metrics['monitors'][0]
        failed = {(c['check'], c['column']) for c in record['checks']
                  if c['result'] == 'fail'}
        self.assertTrue({
            ('drift', 'score'), ('drift', 'segment'), ('missing_increase', 'score'),
            ('row_change', None), ('freshness', 'at'),
        } <= failed, monitors.summary(record))
        self.assertEqual(shifted.status, BlockRun.BlockRunStatus.COMPLETED)
        self.assertEqual(len(received), 1)
        self.assertIn('Monitor failed', json.dumps(received[0]))

        history = monitors.saved(self.pipeline, monitors.load_monitor(
            self.pipeline.uuid, self.repo_path,
        ))
        self.assertEqual([r['result'] for r in history][-2:], ['pass', 'fail'])
        self.assertEqual(history[-1]['baseline_run_id'], run.id)

    def test_a_monitor_can_fail_the_block_run(self):
        self.set_monitor('fail_block: true\nnotify: false\n')
        newest = datetime.now(timezone.utc).isoformat()
        self.run_pipeline(seed=1, newest=newest)
        _, shifted = self.run_pipeline(seed=2, shift=3, newest=newest)
        self.assertEqual(shifted.status, BlockRun.BlockRunStatus.FAILED)

    def test_bad_monitor_files_are_explained(self):
        self.monitor_path.write_text('pipeline: x\nblock: y\nbaseline: yesterday\n')
        with self.assertRaises(monitors.MonitorError) as caught:
            monitors.load_monitor(self.pipeline.uuid, self.repo_path)
        self.assertIn('previous or run:', str(caught.exception))
        self.monitor_path.write_text('pipeline: x\nblock: y\ndrfit: {fail: 1}\n')
        with self.assertRaises(monitors.MonitorError) as caught:
            monitors.load_monitor(self.pipeline.uuid, self.repo_path)
        self.assertIn('unknown settings: drfit', str(caught.exception))


MODEL = """
import numpy as np
import pandas as pd


@transformer
def score(*args, **kwargs):
    rng = np.random.default_rng(int(kwargs.get('seed', 0)))
    rows = 3000
    noise = float(kwargs.get('noise', 0.0))
    x = rng.normal(0, 1, rows)
    label = (x > 0).astype(int)
    probability = 1 / (1 + np.exp(-(x + rng.normal(0, noise, rows)) * 4))
    return pd.DataFrame({
        'x': x, 'label': label, 'probability': probability,
        'prediction': (probability > 0.5).astype(int),
        'amount': x * 10, 'estimate': x * 10 + rng.normal(0, noise * 10, rows),
    })
"""


class PerformanceFlowTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        block = Block.create(
            f'{self.pipeline.uuid}_score', 'transformer', self.repo_path, pipeline=self.pipeline,
        )
        Path(block.file_path).write_text(textwrap.dedent(MODEL))
        self.block_uuid = block.uuid
        self.monitors = Path(self.repo_path, 'monitors')
        self.monitors.mkdir(exist_ok=True)

    def write(self, name, text):
        path = self.monitors / f'{self.pipeline.uuid}_{name}.yaml'
        path.write_text(textwrap.dedent(text).format(
            pipeline=self.pipeline.uuid, block=self.block_uuid,
        ))
        self.addCleanup(path.unlink, missing_ok=True)

    def run_pipeline(self, **variables):
        run = run_in_process(
            trigger_pipeline(self.pipeline.uuid, variables=variables), log=lambda _l: None,
        )
        block_run = next(b for b in run.block_runs if b.block_uuid == self.block_uuid)
        return {r['monitor'].rsplit('_', 1)[-1]: r for r in block_run.metrics['monitors']}

    def test_classification_and_regression_metrics_against_the_baseline(self):
        self.write('classifier', """
            pipeline: {pipeline}
            block: {block}
            columns: [x]
            performance:
              target: label
              prediction: prediction
              score: probability
              metrics: [accuracy, f1, roc_auc]
              min: {{accuracy: 0.7}}
              max_drop: {{roc_auc: 0.05}}
            notify: on_change
        """)
        self.write('regressor', """
            pipeline: {pipeline}
            block: {block}
            columns: [x]
            performance:
              task: regression
              target: amount
              prediction: estimate
              max: {{rmse: 5}}
        """)
        first = self.run_pipeline(seed=1, noise=0.0)
        values = {c['check']: c['value'] for c in first['classifier']['checks']}
        self.assertEqual(values['accuracy'], 1.0)
        self.assertEqual(values['roc_auc'], 1.0)
        # The first run has no baseline: drift holds, and rmse passes its bound.
        rmse = next(c for c in first['regressor']['checks'] if c['check'] == 'rmse')
        self.assertEqual((rmse['result'], rmse['value']), ('pass', 0.0))

        worse = self.run_pipeline(seed=2, noise=1.5)
        failed = {c['check'] for c in worse['classifier']['checks'] if c['result'] == 'fail'}
        self.assertIn('roc_auc', failed, monitors.summary(worse['classifier']))
        self.assertTrue(worse['classifier']['should_notify'])
        regression = {c['check']: c for c in worse['regressor']['checks']}
        self.assertEqual(regression['rmse']['result'], 'fail')

        # Still failing: on_change does not notify again.
        again = self.run_pipeline(seed=3, noise=1.5)
        self.assertEqual(again['classifier']['result'], 'fail')
        self.assertFalse(again['classifier']['should_notify'])
