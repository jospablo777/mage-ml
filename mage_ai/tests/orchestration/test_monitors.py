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
