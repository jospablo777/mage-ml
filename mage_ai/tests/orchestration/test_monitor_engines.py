"""
Monitors with Evidently and NannyML: the engines run in an environment uv builds for
them, their failed tests become checks, and their HTML reports are kept for the run and
served by the API. Skipped where uv is not installed; the first run downloads the engines.
"""
import shutil
import textwrap
import unittest
from pathlib import Path

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import monitors
from mage_ai.orchestration.fusion_verify import run_in_process
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user

MODEL = '''
import numpy as np
import pandas as pd


@transformer
def score(*args, **kwargs):
    rng = np.random.default_rng(int(kwargs['seed']))
    rows = 3000
    x = rng.normal(float(kwargs.get('shift', 0)), 1, rows)
    probability = 1 / (1 + np.exp(-x))
    return pd.DataFrame({
        'at': pd.date_range('2026-01-01', periods=rows, freq='min'),
        'x': x,
        'other': rng.normal(0, 1, rows),
        'probability': probability,
        'prediction': (probability > 0.5).astype(int),
        'label': (rng.random(rows) < probability).astype(int),
    })
'''


@unittest.skipIf(shutil.which('uv') is None, 'uv is not installed')
class MonitorEnginesTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        block = Block.create(
            f'{self.pipeline.uuid}_score', 'transformer', self.repo_path, pipeline=self.pipeline,
        )
        Path(block.file_path).write_text(textwrap.dedent(MODEL))
        self.block_uuid = block.uuid
        path = Path(self.repo_path, 'monitors', f'{self.pipeline.uuid}.yaml')
        path.parent.mkdir(exist_ok=True)
        path.write_text(textwrap.dedent(f'''
            pipeline: {self.pipeline.uuid}
            block: {self.block_uuid}
            columns: [x, other]
            evidently:
              presets: [DataDriftPreset]
              severity: fail
            nannyml:
              problem_type: classification_binary
              timestamp: at
              y_pred: prediction
              y_pred_proba: probability
              y_true: label
              metrics: [roc_auc]
              features: [x, other]
              chunk_size: 1000
        '''))
        self.addCleanup(path.unlink, missing_ok=True)

    def run_pipeline(self, **variables):
        run = run_in_process(
            trigger_pipeline(self.pipeline.uuid, variables=variables), log=lambda _l: None,
        )
        block_run = next(b for b in run.block_runs if b.block_uuid == self.block_uuid)
        return run, block_run.metrics['monitors'][0]

    async def test_engines_check_against_the_baseline_and_keep_their_reports(self):
        _, first = self.run_pipeline(seed=1)
        engine_checks = [c for c in first['checks'] if c['check'] in ('evidently', 'nannyml')]
        self.assertEqual({c['result'] for c in engine_checks}, {'hold'})

        run, shifted = self.run_pipeline(seed=2, shift=1.5)
        self.assertEqual(shifted['engines']['evidently']['error'], None)
        self.assertEqual(shifted['engines']['nannyml']['error'], None)
        failed = {(c['check'], c['column']) for c in shifted['checks'] if c['result'] == 'fail'}
        self.assertIn(('nannyml_drift', 'x'), failed, monitors.summary(shifted))
        self.assertTrue(any(
            c['check'] == 'evidently' and c['result'] == 'fail' and 'column x' in c['detail']
            for c in shifted['checks']
        ), monitors.summary(shifted))
        self.assertIn('evidently.html', shifted['engines']['evidently']['reports'])
        self.assertIn('nannyml_performance.html', shifted['engines']['nannyml']['reports'])

        owner = create_user(_owner=True)
        report = (await BaseOperation(
            action=OperationType.DETAIL, pk=self.pipeline.uuid, resource='monitor_reports',
            query=dict(pipeline_run_id=[str(run.id)], report=['evidently.html']), user=owner,
        ).execute())['monitor_report']
        self.assertIn('<html', report['html'].lower())
        missing = await BaseOperation(
            action=OperationType.DETAIL, pk=self.pipeline.uuid, resource='monitor_reports',
            query=dict(pipeline_run_id=[str(run.id)], report=['../../secrets.html']), user=owner,
        ).execute()
        self.assertIn('no such report', missing['error']['message'])
