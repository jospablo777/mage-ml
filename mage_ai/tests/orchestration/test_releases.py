"""
Releases: rules decide pass, hold or fail; a version a block registers is evaluated
against its model's policy and champion, promoted by moving an MLflow alias (at once or
on approval), and rolled back from the release log.
"""
import os
import shutil
import tempfile
import textwrap
import unittest
from pathlib import Path

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import releases
from mage_ai.orchestration.db.models.schedules import BlockRun
from mage_ai.orchestration.fusion_verify import run_in_process
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import DBTestCase, TestCase

try:
    import mlflow
except ImportError:  # pragma: no cover
    mlflow = None

TRAIN = '''
import mlflow
import pandas as pd


class Identity(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input, params=None):
        return model_input


@transformer
def train(*args, **kwargs):
    mlflow.set_experiment('churn')
    with mlflow.start_run(run_name='train'):
        mlflow.log_metric('auc', float(kwargs['auc']))
        mlflow.log_metric('eval_rows', 5000)
        mlflow.pyfunc.log_model(
            name='model', python_model=Identity(), registered_model_name='{model}',
            pip_requirements=['pandas'],
        )
    return pd.DataFrame({{'auc': [kwargs['auc']]}})
'''

POLICY = '''
alias: champion
approval: {approval}
fail_block: {fail_block}
rules:
  - metric: auc
    min: 0.6
    max_regression: 0.02
  - metric: eval_rows
    min: 1000
'''


class RuleTest(TestCase):
    def policy(self, *rules):
        return releases.Policy(model='m', rules=list(rules))

    def test_rules_pass_hold_and_fail(self):
        auc = releases.Rule(metric='auc', min=0.7, max_regression=0.01)
        loss = releases.Rule(metric='loss', max=0.5, max_regression=0.1, higher_is_better=False)
        policy = self.policy(auc, loss)

        first = releases.evaluate(policy, dict(auc=0.8, loss=0.3), None)
        self.assertEqual(first['decision'], 'pass')
        self.assertIn('no champion to compare with', first['rules'][0]['reasons'])

        regressed = releases.evaluate(policy, dict(auc=0.78, loss=0.45), dict(auc=0.8, loss=0.3))
        self.assertEqual(regressed['decision'], 'fail')
        self.assertEqual([r['result'] for r in regressed['rules']], ['fail', 'fail'])
        self.assertIn('worse than the champion', regressed['rules'][1]['reasons'][0])

        missing = releases.evaluate(policy, dict(auc=float('nan')), dict(auc=0.8, loss=0.3))
        self.assertEqual(missing['decision'], 'hold')

        # A known failure fails even when another rule cannot be checked.
        mixed = releases.evaluate(policy, dict(auc=0.5), dict(auc=0.8, loss=0.3))
        self.assertEqual(mixed['decision'], 'fail')

    def test_policies_are_checked(self):
        repo = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo, True)
        os.makedirs(os.path.join(repo, 'releases'))
        for text, expected in [
            ('rules:\n  - metric: auc\n    minimum: 1\n', 'unknown settings: minimum'),
            ('rules: []\n', 'has no rules'),
            ('rules:\n  - metric: auc\n', 'needs min, max or max_regression'),
            ('rules:\n  - metric: auc\n    min: .nan\n', 'finite number'),
            ('approval: sometimes\nrules:\n  - {metric: auc, min: 1}\n', 'manual or automatic'),
        ]:
            Path(repo, 'releases', 'm.yaml').write_text(text)
            with self.assertRaises(releases.ReleaseError) as caught:
                releases.load_policy('m', repo)
            self.assertIn(expected, str(caught.exception))
        self.assertIsNone(releases.load_policy('other', repo))


@unittest.skipIf(mlflow is None, 'MLflow is not installed')
class ReleaseFlowTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.tracking = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tracking, True)
        uri = f'sqlite:///{self.tracking}/mlflow.db'
        os.environ['MLFLOW_TRACKING_URI'] = uri
        self.addCleanup(os.environ.pop, 'MLFLOW_TRACKING_URI', None)
        mlflow.set_tracking_uri(uri)
        self.model = f'model_{self.faker.unique.random_int(1, 10**9)}'
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        block = Block.create(
            f'{self.pipeline.uuid}_train', 'transformer', self.repo_path, pipeline=self.pipeline,
        )
        Path(block.file_path).write_text(textwrap.dedent(TRAIN.format(model=self.model)))
        self.block_uuid = block.uuid
        self.policy_path = Path(self.repo_path, 'releases', f'{self.model}.yaml')
        self.policy_path.parent.mkdir(exist_ok=True)
        self.addCleanup(self.policy_path.unlink, missing_ok=True)

    def set_policy(self, approval='automatic', fail_block='false'):
        self.policy_path.write_text(POLICY.format(approval=approval, fail_block=fail_block))

    def run_pipeline(self, auc):
        run = run_in_process(
            trigger_pipeline(self.pipeline.uuid, variables=dict(auc=auc)), log=lambda _l: None,
        )
        block_run = next(b for b in run.block_runs if b.block_uuid == self.block_uuid)
        return block_run

    def champion(self):
        return releases.champion_of(
            releases._client(), releases.load_policy(self.model, self.repo_path),
        ).version

    def test_automatic_promotion_regression_and_rollback(self):
        self.set_policy()
        first = self.run_pipeline(0.8)
        self.assertEqual(first.metrics['releases'][0]['status'], 'promoted')
        self.assertEqual(str(self.champion()), '1')

        worse = self.run_pipeline(0.75)
        evaluation = worse.metrics['releases'][0]
        self.assertEqual((evaluation['decision'], evaluation['status']), ('fail', 'not promoted'))
        self.assertEqual(evaluation['champion'], '1')
        self.assertEqual(worse.status, BlockRun.BlockRunStatus.COMPLETED)
        self.assertEqual(str(self.champion()), '1')

        better = self.run_pipeline(0.81)
        self.assertEqual(better.metrics['releases'][0]['status'], 'promoted')
        self.assertEqual(str(self.champion()), '3')

        entry = releases.rollback(self.repo_path, self.model, 'tester')
        self.assertEqual((entry['action'], entry['version'], entry['previous']),
                         ('rollback', '1', '3'))
        self.assertEqual(str(self.champion()), '1')
        log = releases.release_log(self.repo_path, self.model)
        self.assertEqual([e['action'] for e in log], ['promotion', 'promotion', 'rollback'])

    def test_manual_approval_checks_the_champion_it_was_compared_with(self):
        self.set_policy(approval='manual')
        first = self.run_pipeline(0.8)
        evaluation = first.metrics['releases'][0]
        self.assertEqual(evaluation['status'], 'awaiting approval')
        releases.promote(self.repo_path, self.model, '1', None, 'tester')
        second = self.run_pipeline(0.82)
        self.assertEqual(second.metrics['releases'][0]['champion'], '1')

        with self.assertRaises(releases.ReleaseError) as caught:
            releases.promote(self.repo_path, self.model, '2', None, 'tester')
        self.assertIn('evaluate it again', str(caught.exception))
        releases.promote(self.repo_path, self.model, '2', '1', 'tester')
        self.assertEqual(str(self.champion()), '2')

    def test_a_policy_can_fail_the_block_run(self):
        self.set_policy(fail_block='true')
        failed = self.run_pipeline(0.5)
        self.assertEqual(failed.status, BlockRun.BlockRunStatus.FAILED)
