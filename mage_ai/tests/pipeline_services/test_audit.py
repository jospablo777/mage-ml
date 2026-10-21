"""
Audit records of a service's predictions (item 80): one per scored input, before predict
returns, with the model's version, the run and block, the features as configured (values,
masked, hashed), the output and reason codes, in per-process files chained by hash.
"""
import hashlib
import json
import os
import pickle
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from mage_ai.pipeline_services import models


class Doubler:
    def predict(self, frame):
        return frame['x'].to_numpy() * 2


class AuditTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.model_dir = self.root / 'models' / 'churn'
        (self.model_dir / 'artifacts').mkdir(parents=True)
        (self.model_dir / 'artifacts' / 'model.pkl').write_bytes(pickle.dumps(Doubler()))
        (self.model_dir / models.METADATA_FILE).write_text(json.dumps(
            dict(version='3', sha256='abc')))
        self.audit_dir = self.root / 'audit'
        environment = patch.dict(os.environ, {
            models.MODELS_DIR_ENV: str(self.root / 'models'),
            models.AUDIT_DIR_ENV: str(self.audit_dir),
            'MAGE_SERVICE': '1', 'MAGE_SERVICE_NAME': 'scoring',
            'MAGE_SERVICE_SOURCE_SHA256': 'feed',
        })
        environment.start()
        self.addCleanup(environment.stop)
        models._loaded.clear()
        models._chain.clear()
        self.addCleanup(models._loaded.clear)
        models.set_decision_context(pipeline='score', block='score_block', block_sha256='b1',
                                    run_id='run-1')

    def settings(self, **audit):
        (self.model_dir / models.AUDIT_FILE).write_text(json.dumps(audit))

    def records(self):
        lines = []
        for path in sorted((self.audit_dir / 'churn').glob('*.jsonl')):
            lines += path.read_text().splitlines()
        return lines

    def frame(self):
        return pd.DataFrame(dict(id=[1, 2], x=[1.5, 4.0], ssn=['111', '222'],
                                 email=['a@x.org', 'b@x.org']))

    def test_each_input_gets_a_record_with_what_decided_it(self):
        self.settings(features='values', mask=['ssn'], hash=['email'])

        output = models.predict('churn', self.frame(), key='id', flavor='native',
                                features=None)

        self.assertEqual(output.tolist(), [3.0, 8.0])
        first, second = [json.loads(line) for line in self.records()]
        self.assertEqual(
            {k: first[k] for k in ('key', 'model', 'version', 'model_sha256', 'service',
                                   'service_sha256', 'pipeline', 'block', 'block_sha256',
                                   'run_id', 'output')},
            dict(key=1, model='churn', version='3', model_sha256='abc', service='scoring',
                 service_sha256='feed', pipeline='score', block='score_block',
                 block_sha256='b1', run_id='run-1', output=3.0),
        )
        self.assertEqual(first['features']['x'], 1.5)
        self.assertEqual(first['features']['ssn'], '***')
        self.assertEqual(first['features']['email'], models._hash_value('a@x.org'))
        self.assertNotIn('a@x.org', json.dumps(first))
        self.assertEqual(second['key'], 2)

    def test_the_records_of_a_file_are_a_chain(self):
        self.settings(features='hash')
        for _ in range(2):
            models.predict('churn', self.frame()[['id', 'x']], key='id', flavor='native')
        lines = self.records()
        self.assertEqual(len(lines), 4)
        self.assertIsNone(json.loads(lines[0])['previous'])
        for before, line in zip(lines, lines[1:]):
            self.assertEqual(json.loads(line)['previous'],
                             hashlib.sha256(before.encode()).hexdigest())
        self.assertIn('features_sha256', json.loads(lines[0]))
        self.assertNotIn('features', json.loads(lines[0]))

    def test_a_keyed_hash_cannot_be_checked_without_the_key(self):
        self.settings(features='values', hash=['email'])
        with patch.dict(os.environ, {models.AUDIT_KEY_ENV: 'secret'}):
            models.predict('churn', self.frame()[['id', 'x', 'email']], key='id',
                           flavor='native', features=['x'])
        record = json.loads(self.records()[0])
        self.assertEqual(set(record['features']), {'x'})

    def test_a_model_without_audit_settings_writes_nothing(self):
        models.predict('churn', self.frame(), key='id', flavor='native', features=['x'])
        self.assertEqual(self.records(), [])

    @unittest.skipUnless(__import__('importlib').util.find_spec('catboost'), 'needs catboost')
    def test_reason_codes_come_from_the_trees(self):
        import catboost

        train = pd.DataFrame(dict(a=[0, 1, 2, 3, 4, 5] * 5, b=[5, 3, 1, 4, 2, 0] * 5))
        target = train['a'] * 3.0
        model = catboost.CatBoostRegressor(iterations=20, verbose=False,
                                           allow_writing_files=False).fit(train, target)
        shutil.rmtree(self.model_dir / 'artifacts')
        (self.model_dir / 'artifacts').mkdir()
        model.save_model(str(self.model_dir / 'artifacts' / 'model.cbm'))
        self.settings(features='none', reasons=1)

        rows = pd.DataFrame(dict(id=[7], a=[5], b=[0]))
        models.predict('churn', rows, key='id', flavor='native', features=['a', 'b'])

        [record] = [json.loads(line) for line in self.records()]
        self.assertEqual([r['feature'] for r in record['reasons']], ['a'])
        self.assertNotIn('features', record)
