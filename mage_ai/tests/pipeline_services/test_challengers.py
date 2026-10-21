"""
Champion and challengers in a service (item 79): predict returns the champion's output,
and a sample of the inputs is scored by each challenger in the background, as shadow
records by input key; a failing or slow challenger never reaches the caller.
"""
import json
import os
import pickle
import queue
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


class Rounder:
    def predict(self, frame):
        return (frame['x'].to_numpy() * 2).round(-1)


class Broken:
    def predict(self, frame):
        raise RuntimeError('the challenger is broken')


def embed(root: Path, path: Path, model, version: str):
    (path / 'artifacts').mkdir(parents=True)
    (path / 'artifacts' / 'model.pkl').write_bytes(pickle.dumps(model))
    (path / models.METADATA_FILE).write_text(json.dumps(dict(version=version)))


class ChallengersTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        churn = self.root / 'models' / 'churn'
        embed(self.root, churn, Doubler(), '3')
        embed(self.root, churn / 'challengers' / 'v4', Rounder(), '4')
        embed(self.root, churn / 'challengers' / 'broken', Broken(), '5')
        self.settings = churn / models.CHALLENGERS_FILE
        self.write_settings(1.0)
        self.shadow = self.root / 'shadow'
        environment = patch.dict(os.environ, {
            models.MODELS_DIR_ENV: str(self.root / 'models'),
            models.SHADOW_DIR_ENV: str(self.shadow),
            'MAGE_SERVICE': '1',
        })
        environment.start()
        self.addCleanup(environment.stop)
        models._loaded.clear()
        self.addCleanup(models._loaded.clear)

    def write_settings(self, sample):
        self.settings.write_text(json.dumps(dict(sample=sample, challengers=[
            dict(name='v4', version='4'), dict(name='broken', version='5'),
        ])))

    def records(self, label):
        lines = []
        for path in sorted((self.shadow / 'churn' / label).glob('*.jsonl')):
            lines += [json.loads(line) for line in path.read_text().splitlines()]
        return lines

    def test_the_caller_gets_the_champions_output_and_the_challengers_are_shadowed(self):
        frame = pd.DataFrame(dict(id=[10, 11, 12], x=[1.0, 7.0, 4.0]))

        output = models.predict('churn', frame, key='id', flavor='native')
        models.wait_for_challengers()

        self.assertEqual(output.tolist(), Doubler().predict(frame).tolist())
        call, *rows = self.records('v4')
        self.assertEqual((call['kind'], call['rows'], call['error']), ('call', 3, None))
        self.assertEqual((call['champion_version'], call['challenger_version']), ('3', '4'))
        self.assertGreaterEqual(call['challenger_ms'], 0)
        self.assertEqual(
            [(r['key'], r['champion'], r['challenger']) for r in rows],
            [(10, 2.0, 0.0), (11, 14.0, 10.0), (12, 8.0, 10.0)],
        )
        [broken] = self.records('broken')
        self.assertEqual(broken['error'], 'RuntimeError: the challenger is broken')

    def test_a_sample_takes_the_same_keys_every_time(self):
        self.write_settings(0.5)
        frame = pd.DataFrame(dict(id=list(range(200)), x=[1.0] * 200))
        for _ in range(2):
            models.predict('churn', frame, key='id', flavor='native')
        models.wait_for_challengers()
        rows = [r for r in self.records('v4') if r['kind'] == 'row']
        first, second = rows[:len(rows) // 2], rows[len(rows) // 2:]
        self.assertEqual([r['key'] for r in first], [r['key'] for r in second])
        self.assertTrue(40 < len(first) < 160, len(first))

    def test_a_full_queue_drops_the_call_without_waiting(self):
        full = queue.Queue(maxsize=1)
        full.put(None)
        frame = pd.DataFrame(dict(id=[1], x=[2.0]))
        with patch.object(models, '_shadow_queue', full):
            output = models.predict('churn', frame, key='id', flavor='native')
        self.assertEqual(output.tolist(), [4.0])
        [dropped] = self.records('v4')
        self.assertTrue(dropped['dropped'])

    def test_without_a_shadow_folder_only_the_champion_scores(self):
        with patch.dict(os.environ):
            del os.environ[models.SHADOW_DIR_ENV]
            frame = pd.DataFrame(dict(id=[1], x=[2.0]))
            self.assertEqual(models.predict('churn', frame, key='id', flavor='native').tolist(),
                             [4.0])
        self.assertFalse(self.shadow.exists())
