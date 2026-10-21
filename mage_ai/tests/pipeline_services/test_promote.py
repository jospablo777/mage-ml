"""
Promoting a model in a saved service (item 82): which version each model's build has and
the registry's uri points to now, and a promotion that changes the definition (or moves the
alias, when asked), drops a promoted challenger, and records who did it and when.
"""
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from mage_ai.pipeline_services import definitions, lifecycle


class PromoteTest(unittest.TestCase):
    def setUp(self):
        self.repo_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.repo_path, ignore_errors=True)
        (Path(self.repo_path) / 'pipelines').mkdir()
        self.registry = {
            'models:/churn/3': '3', 'models:/churn/4': '4', 'models:/churn/6': '6',
            'models:/churn@champion': '5',
        }
        version_of = patch.object(lifecycle, '_version_of', side_effect=self.registry.get)
        version_of.start()
        self.addCleanup(version_of.stop)

    def save(self, uri, challengers=None, built='3'):
        model = dict(name='churn', uri=uri)
        if challengers:
            model['challengers'] = challengers
            model['shadow'] = dict(sample=0.5)
        definitions.save_definition(self.repo_path, 'scoring',
                                    dict(pipelines=['score'], models=[model]))
        lifecycle.write_lock(self.repo_path, 'scoring', dict(
            service='scoring', models=[dict(name='churn', uri=uri, version=built)]))

    def model(self):
        return definitions.load_definition(self.repo_path, 'scoring')['models'][0]

    def test_a_challenger_becomes_the_pinned_version_and_leaves_the_challengers(self):
        self.save('models:/churn/3', [dict(name='v4', uri='models:/churn/4')])

        record = lifecycle.promote(self.repo_path, 'scoring', 'churn', challenger='v4',
                                   reason='better recall on October')

        self.assertEqual(self.model(), dict(name='churn', uri='models:/churn/4'))
        self.assertEqual(
            {k: record[k] for k in ('model', 'from_version', 'to_version', 'how', 'challenger',
                                    'reason')},
            dict(model='churn', from_version='3', to_version='4', how='pinned',
                 challenger='v4', reason='better recall on October'),
        )
        self.assertTrue(record['by'] and record['at'])
        written = yaml.safe_load((Path(self.repo_path) / 'services' / 'scoring'
                                  / lifecycle.PROMOTIONS_FILE).read_text())
        self.assertEqual(written, [record])

    def test_an_alias_that_moved_is_followed_by_a_rebuild(self):
        self.save('models:/churn@champion', built='4')
        [status] = lifecycle.models_status(self.repo_path, 'scoring')
        self.assertEqual((status['built'], status['registry'], status['status']),
                         ('4', '5', 'moved'))

        record = lifecycle.promote(self.repo_path, 'scoring', 'churn')

        self.assertEqual((record['to_version'], record['how']), ('5', 'alias followed'))
        self.assertEqual(self.model()['uri'], 'models:/churn@champion')

    def test_an_alias_moves_in_the_registry_only_when_asked(self):
        self.save('models:/churn@champion', [dict(name='next', uri='models:/churn/6')],
                  built='5')
        with self.assertRaisesRegex(lifecycle.PromoteError, '--move-alias'):
            lifecycle.promote(self.repo_path, 'scoring', 'churn', challenger='next')

        client = MagicMock()
        with patch('mlflow.tracking.MlflowClient', return_value=client):
            record = lifecycle.promote(self.repo_path, 'scoring', 'churn', challenger='next',
                                       move_alias=True)

        client.set_registered_model_alias.assert_called_once_with('churn', 'champion', '6')
        self.assertEqual(record['how'], 'alias champion moved')
        self.assertNotIn('challengers', self.model())

    def test_the_same_version_again_is_refused(self):
        self.save('models:/churn/3')
        with self.assertRaisesRegex(lifecycle.PromoteError, 'already embeds version 3'):
            lifecycle.promote(self.repo_path, 'scoring', 'churn', version='3')
        with self.assertRaisesRegex(lifecycle.PromoteError, 'name the version'):
            lifecycle.promote(self.repo_path, 'scoring', 'churn')
        with self.assertRaisesRegex(lifecycle.PromoteError, 'no model fraud'):
            lifecycle.promote(self.repo_path, 'scoring', 'fraud', version='2')
