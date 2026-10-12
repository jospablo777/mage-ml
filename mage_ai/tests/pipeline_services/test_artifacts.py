"""Model files in their frameworks' formats, loaded once per process by load_artifact."""
import importlib.util
import os
import shutil
import tempfile
import time
import unittest
from unittest.mock import patch

import numpy as np

from mage_ai.pipeline_services import models
from mage_ai.tests.base_test import DBTestCase


class LoadArtifactTest(unittest.TestCase):
    def setUp(self):
        self.project = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.project, True)
        os.makedirs(os.path.join(self.project, 'models'))
        patcher = patch('mage_ai.settings.repo.get_repo_path', return_value=self.project)
        patcher.start()
        self.addCleanup(patcher.stop)
        models._artifacts.clear()

    def path(self, name):
        return os.path.join(self.project, 'models', name)

    def regressor(self):
        from sklearn.linear_model import LinearRegression

        X = np.array([[0.0], [1.0], [2.0]])
        return LinearRegression().fit(X, 2 * X[:, 0] + 1)

    def test_skops_and_joblib_files_load_from_a_path_in_the_project(self):
        import joblib
        import skops.io

        skops.io.dump(self.regressor(), self.path('model.skops'))
        joblib.dump(self.regressor(), self.path('model.joblib'))

        for name in ('model.skops', 'model.joblib'):
            with self.subTest(name=name):
                model = models.load_artifact(f'models/{name}')
                self.assertAlmostEqual(model.predict([[3.0]])[0], 7.0)

    def test_untrusted_skops_types_are_named(self):
        import skops.io
        from sklearn.preprocessing import FunctionTransformer

        skops.io.dump(FunctionTransformer(np.log1p), self.path('custom.skops'))
        try:
            models.load_artifact('models/custom.skops')
        except ValueError as error:
            self.assertIn('holds types skops does not trust by default', str(error))
            self.assertIn('trusted=[...]', str(error))
        else:
            # Newer skops trusts these types: then the file simply loads.
            pass

    # No extra installs torch or safetensors; the test needs both.
    @unittest.skipUnless(
        all(importlib.util.find_spec(name) for name in ('torch', 'safetensors')),
        'torch and safetensors are not installed',
    )
    def test_torch_export_programs_and_safetensors(self):
        import torch
        from safetensors.torch import save_file

        program = torch.export.export(torch.nn.Linear(2, 1), (torch.zeros(3, 2),))
        torch.export.save(program, self.path('net.pt2'))
        save_file({'weight': torch.ones(2, 2)}, self.path('weights.safetensors'))

        net = models.load_artifact('models/net.pt2')
        self.assertEqual(tuple(net(torch.zeros(3, 2)).shape), (3, 1))
        weights = models.load_artifact('models/weights.safetensors')
        self.assertEqual(float(weights['weight'].sum()), 4.0)

    @unittest.skipUnless(importlib.util.find_spec('catboost'), 'catboost is not installed')
    def test_catboost_models_load_as_the_class_they_were_saved_from(self):
        """
        A classifier saved as .cbm loaded as the generic CatBoost, whose predict returned
        raw scores where the classifier returned labels, and which had no predict_proba.
        """
        from catboost import CatBoostClassifier, CatBoostRegressor

        X = np.array([[0.0, 1.0], [1.0, 0.0], [2.0, 1.0], [3.0, 0.0]] * 5)
        y = np.array([0, 1, 0, 1] * 5)
        classifier = CatBoostClassifier(iterations=5, verbose=0).fit(X, y)
        classifier.save_model(self.path('cls.cbm'))
        regressor = CatBoostRegressor(iterations=5, verbose=0).fit(X, X[:, 0])
        regressor.save_model(self.path('reg.cbm'))

        loaded = models.load_artifact('models/cls.cbm')
        self.assertIsInstance(loaded, CatBoostClassifier)
        self.assertEqual(loaded.predict(X).tolist(), classifier.predict(X).tolist())
        self.assertEqual(loaded.predict_proba(X).shape, (20, 2))
        self.assertIsInstance(models.load_artifact('models/reg.cbm'), CatBoostRegressor)

    def test_a_function_loads_formats_without_a_known_suffix(self):
        with open(self.path('forecaster.bin'), 'w') as file:
            file.write('state')

        loaded = models.load_artifact('models/forecaster.bin', loader=lambda p: open(p).read())

        self.assertEqual(loaded, 'state')

    def test_a_model_is_loaded_once_and_again_when_the_file_changes(self):
        import joblib

        joblib.dump(self.regressor(), self.path('model.joblib'))
        first = models.load_artifact('models/model.joblib')
        self.assertIs(models.load_artifact('models/model.joblib'), first)

        time.sleep(0.01)
        joblib.dump(self.regressor(), self.path('model.joblib'))
        os.utime(self.path('model.joblib'), (time.time() + 5, time.time() + 5))
        self.assertIsNot(models.load_artifact('models/model.joblib'), first)

    def test_errors_say_what_to_do(self):
        with self.assertRaises(models.ModelNotFound) as caught:
            models.load_artifact('models/missing.cbm')
        self.assertIn('Paths are relative to the project', str(caught.exception))

        with open(self.path('booster.txt'), 'w') as file:
            file.write('x')
        with self.assertRaises(ValueError) as caught:
            models.load_artifact('models/booster.txt')
        self.assertIn('pass loader=', str(caught.exception))

        with self.assertRaises(ValueError) as caught:
            models.load_artifact('models/booster.txt', loader='nope')
        self.assertIn('Unknown loader', str(caught.exception))

    def test_every_suffix_names_its_loader_and_module(self):
        for suffix, (loader, module) in models.ARTIFACT_LOADERS.items():
            with self.subTest(suffix=suffix):
                self.assertTrue(loader == 'pickle' or models.LOADER_MODULES[loader] == module)


class ModelInferenceTemplateTest(DBTestCase):
    def test_the_template_scores_rows_with_a_model_file(self):
        import pandas as pd
        import skops.io
        from sklearn.linear_model import LinearRegression

        from mage_ai.data_preparation.models.block import Block
        from mage_ai.data_preparation.models.pipeline import Pipeline
        from mage_ai.data_preparation.templates.template import template_env

        os.makedirs(os.path.join(self.repo_path, 'models'), exist_ok=True)
        X = pd.DataFrame({'x': [0.0, 1.0, 2.0]})
        skops.io.dump(LinearRegression().fit(X, 2 * X['x'] + 1),
                      os.path.join(self.repo_path, 'models', 'line.skops'))
        pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        load = Block.create(f'{pipeline.uuid}_load', 'data_loader', self.repo_path,
                            pipeline=pipeline)
        with open(load.file_path, 'w') as file:
            file.write(
                "import pandas as pd\n\n@data_loader\ndef load(*args, **kwargs):\n"
                "    return pd.DataFrame({'id': [1, 2], 'x': [3.0, 4.0]})\n"
            )
        score = Block.create(f'{pipeline.uuid}_score', 'transformer', self.repo_path,
                             pipeline=pipeline, upstream_block_uuids=[load.uuid])
        with open(score.file_path, 'w') as file:
            file.write(template_env.get_template('transformers/model_inference.jinja')
                       .render(code=''))
        pipeline = Pipeline.get(pipeline.uuid, repo_path=self.repo_path)
        variables = dict(model='models/line.skops', features=['x'])

        pipeline.get_block(load.uuid).execute_sync(global_vars=variables)
        result = pipeline.get_block(score.uuid).execute_sync(global_vars=variables)

        scored = result['output'][0]
        self.assertEqual(scored['prediction'].round(6).tolist(), [7.0, 9.0])
        self.assertEqual(scored['id'].tolist(), [1, 2])
