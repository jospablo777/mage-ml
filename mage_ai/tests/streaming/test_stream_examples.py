"""Stream examples: each writes into a project, runs on its sample and exports as a service."""
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.examples import streams
from mage_ai.pipeline_services import export
from mage_ai.streaming import sample_runner, samples
from mage_ai.tests.base_test import DBTestCase


class StreamExamplesTest(DBTestCase):
    def test_each_example_runs_on_its_sample_and_exports(self):
        for name in streams.EXAMPLES:
            with self.subTest(example=name):
                written = streams.write(self.repo_path, name, f'{name}_{self._testMethodName}')
                pipeline = Pipeline.get(written['pipeline'], repo_path=self.repo_path)
                self.addCleanup(pipeline.delete)
                sample = samples.read_sample(self.repo_path, pipeline.uuid, 'example')
                for block in pipeline.blocks_by_uuid.values():
                    if str(block.type) != 'transformer':
                        continue
                    result = sample_runner.run_on_sample(
                        self.repo_path, pipeline.uuid, block.uuid, sample)
                    # The samples are clean; dead letters show on generated orders.
                    self.assertIsNone(result.get('error'), result.get('error'))
                captured = export.capture(self.repo_path, [pipeline.uuid])
                self.assertTrue(captured.manifest['pipelines'][0]['stream'])

    def test_the_cli_writes_an_example_into_a_project_elsewhere(self):
        import os
        import shutil
        import tempfile

        from typer.testing import CliRunner

        from mage_ai.cli.main import app
        from mage_ai.data_preparation.repo_manager import init_repo
        from mage_ai.settings.repo import set_repo_path

        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder, True)
        project = os.path.join(folder, 'elsewhere')
        init_repo(project)
        self.addCleanup(set_repo_path, self.repo_path)

        result = CliRunner().invoke(app, ['examples', 'stream', 'orders', project])

        self.assertEqual(result.exit_code, 0, result.output)
        for path in ('pipelines/orders_stream/metadata.yaml',
                     'data_loaders/orders_stream_orders.yaml',
                     'transformers/orders_stream_parse_orders.py'):
            self.assertTrue(os.path.isfile(os.path.join(project, path)), path)
        self.assertIn('connector_type: kafka', open(
            os.path.join(project, 'data_loaders/orders_stream_orders.yaml')).read())
        # As MageML writes its own blocks: the decorator imported outside MageML.
        code = open(os.path.join(project, 'transformers/orders_stream_parse_orders.py')).read()
        compile(code, 'parse_orders', 'exec')
        namespace = {}
        exec(code, namespace)
        self.assertTrue(callable(namespace['parse_orders']))

    def test_scores_come_from_the_trained_model(self):
        written = streams.write(self.repo_path, 'payments', 'payments_scores')
        pipeline = Pipeline.get(written['pipeline'], repo_path=self.repo_path)
        self.addCleanup(pipeline.delete)
        risky = dict(payment_id=1, customer='c', amount=900.0, hour=3, new_device=1,
                     foreign=1, attempts=5)
        usual = dict(payment_id=2, customer='c', amount=20.0, hour=14, new_device=0,
                     foreign=0, attempts=1)

        result = sample_runner.run_on_sample(
            self.repo_path, pipeline.uuid, f'{pipeline.uuid}_score_payments', [risky, usual])

        [high, low] = result['outputs'][f'{pipeline.uuid}_score_payments']
        self.assertGreater(high['risk'], low['risk'])
        self.assertTrue(high['review'])
        self.assertFalse(low['review'])

    def test_generated_messages_repeat_with_a_seed(self):
        self.assertEqual(list(streams.messages('orders', 5, seed=3)),
                         list(streams.messages('orders', 5, seed=3)))
        with self.assertRaisesRegex(streams.ExampleError, 'there are orders, iot, payments'):
            streams.write(self.repo_path, 'nope')
