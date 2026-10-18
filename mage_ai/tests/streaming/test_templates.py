"""
Streaming pipelines from a template per source: each runs on its sample at once and exports
as a service as it stands, with nothing secret written in its files.
"""
from pathlib import Path

from mage_ai.data_preparation.models.constants import PipelineType
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.pipeline_services import export
from mage_ai.streaming import sample_runner, samples, templates
from mage_ai.tests.base_test import DBTestCase


class StreamTemplateTest(DBTestCase):
    def make(self, template):
        pipeline = Pipeline.create(f'{self._testMethodName}_{template}', repo_path=self.repo_path,
                                   pipeline_type=PipelineType.STREAMING)
        self.addCleanup(pipeline.delete)
        templates.apply(pipeline, template)
        return Pipeline.get(pipeline.uuid, repo_path=self.repo_path)

    def test_each_template_runs_on_its_sample_and_exports(self):
        for template in templates.SOURCES:
            with self.subTest(template=template):
                pipeline = self.make(template)
                blocks = {b.type: b for b in pipeline.blocks_by_uuid.values()}
                transformer = blocks['transformer']
                sample = samples.read_sample(self.repo_path, pipeline.uuid, 'example')

                result = sample_runner.run_on_sample(
                    self.repo_path, pipeline.uuid, transformer.uuid, sample)

                self.assertIsNone(result.get('error'), result.get('error'))
                output = result['outputs'][transformer.uuid]
                # The refund (a negative amount) is dropped.
                self.assertEqual(len(output), 4)
                self.assertEqual(output[0]['amount_cents'], 4250)

                captured = export.capture(self.repo_path, [pipeline.uuid])
                stream = captured.manifest['pipelines'][0]['stream']
                self.assertEqual(stream['sinks'][0]['kind'], 'stdout')
                # Every password reads the environment.
                for block in pipeline.blocks_by_uuid.values():
                    for line in Path(block.file_path).read_text().splitlines():
                        if line.strip().startswith('password:'):
                            self.assertIn('env_var(', line)

    def test_an_unknown_template_names_the_ones_there_are(self):
        pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path,
                                   pipeline_type=PipelineType.STREAMING)
        self.addCleanup(pipeline.delete)
        with self.assertRaisesRegex(templates.TemplateError, 'there are kafka, rabbitmq'):
            templates.apply(pipeline, 'kinesis')
