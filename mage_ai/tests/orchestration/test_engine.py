import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import engine
from mage_ai.tests.base_test import DBTestCase

LOADER = '''
    import pandas as pd

    @data_loader
    def load(*args, **kwargs):
        return pd.DataFrame({'value': [1, 2, int(kwargs.get('extra', 3))]})
'''
DOUBLE = '''
    @transformer
    def double(frame, *args, **kwargs):
        return frame.assign(value=frame['value'] * 2)
'''


def built() -> bool:
    try:
        return engine.binary().is_file()
    except engine.EngineError:
        return False


class EngineTest(DBTestCase):
    def pipeline(self, name: str, sensor: bool = False) -> Pipeline:
        pipeline = Pipeline.create(name, repo_path=self.repo_path)
        self.addCleanup(pipeline.delete)
        loader = Block.create(f'{name}_load', 'data_loader', self.repo_path)
        Path(loader.file_path).write_text(textwrap.dedent(LOADER))
        pipeline.add_block(loader)
        double = Block.create(f'{name}_double', 'transformer', self.repo_path)
        Path(double.file_path).write_text(textwrap.dedent(DOUBLE))
        pipeline.add_block(double, upstream_block_uuids=[loader.uuid])
        if sensor:
            check = Block.create(f'{name}_check', 'sensor', self.repo_path)
            Path(check.file_path).write_text(
                '@sensor\ndef check(*args, **kwargs):\n    return True\n',
            )
            pipeline.add_block(check, upstream_block_uuids=[double.uuid])
        return Pipeline.get(pipeline.uuid, repo_path=self.repo_path)

    def test_a_supported_pipeline_gets_a_manifest_in_place_and_it_is_kept(self):
        pipeline = self.pipeline('engine_supported')

        plan = engine.prepare(self.repo_path, pipeline.uuid)

        self.assertTrue(plan.supported, plan.reasons)
        self.assertTrue((plan.directory / 'service.json').is_file())
        link = plan.directory / Path(self.repo_path).resolve().name
        self.assertEqual(link.resolve(), Path(self.repo_path).resolve())
        with patch('mage_ai.pipeline_services.export.capture', side_effect=AssertionError):
            self.assertTrue(engine.prepare(self.repo_path, pipeline.uuid).supported)

        block = pipeline.get_block(f'{pipeline.uuid}_double')
        Path(block.file_path).write_text(textwrap.dedent(DOUBLE) + '\n# changed\n')
        with patch('mage_ai.pipeline_services.export.capture', side_effect=RuntimeError('again')):
            with self.assertRaisesRegex(RuntimeError, 'again'):
                engine.prepare(self.repo_path, pipeline.uuid)

    def test_a_pipeline_the_engine_cannot_run_keeps_the_python_path(self):
        pipeline = self.pipeline('engine_sensor', sensor=True)

        plan = engine.prepare(self.repo_path, pipeline.uuid)

        self.assertFalse(plan.supported)
        self.assertTrue(any('sensor' in reason for reason in plan.reasons), plan.reasons)
        with self.assertRaises(engine.EngineError):
            engine.run(plan)

    @unittest.skipUnless(built(), 'needs the mage-service binary (cargo build)')
    def test_the_engine_runs_the_pipeline_with_its_variables(self):
        pipeline = self.pipeline('engine_run')

        result = engine.run(engine.prepare(self.repo_path, pipeline.uuid), {'extra': 5})

        self.assertEqual(result['status'], 'completed')
        self.assertEqual([b['status'] for b in result['blocks']], ['completed', 'completed'])

    def test_a_missing_binary_is_explained(self):
        with patch.dict('os.environ', {'MAGE_SERVICE_BINARY': ''}), \
                patch.object(engine, 'SERVICE_SOURCE', Path('/nonexistent')), \
                patch('shutil.which', return_value=None):
            with self.assertRaisesRegex(engine.EngineError, 'cargo build'):
                engine.binary()
