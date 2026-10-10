"""
mage export service: what it captures from a project, what it refuses, and an exported
pipeline run by mage-service with the same outputs as the pipeline run by Mage.
"""
import json
import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

import polars as pl

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.pipeline_services import export
from mage_ai.tests.base_test import DBTestCase

REPO = Path(__file__).resolve().parents[3]
SERVICE_BINARY = Path(os.environ.get(
    'MAGE_SERVICE_BINARY',
    REPO / 'mage_ai' / 'pipeline_services' / 'service' / 'target' / 'debug' / 'mage-service',
))
WORKER = REPO / 'mage_ai' / 'pipeline_services' / 'runtime' / 'worker.py'

LOAD = '''
    import pandas as pd

    from {project}.utils.rates import RATE

    if 'data_loader' not in globals():
        from mage_ai.data_preparation.decorators import data_loader


    @data_loader
    def load(*args, **kwargs):
        rows = int(kwargs.get('rows', 6))
        return pd.DataFrame({{
            'id': pd.Series(range(rows), dtype='Int64'),
            'amount': [i * RATE for i in range(rows)],
            'region': pd.Categorical(['north', 'south', 'east'] * (rows // 3)),
        }})
'''
SUMMARIZE = '''
    import polars as pl


    @transformer
    def summarize(frame, **kwargs):
        return (
            pl.from_pandas(frame)
            .group_by('region')
            .agg(total=pl.col('amount').sum(), rows=pl.len())
            .sort('region')
        )


    @test
    def has_every_region(output, *args):
        assert output.height == 3, output
'''
EXPORT = '''
    @data_exporter
    def export(summary, *args, **kwargs):
        return {'regions': summary.height}
'''


class ExportTestCase(DBTestCase):
    def setUp(self):
        super().setUp()
        self.project = Path(self.repo_path).resolve()
        (self.project / 'utils').mkdir(exist_ok=True)
        (self.project / 'utils' / '__init__.py').write_text('')
        (self.project / 'utils' / 'rates.py').write_text('RATE = 1.5\n')
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)

    def add(self, name, kind, code, upstream=(), language='python', **kwargs):
        block = Block.create(
            f'{self.pipeline.uuid}_{name}', kind, self.repo_path, language=language, **kwargs,
        )
        Path(block.file_path).write_text(textwrap.dedent(code))
        self.pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    def chain(self):
        load = self.add('load', 'data_loader', LOAD.format(project=self.project.name))
        summary = self.add('summary', 'transformer', SUMMARIZE, upstream=[load])
        exported = self.add('export', 'data_exporter', EXPORT, upstream=[summary])
        return load, summary, exported


class CaptureTest(ExportTestCase):
    def test_a_pipeline_is_captured_with_its_shared_code_and_packages(self):
        load, summary, exported = self.chain()

        captured = export.capture(self.repo_path, [self.pipeline.uuid], name='Sales Service')

        self.assertEqual(captured.name, 'sales-service')
        pipeline = captured.manifest['pipelines'][0]
        self.assertEqual(
            [(b['uuid'], b['upstream']) for b in pipeline['blocks']],
            [(load.uuid, []), (summary.uuid, [load.uuid]), (exported.uuid, [summary.uuid])],
        )
        self.assertEqual(pipeline['blocks'][0]['retry']['retries'], 0)
        files = {str(p.relative_to(self.project)) for p in captured.files}
        # The loader imports utils.rates; the package's __init__ comes along.
        self.assertTrue({'utils/rates.py', 'utils/__init__.py'} <= files, files)
        self.assertIn('pandas', captured.requirements)
        self.assertIn('polars', captured.requirements)
        self.assertIn('pyarrow', captured.requirements)
        self.assertNotIn('mage-ml', captured.requirements)
        # Namespace packages and Mage's optional imports stay out.
        self.assertFalse(any(name.startswith('google-') for name in captured.requirements))
        self.assertNotIn('scipy', captured.requirements)

    def test_unsupported_blocks_stop_the_export_with_every_reason(self):
        load = self.add('load', 'data_loader', LOAD.format(project=self.project.name))
        self.add('sensor', 'sensor', '@sensor\ndef s(*args, **kwargs):\n    return True\n',
                 upstream=[load])

        with self.assertRaises(export.ExportError) as caught:
            export.capture(self.repo_path, [self.pipeline.uuid, 'missing_pipeline'])

        problems = '\n'.join(caught.exception.problems)
        self.assertIn('is a sensor block', problems)
        self.assertIn('missing_pipeline', problems)

    def test_the_build_context_holds_everything_the_image_needs(self):
        self.chain()
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)

        export.write(captured, str(out), force=True)

        for name in ('service.json', 'Dockerfile', 'compose.yaml', 'README.md', 'report.txt',
                     'requirements.txt'):
            self.assertTrue((out / name).is_file(), name)
        self.assertTrue((out / 'python/mage_ai/pipeline_services/runtime/worker.py').is_file())
        self.assertTrue((out / 'build/mage_service/Cargo.toml').is_file())
        self.assertFalse((out / 'build/mage_service/target').exists())
        self.assertFalse((out / 'python/mage_ai/frontend').exists())
        self.assertTrue((out / self.project.name / 'utils' / 'rates.py').is_file())
        dockerfile = (out / 'Dockerfile').read_text()
        self.assertIn('USER mage', dockerfile)
        self.assertIn('RUN mage-service validate', dockerfile)
        self.assertNotIn('$', (out / 'README.md').read_text().split('## Call it')[0]
                         .replace('$TOKEN', ''))
        manifest = json.loads((out / 'service.json').read_text())
        self.assertEqual(manifest['schema_version'], 1)

        # Deployment files for IBM Cloud and Kubernetes, named after the service.
        import yaml

        for name in ('.tekton/pipeline.yaml', '.tekton/tasks.yaml', '.tekton/listener.yaml',
                     'deploy/kubernetes.yaml'):
            documents = list(yaml.safe_load_all((out / name).read_text()))
            self.assertTrue(documents, name)
            self.assertNotIn('$name', (out / name).read_text(), name)
        kinds = {d['kind'] for d in yaml.safe_load_all((out / '.tekton/listener.yaml').read_text())}
        self.assertEqual(kinds, {'TriggerTemplate', 'TriggerBinding', 'EventListener'})
        script = out / 'deploy/ibm/code-engine.sh'
        self.assertTrue(os.access(script, os.X_OK))
        subprocess.run(['bash', '-n', str(script)], check=True)
        self.assertIn(captured.name, script.read_text())
        ignored = (out / '.dockerignore').read_text()
        self.assertIn('.env', ignored)

        # A second export to the same directory replaces the first.
        export.write(captured, str(out))
        with self.assertRaises(export.ExportError):
            other = Path(tempfile.mkdtemp())
            (other / 'unrelated.txt').write_text('keep me')
            export.write(captured, str(other))


@unittest.skipUnless(SERVICE_BINARY.is_file(), 'needs the mage-service binary (cargo build)')
class ExportedRunTest(ExportTestCase):
    def test_the_exported_pipeline_has_the_outputs_mage_gives(self):
        load, summary, exported = self.chain()
        self.pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        self.pipeline.execute_sync(global_vars={'rows': 9})
        expected = self.pipeline.get_block_variable(summary.uuid, 'output_0')

        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)
        export.write(export.capture(self.repo_path, [self.pipeline.uuid]), str(out), force=True)
        env = dict(
            os.environ,
            MAGE_SERVICE_DIR=str(out),
            MAGE_SERVICE_WORKER=str(WORKER),
            MAGE_SERVICE_PYTHON=str(Path(os.sys.executable)),
            PYTHONPATH=str(REPO),
        )
        result = subprocess.run(
            [str(SERVICE_BINARY), 'run', self.pipeline.uuid, '--var', 'rows=9', '--json'],
            capture_output=True, text=True, env=env, timeout=300,
        )
        self.assertEqual(result.returncode, 0, result.stdout[-3000:] + result.stderr[-3000:])
        run = json.loads(result.stdout[result.stdout.index('{'):])
        self.assertEqual(run['status'], 'completed')
        self.assertEqual([b['status'] for b in run['blocks']], ['completed'] * 3)

        summary_output = next(b for b in run['blocks'] if b['block'] == summary.uuid)
        actual = pl.read_ipc(summary_output['outputs'][0]['path'])
        self.assertTrue(actual.equals(expected), f'{actual}\n{expected}')


class EnvironmentTest(ExportTestCase):
    def test_the_variables_the_code_reads_are_listed_without_values(self):
        (self.project / 'io_config.yaml').write_text(textwrap.dedent('''
            version: 0.1.1
            default:
              POSTGRES_HOST: "{{ env_var('PGHOST') }}"
              POSTGRES_PASSWORD: "{{ env_var('PGPASSWORD') }}"
              API_KEY: "{{ mage_secret_var('payments api') }}"
        '''))
        self.add('load', 'data_loader', '''
            import os

            @data_loader
            def load(*args, **kwargs):
                region = os.getenv('AWS_REGION', 'us-east-1')
                bucket = os.environ['BUCKET']
                return {'region': region, 'bucket': bucket, 'debug': os.environ.get('DEBUG')}
        ''')

        captured = export.capture(self.repo_path, [self.pipeline.uuid])

        variables = {v['name']: v for v in captured.manifest['environment']}
        self.assertEqual(
            sorted(variables),
            ['AWS_REGION', 'BUCKET', 'DEBUG', 'MAGE_SECRET_PAYMENTS_API', 'PGHOST', 'PGPASSWORD'],
        )
        self.assertTrue(variables['BUCKET']['required'])
        self.assertFalse(variables['AWS_REGION']['required'])
        self.assertTrue(variables['PGPASSWORD']['secret'])
        self.assertTrue(variables['MAGE_SECRET_PAYMENTS_API']['secret'])
        self.assertFalse(variables['PGHOST']['secret'])
        self.assertEqual(variables['PGHOST']['used_by'], ['io_config.yaml'])

        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)
        export.write(captured, str(out), force=True)
        example = (out / '.env.example').read_text()
        self.assertIn('BUCKET=\n', example)
        self.assertIn('# required', example)
        self.assertIn('`PGPASSWORD` | no | yes', (out / 'README.md').read_text())

    def test_mage_secrets_come_from_the_environment_in_a_service(self):
        from unittest.mock import patch

        from mage_ai.data_preparation.shared.utils import get_template_vars

        secret = get_template_vars()['mage_secret_var']
        with patch.dict(os.environ, {'MAGE_SERVICE': '1', 'MAGE_SECRET_PAYMENTS_API': 'k-123'}):
            self.assertEqual(secret('payments api'), 'k-123')
            with self.assertRaisesRegex(KeyError, 'MAGE_SECRET_OTHER'):
                secret('other')


SCORE = '''
    import pandas as pd

    from mage_ai.pipeline_services.models import load_model


    @transformer
    def score(frame, *args, **kwargs):
        model = load_model('scorer', uri='models:/scorer/1')
        return frame.assign(score=model.predict(frame[['amount']]))
'''


class ModelTest(ExportTestCase):
    def setUp(self):
        super().setUp()
        import mlflow

        self.tracking = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tracking, True)
        uri = f'sqlite:///{self.tracking}/mlflow.db'
        os.environ['MLFLOW_TRACKING_URI'] = uri
        self.addCleanup(os.environ.pop, 'MLFLOW_TRACKING_URI', None)
        mlflow.set_tracking_uri(uri)
        mlflow.set_experiment('scoring')

        class Doubler(mlflow.pyfunc.PythonModel):
            def predict(self, context, model_input, params=None):
                return model_input['amount'] * 2

        with mlflow.start_run() as run:
            mlflow.log_param('factor', 2)
            mlflow.log_metric('rmse', 0.125)
            mlflow.pyfunc.log_model(
                name='model', python_model=Doubler(), registered_model_name='scorer',
                pip_requirements=['pandas'],
            )
        self.run_id = run.info.run_id

    def test_a_model_the_blocks_load_is_embedded_with_its_metadata(self):
        load = self.add('load', 'data_loader', LOAD.format(project=self.project.name))
        self.add('score', 'transformer', SCORE, upstream=[load])

        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        self.assertEqual(captured.models, [('scorer', 'models:/scorer/1')])
        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)
        export.write(captured, str(out), force=True)

        model_dir = out / 'models' / 'scorer'
        self.assertTrue((model_dir / 'artifacts' / 'MLmodel').is_file())
        metadata = json.loads((model_dir / 'mage-model.json').read_text())
        self.assertEqual(metadata['version'], '1')
        self.assertEqual(metadata['run_id'], self.run_id)
        self.assertEqual(metadata['params'], {'factor': '2'})
        self.assertEqual(metadata['metrics'], {'rmse': 0.125})
        self.assertIn('python_function', metadata['flavors'])
        manifest = json.loads((out / 'service.json').read_text())
        self.assertEqual(manifest['models'][0]['name'], 'scorer')
        self.assertEqual(manifest['models'][0]['sha256'], metadata['sha256'])
        requirements = (out / 'requirements.txt').read_text()
        self.assertTrue('mlflow-skinny==' in requirements or 'mlflow==' in requirements)
        dockerfile = (out / 'Dockerfile').read_text()
        self.assertIn('COPY --chown=mage:mage models ./models', dockerfile)
        self.assertIn('MAGE_SERVICE_MODELS_DIR=/srv/mage-service/models', dockerfile)

        if not SERVICE_BINARY.is_file():
            return
        # The service runs the embedded model with no MLflow server.
        env = dict(
            os.environ, MAGE_SERVICE_DIR=str(out), MAGE_SERVICE_WORKER=str(WORKER),
            MAGE_SERVICE_PYTHON=os.sys.executable, PYTHONPATH=str(REPO),
            MLFLOW_TRACKING_URI='http://127.0.0.1:9/unreachable',
        )
        result = subprocess.run(
            [str(SERVICE_BINARY), 'run', self.pipeline.uuid, '--var', 'rows=3', '--json'],
            capture_output=True, text=True, env=env, timeout=300,
        )
        self.assertEqual(result.returncode, 0, result.stdout[-3000:] + result.stderr[-3000:])
        run = json.loads(result.stdout[result.stdout.index('{'):])
        scored = next(b for b in run['blocks'] if b['block'].endswith('score'))
        import pyarrow.ipc as ipc

        frame = ipc.open_file(scored['outputs'][0]['path']).read_all().to_pandas()
        self.assertEqual(frame['score'].tolist(), [0.0, 3.0, 6.0])

    def test_conflicting_and_invalid_model_options_are_explained(self):
        self.add('load', 'data_loader', LOAD.format(project=self.project.name))
        with self.assertRaises(export.ExportError) as caught:
            export.capture(self.repo_path, [self.pipeline.uuid], models=['no-uri', 'bad name=x'])
        problems = '\n'.join(caught.exception.problems)
        self.assertIn('is not NAME=URI', problems)
        self.assertIn('must use letters', problems)


@unittest.skipUnless(shutil.which('cargo'), 'needs cargo')
class CargoLockTest(unittest.TestCase):
    def setUp(self):
        self.workspace = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.workspace, ignore_errors=True)
        (self.workspace / 'Cargo.toml').write_text(
            '[workspace]\nresolver = "3"\nmembers = ["blocks/*"]\n'
        )
        crate = self.workspace / 'blocks' / 'block_a'
        (crate / 'src').mkdir(parents=True)
        (crate / 'Cargo.toml').write_text(
            '[package]\nname = "block_a"\nversion = "0.1.0"\nedition = "2024"\n'
        )
        (crate / 'src' / 'main.rs').write_text('fn main() {}\n')
        self.capture = export.Capture(
            name='lock', project=self.workspace, manifest={'pipelines': []},
            rust_blocks=[('a', 'block_a')],
        )

    def test_a_stale_lock_is_updated_and_the_image_builds_locked(self):
        # A lock written before block_a existed, as when Mage built only other blocks.
        (self.workspace / 'Cargo.lock').write_text('version = 4\n')
        self.assertTrue(export._sync_cargo_lock(self.capture, self.workspace))
        self.assertIn('name = "block_a"', (self.workspace / 'Cargo.lock').read_text())
        self.assertIn('Cargo.lock was updated', ' '.join(self.capture.notes))
        self.capture.rust_locked = True
        self.assertIn('cargo build --release --locked --manifest-path rust/Cargo.toml -p block_a',
                      export._dockerfile(self.capture))

    def test_a_lock_cargo_cannot_resolve_builds_unlocked(self):
        (self.workspace / 'blocks' / 'block_a' / 'Cargo.toml').write_text(
            '[package]\nname = "block_a"\nversion = "0.1.0"\nedition = "2024"\n'
            '[dependencies]\nmissing = { path = "../nowhere" }\n'
        )
        self.assertFalse(export._sync_cargo_lock(self.capture, self.workspace))
        self.assertIn('could not be updated', ' '.join(self.capture.notes))
        self.assertIn('cargo build --release --manifest-path rust/Cargo.toml -p block_a',
                      export._dockerfile(self.capture))
