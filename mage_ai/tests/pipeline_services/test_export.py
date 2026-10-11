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
import yaml

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.data_preparation.repo_manager import init_project_uuid
from mage_ai.pipeline_services import export
from mage_ai.settings.repo import set_repo_path
from mage_ai.tests.base_test import DBTestCase
from mage_ai.tests.shared.tracking import use_temporary_mlflow

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
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # The blocks import `<project>.utils`. The test project is named test, like the
        # package of Python's own tests, which interpreters from pyenv and python.org ship
        # and Python finds before the project.
        cls.repo_path = os.path.join(cls._test_directory, 'shop')
        os.mkdir(cls.repo_path)
        set_repo_path(cls.repo_path)
        init_project_uuid()

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

    def test_contracts_and_monitors_the_service_does_not_check_are_noted(self):
        _, summary, _ = self.chain()
        summary.configuration = dict(contract='regions')
        self.pipeline.save()
        (self.project / 'monitors').mkdir(exist_ok=True)
        monitor = self.project / 'monitors' / f'{self.pipeline.uuid}.yaml'
        monitor.write_text(f'pipeline: {self.pipeline.uuid}\nblock: {summary.uuid}\n')
        self.addCleanup(monitor.unlink)

        notes = '\n'.join(export.capture(self.repo_path, [self.pipeline.uuid]).notes)

        self.assertIn(f'block {summary.uuid}: the service does not check data contract '
                      'regions', notes)
        self.assertIn(f'the service does not run monitor {self.pipeline.uuid}.', notes)

    def test_model_files_the_blocks_name_and_includes_go_in_the_image(self):
        import joblib
        from sklearn.linear_model import LinearRegression

        models_dir = self.project / 'models'
        models_dir.mkdir(exist_ok=True)
        self.addCleanup(shutil.rmtree, models_dir, True)
        joblib.dump(LinearRegression().fit([[0.0], [1.0]], [0.0, 1.0]), models_dir / 'm.joblib')
        (models_dir / 'forecaster').mkdir()
        (models_dir / 'forecaster' / 'state.bin').write_bytes(b'x' * 10)
        (models_dir / 'extra.txt').write_text('computed path')
        self.add('score', 'data_loader', '''
            from mage_ai.pipeline_services.models import load_artifact

            @data_loader
            def score(*args, **kwargs):
                model = load_artifact('models/m.joblib')
                forecaster = 'models/forecaster'
                missing = 'models/not_there.bin'
                return {'prediction': float(model.predict([[2.0]])[0])}
        ''')

        captured = export.capture(
            self.repo_path, [self.pipeline.uuid], includes=['models/extra.txt'],
        )

        files = {str(p.relative_to(self.project)) for p in captured.files}
        self.assertTrue({
            'models/m.joblib', 'models/forecaster/state.bin', 'models/extra.txt',
        } <= files, files)
        self.assertNotIn('models/not_there.bin', files)
        notes = '\n'.join(captured.notes)
        self.assertIn('Included models/m.joblib', notes)
        self.assertIn('Included models/extra.txt (13 bytes), with --include.', notes)
        self.assertIn('joblib', captured.requirements)

    def test_a_model_only_rust_or_r_reads_adds_no_python_package(self):
        project = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, project, True)
        (project / 'models').mkdir()
        (project / 'models' / 'rust.onnx').write_bytes(b'onnx')
        (project / 'models' / 'python.cbm').write_bytes(b'cbm')
        (project / 'score.rs').write_text('let m = mage::onnx::model("models/rust.onnx")?;\n')
        (project / 'score.py').write_text("model = load_artifact('models/python.cbm')\n")
        capture_ = export.Capture(
            name='s', project=project, manifest={'pipelines': []},
            files={project / 'score.rs', project / 'score.py'},
        )
        problems = []

        export._include_artifacts(capture_, [], problems)

        self.assertEqual(problems, [])
        self.assertTrue({project / 'models' / 'rust.onnx', project / 'models' / 'python.cbm'}
                        <= capture_.files)
        # ONNX Runtime is linked into the Rust program; only CatBoost is a Python package.
        self.assertEqual(capture_.artifact_modules, {'catboost'})

    def test_onnx_blocks_build_and_run_on_trixie_with_a_cxx_library(self):
        project = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, project, True)
        (project / 'rust').mkdir()
        manifest = (
            '[workspace.dependencies]\nmage-block = {{ path = ".mage/sdk", features = {} }}\n'
        )
        capture_ = export.Capture(
            name='s', project=project,
            manifest={'pipelines': [{'blocks': [{'language': 'rust'}]}]},
            rust_blocks=[('score', 'block_score')],
        )

        (project / 'rust' / 'Cargo.toml').write_text(manifest.format('["postgres"]'))
        self.assertFalse(export._uses_sdk_feature(capture_, 'onnx'))
        plain = export._dockerfile(capture_)
        self.assertIn('-slim-bookworm AS build', plain)
        self.assertNotIn('g++', plain)

        (project / 'rust' / 'Cargo.toml').write_text(manifest.format('["postgres", "onnx"]'))
        self.assertTrue(export._uses_sdk_feature(capture_, 'onnx'))
        onnx = export._dockerfile(capture_)
        self.assertIn('-slim-trixie AS build', onnx)
        self.assertIn('FROM debian:trixie-slim', onnx)
        self.assertIn('install -y --no-install-recommends g++', onnx)

    def test_an_include_outside_the_project_or_missing_stops_the_export(self):
        self.chain()
        with self.assertRaises(export.ExportError) as caught:
            export.capture(self.repo_path, [self.pipeline.uuid], includes=['../x', 'nope.bin'])
        problems = '\n'.join(caught.exception.problems)
        self.assertIn('--include ../x: the path is outside the project.', problems)
        self.assertIn('--include nope.bin: there is no such file or folder', problems)

    def test_exporting_again_keeps_the_env_file_local_data_and_the_git_repository(self):
        self.chain()
        captured = export.capture(self.repo_path, [self.pipeline.uuid])
        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)
        export.write(captured, str(out), force=True)
        (out / '.env').write_text('PGPASSWORD=secret\n')
        (out / '.env.staging').write_text('PGPASSWORD=other\n')
        (out / 'data').mkdir()
        (out / 'data' / 'sample.csv').write_text('a\n1\n')
        (out / 'stale.txt').write_text('from an older export')
        (out / '.env.example').write_text('OLD=\n')
        (out / '.git').mkdir()
        (out / '.git' / 'HEAD').write_text('ref: refs/heads/main\n')

        export.write(captured, str(out))

        self.assertEqual((out / '.env').read_text(), 'PGPASSWORD=secret\n')
        self.assertTrue((out / '.env.staging').is_file())
        self.assertTrue((out / 'data' / 'sample.csv').is_file())
        self.assertFalse((out / 'stale.txt').exists())
        self.assertNotIn('OLD=', (out / '.env.example').read_text())
        self.assertTrue((out / 'service.json').is_file())
        self.assertTrue((out / '.git' / 'HEAD').is_file())
        ignored = (out / '.gitignore').read_text().splitlines()
        self.assertIn('.env', ignored)
        self.assertIn('!.env.example', ignored)

    def test_the_folder_builds_for_other_platforms_and_in_ci(self):
        from unittest.mock import patch

        self.chain()
        captured = export.capture(self.repo_path, [self.pipeline.uuid], name='shop')
        out = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, out, True)
        export.write(captured, str(out))

        workflow = yaml.safe_load((out / '.github' / 'workflows' / 'build-image.yml').read_text())
        build = workflow['jobs']['image']['steps'][-1]['with']
        self.assertEqual(build['platforms'], 'linux/amd64,linux/arm64')
        self.assertIn("'shop'", build['tags'])
        self.assertIn('.github/', (out / '.dockerignore').read_text())
        self.assertIn('docker buildx build --platform', (out / 'README.md').read_text())

        with patch.object(export.shutil, 'which', return_value='/usr/bin/docker'):
            self.assertEqual(export.build_command(out, 'shop')[1:3], ['build', '-t'])
            pushed = export.build_command(
                out, 'icr.io/team/shop:1', platforms=['linux/amd64', 'linux/arm64'], push=True,
            )
            self.assertEqual(pushed[1:], [
                'buildx', 'build', '--tag', 'icr.io/team/shop:1',
                '--platform', 'linux/amd64,linux/arm64', '--push', str(out),
            ])
            self.assertIn('--load', export.build_command(out, 'shop', platforms=['linux/arm64']))
            with self.assertRaisesRegex(export.ExportError, 'Several platforms need --push'):
                export.build_command(out, 'shop', platforms=['linux/amd64', 'linux/arm64'])
        with patch.object(export.shutil, 'which', return_value=None):
            with self.assertRaisesRegex(export.ExportError, 'docker is not installed'):
                export.build_command(out, 'shop')

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
        # Placeholders are all filled; the shell variables stay.
        self.assertNotIn('$', (out / 'README.md').read_text().split('## Call it')[0]
                         .replace('$TOKEN', '').replace('$IBM_CLOUD_API_KEY', ''))
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
            # The blocks run as one chain, which keeps only the last block's outputs
            # unless asked to keep every block's.
            MAGE_SERVICE_KEEP_OUTPUTS='on',
        )
        result = subprocess.run(
            [str(SERVICE_BINARY), 'run', self.pipeline.uuid, '--var', 'rows=9', '--json'],
            capture_output=True, text=True, env=env, timeout=300,
        )
        self.assertEqual(result.returncode, 0, result.stdout[-3000:] + result.stderr[-3000:])
        self.assertIn('as one chain in one process of 3 blocks', result.stderr)
        run = json.loads(result.stdout[result.stdout.index('{'):])
        self.assertEqual(run['status'], 'completed')
        self.assertEqual([b['status'] for b in run['blocks']], ['completed'] * 3)
        # Each chained block has its own span; the spans follow one another.
        spans = [(b['started_at'], b['finished_at']) for b in run['blocks']]
        for (_, finished), (started, _) in zip(spans, spans[1:]):
            self.assertEqual(finished, started)
        self.assertLessEqual(spans[0][1], spans[-1][1])

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

            from mage_ai.io.config import ConfigFileLoader

            @data_loader
            def load(*args, **kwargs):
                region = os.getenv('AWS_REGION', 'us-east-1')
                bucket = os.environ['BUCKET']
                ConfigFileLoader('io_config.yaml', 'default')
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

    def test_io_config_variables_are_left_out_when_no_block_reads_a_profile(self):
        (self.project / 'io_config.yaml').write_text(
            "default:\n  AWS_ACCESS_KEY_ID: \"{{ env_var('AWS_ACCESS_KEY_ID') }}\"\n"
        )
        self.add('load', 'data_loader', '''
            import os

            @data_loader
            def load(*args, **kwargs):
                return {'bucket': os.getenv('BUCKET')}
        ''')

        captured = export.capture(self.repo_path, [self.pipeline.uuid])

        self.assertEqual([v['name'] for v in captured.manifest['environment']], ['BUCKET'])

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

        use_temporary_mlflow(self, experiments=['scoring'])
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



class RExportTest(ExportTestCase):
    R_TRANSFORM = """
        #* @transformer
        transform <- function(df_1, ...) {
          df_1
        }
    """

    def setUp(self):
        super().setUp()
        self.addCleanup(shutil.rmtree, self.project / 'r', ignore_errors=True)

    def add_r_pipeline(self):
        load = self.add('load', 'data_loader', LOAD.format(project=self.project.name))
        return self.add('r_step', 'transformer', self.R_TRANSFORM, upstream=[load], language='r')

    def test_r_blocks_export_with_their_locked_environment(self):
        self.add_r_pipeline()
        r_dir = self.project / 'r'
        r_dir.mkdir(exist_ok=True)
        (r_dir / 'rproject.toml').write_text('[project]\nname = "p"\nr_version = "4.6"\n')
        (r_dir / 'rv.lock').write_text('version = 2\nr_version = "4.6"\n')
        captured = export.capture(str(self.project), [self.pipeline.uuid], name='r-service')
        self.assertTrue(captured.needs_r)
        self.assertTrue(captured.needs_python)
        self.assertIn('polars', captured.requirements)
        out = export.write(captured, tempfile.mkdtemp(), force=True)
        self.addCleanup(shutil.rmtree, out, ignore_errors=True)
        self.assertEqual(
            (out / 'r' / 'rv.lock').read_text(), (r_dir / 'rv.lock').read_text(),
        )
        runner = out / 'python/mage_ai/data_preparation/models/block/r'
        for name in ('runner.R', 'execution.py', 'mageml/DESCRIPTION', 'mageml/NAMESPACE'):
            self.assertTrue((runner / name).is_file(), name)
        dockerfile = (out / 'Dockerfile').read_text()
        self.assertIn('-slim-trixie', dockerfile)
        self.assertIn('trixie-cran46', dockerfile)
        self.assertIn('rv sync', dockerfile)
        self.assertIn("rt.prepare(rt.r_config())", dockerfile)
        manifest = json.loads((out / 'service.json').read_text())
        languages = {b['uuid'].split('_')[-1]: b['language']
                     for b in manifest['pipelines'][0]['blocks']}
        self.assertEqual(languages['step'], 'r')

    def test_r_system_packages_are_installed_in_the_image(self):
        self.add_r_pipeline()
        r_dir = self.project / 'r'
        r_dir.mkdir(exist_ok=True)
        (r_dir / 'rproject.toml').write_text('[project]\nname = "p"\nr_version = "4.6"\n')
        (r_dir / 'rv.lock').write_text('version = 2\nr_version = "4.6"\n')
        (r_dir / 'system-packages.txt').write_text(
            '# for sf\nlibgdal-dev libgeos-dev\nlibproj-dev  # PROJ\n\nlibgdal-dev\n'
        )
        captured = export.capture(str(self.project), [self.pipeline.uuid], name='r-service')
        out = export.write(captured, tempfile.mkdtemp(), force=True)
        self.addCleanup(shutil.rmtree, out, ignore_errors=True)
        self.assertTrue((out / 'r' / 'system-packages.txt').is_file())
        dockerfile = (out / 'Dockerfile').read_text()
        self.assertIn(
            'zlib1g-dev \\\n      libgdal-dev \\\n      libgeos-dev \\\n      libproj-dev && \\',
            dockerfile,
        )
        self.assertEqual(dockerfile.count('libgdal-dev'), 1)

    def test_r_system_packages_must_be_debian_package_names(self):
        self.add_r_pipeline()
        r_dir = self.project / 'r'
        r_dir.mkdir(exist_ok=True)
        (r_dir / 'rproject.toml').write_text('[project]\nname = "p"\nr_version = "4.6"\n')
        (r_dir / 'rv.lock').write_text('version = 2\nr_version = "4.6"\n')
        (r_dir / 'system-packages.txt').write_text('libgdal-dev\n$(curl evil)\n&&rm\n')
        with self.assertRaises(export.ExportError) as caught:
            export.capture(str(self.project), [self.pipeline.uuid], name='r-service')
        self.assertIn('$(curl', str(caught.exception))
        self.assertIn('&&rm', str(caught.exception))
        self.assertNotIn('libgdal-dev,', str(caught.exception))

    def test_r_blocks_without_a_locked_environment_are_explained(self):
        self.add_r_pipeline()
        with self.assertRaises(export.ExportError) as caught:
            export.capture(str(self.project), [self.pipeline.uuid], name='r-service')
        self.assertIn('mage r init', str(caught.exception))
        (self.project / 'r').mkdir(exist_ok=True)
        (self.project / 'r' / 'rproject.toml').write_text('[project]\nname = "p"\n')
        with self.assertRaises(export.ExportError) as caught:
            export.capture(str(self.project), [self.pipeline.uuid], name='r-service')
        self.assertIn('rv.lock', str(caught.exception))

@unittest.skipUnless(shutil.which('cargo'), 'needs cargo')
class FakeDistribution:
    """A distribution that counts how often its metadata is parsed."""

    def __init__(self, root: Path, name: str, version: str, files: int):
        self.root = root
        self.name = name
        self._version = version
        self.files = [Path(name, f'module_{i}.py') for i in range(files)]
        self.parses = 0

    @property
    def metadata(self):
        self.parses += 1
        return {'Name': self.name, 'Version': self._version}

    @property
    def version(self):
        return self.metadata['Version']

    def read_text(self, filename):
        return None

    def locate_file(self, file):
        return self.root / file


class FileOwnersTest(unittest.TestCase):
    def test_each_distribution_is_read_once_and_the_first_of_a_name_owns_its_files(self):
        """
        The owner of every file read the distribution's version, which parses its METADATA:
        37,000 parses, 13 seconds per export, and 10 to 24 minutes per export test in a full
        test run, where the importlib_metadata backport lists every distribution twice.
        """
        from unittest.mock import patch

        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        first = FakeDistribution(root / 'a', 'Pandas', '3.0.6', files=50)
        shadowed = FakeDistribution(root / 'b', 'pandas', '2.3.0', files=50)
        other = FakeDistribution(root / 'a', 'polars', '2.0.1', files=10)

        with patch.object(
            export.importlib.metadata, 'distributions', return_value=[first, shadowed, other],
        ):
            owners = export._read_file_owners()

        self.assertEqual(first.parses, 1)
        self.assertLessEqual(shadowed.parses, 1)
        self.assertEqual(other.parses, 1)
        self.assertEqual(
            owners[os.path.realpath(root / 'a' / 'Pandas' / 'module_0.py')], ('pandas', '3.0.6'),
        )
        self.assertNotIn(os.path.realpath(root / 'b' / 'pandas' / 'module_0.py'), owners)
        self.assertEqual(len(owners), 60)


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
        self.assertIn('cargo build --release --locked --manifest-path rust/Cargo.toml '
                      '-p mage-blocks',
                      export._dockerfile(self.capture))

    def test_a_lock_cargo_cannot_resolve_builds_unlocked(self):
        (self.workspace / 'blocks' / 'block_a' / 'Cargo.toml').write_text(
            '[package]\nname = "block_a"\nversion = "0.1.0"\nedition = "2024"\n'
            '[dependencies]\nmissing = { path = "../nowhere" }\n'
        )
        self.assertFalse(export._sync_cargo_lock(self.capture, self.workspace))
        self.assertIn('could not be updated', ' '.join(self.capture.notes))
        self.assertIn('cargo build --release --manifest-path rust/Cargo.toml -p mage-blocks',
                      export._dockerfile(self.capture))


@unittest.skipUnless(shutil.which('cargo'), 'needs cargo')
class RustBundleTest(unittest.TestCase):
    """The service's Rust blocks become one binary that runs the block MAGE_BLOCK names."""

    def test_the_bundle_runs_each_block(self):
        from mage_ai.data_preparation.models.block.rust import source as block_source
        from mage_ai.data_preparation.models.block.rust import workspace as ws

        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        (root / 'Cargo.toml').write_text(
            '[workspace]\nresolver = "3"\nmembers = [".mage/blocks/*"]\n\n'
            '[workspace.dependencies]\nmage-block = { path = ".mage/sdk" }\n'
        )
        # A stand-in for the mage crate: main calls the block function and exits.
        sdk = root / '.mage' / 'sdk'
        (sdk / 'src').mkdir(parents=True)
        (sdk / 'Cargo.toml').write_text(
            '[package]\nname = "mage-block"\nversion = "0.1.0"\nedition = "2024"\n'
            '[lib]\nname = "mage"\n'
        )
        (sdk / 'src' / 'lib.rs').write_text(
            'pub fn main(block: fn(), _tests: Vec<()>) -> ! { block(); std::process::exit(0) }\n'
            'pub struct ChainBlock(fn());\n'
            'pub fn chain_block(block: fn(), _tests: Vec<()>) -> ChainBlock { ChainBlock(block) }\n'
            'pub fn run_chain(entries: Vec<(&\'static str, fn() -> ChainBlock)>) -> ! {\n'
            '    for (_, make) in entries { (make().0)() }\n'
            '    std::process::exit(0)\n'
            '}\n'
        )
        crates = []
        for name in ('a', 'b'):
            crate = ws.crate_name(name)
            crates.append(crate)
            directory = root / '.mage' / 'blocks' / crate
            (directory / 'src').mkdir(parents=True)
            (directory / 'Cargo.toml').write_text(ws.block_manifest(crate, ['mage-block']))
            code = f'fn helper() -> &\'static str {{ "block {name}" }}\n' \
                f'fn load_data() {{ println!("{{}}", helper()); }}\n'
            (directory / 'src' / 'main.rs').write_text(
                f'{code}include!("{block_source.MAIN_FILE}");\n'
            )
            (directory / 'src' / block_source.MAIN_FILE).write_text(
                f'{block_source.GENERATED_MARKER}\nfn main() {{\n'
                '    mage::main(load_data, vec![\n    ]);\n}\n'
            )
            export._block_library(directory)
        export._write_bundle(root, crates)
        self.assertIn('".mage/bundle"', (root / 'Cargo.toml').read_text())
        target = root / 'target'
        subprocess.run(
            ['cargo', 'build', '-q', '-p', export.RUST_BUNDLE], cwd=root, check=True,
            env=dict(os.environ, CARGO_TARGET_DIR=str(target)),
        )
        binary = target / 'debug' / export.RUST_BUNDLE
        for name, crate in zip(('a', 'b'), crates):
            result = subprocess.run([binary, 'job'], capture_output=True, text=True,
                                    env=dict(os.environ, MAGE_BLOCK=crate))
            self.assertEqual((result.returncode, result.stdout), (0, f'block {name}\n'))
        chain = subprocess.run([binary, 'job'], capture_output=True, text=True,
                               env=dict(os.environ, MAGE_CHAIN='1'))
        self.assertEqual((chain.returncode, chain.stdout), (0, 'block a\nblock b\n'))
        result = subprocess.run([binary, 'job'], capture_output=True, text=True,
                                env=dict(os.environ, MAGE_BLOCK='block_missing'))
        self.assertEqual(result.returncode, 2)
        self.assertIn('no Rust block "block_missing"', result.stderr)


class RustIoConfigTest(unittest.TestCase):
    """A Rust block's io_config profile goes into the manifest with its variables kept as
    references, which the service fills in from its environment for each run."""

    CONFIG = textwrap.dedent('''
        default:
          POSTGRES_HOST: "{{ env_var('PG_HOST') }}"
          POSTGRES_PORT: {{ env_var('PG_PORT', 5432) }}
          POSTGRES_PASSWORD: "{{ mage_secret_var('pg password') }}"
          POSTGRES_DBNAME: shop
          POSTGRES_CONNECT_TIMEOUT: 10
          POSTGRES_SCHEMA:
          URL: "postgresql://{{ env_var('PG_USER', 'mage') }}@db/shop"
        other:
          POSTGRES_HOST: elsewhere
    ''')

    def test_variables_and_secrets_become_references(self):
        self.assertEqual(export.compile_io_config(self.CONFIG, 'default'), {
            'POSTGRES_HOST': {'$parts': [{'env': 'PG_HOST'}]},
            'POSTGRES_PORT': {'$parts': [{'env': 'PG_PORT', 'default': 5432}]},
            'POSTGRES_PASSWORD': {'$parts': [{'env': 'MAGE_SECRET_PG_PASSWORD'}]},
            'POSTGRES_DBNAME': 'shop',
            'POSTGRES_CONNECT_TIMEOUT': 10,
            'URL': {'$parts': ['postgresql://', {'env': 'PG_USER', 'default': 'mage'},
                               '@db/shop']},
        })
        self.assertEqual(export.compile_io_config(self.CONFIG, 'other'),
                         {'POSTGRES_HOST': 'elsewhere'})
        self.assertIsNone(export.compile_io_config(self.CONFIG, 'missing'))

    def test_templates_a_service_cannot_fill_in_are_explained(self):
        text = 'default:\n  KEY: "{{ json_value(env_var(\'X\'), \'k\') }}"\n'
        with self.assertRaisesRegex(ValueError, 'json_value'):
            export.compile_io_config(text, 'default')
        with self.assertRaisesRegex(ValueError, 'unknown_function'):
            export.compile_io_config('default:\n  A: "{{ unknown_function() }}"\n', 'default')

    def test_only_blocks_that_read_the_profile_need_it(self):
        from types import SimpleNamespace

        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory)
            (project / 'io_config.yaml').write_text('default:\n  A: "{{ json_value(1) }}"\n')
            reads = project / 'reads.rs'
            reads.write_text('fn load_data() -> Result<DataFrame> { Postgres::connect()?; }\n')
            plain = project / 'plain.rs'
            plain.write_text('fn load_data() -> Result<DataFrame> { todo!() }\n')
            block = SimpleNamespace(configuration={})
            problems = []
            self.assertIsNone(export._rust_io_config(block, plain, project, problems, 'a'))
            self.assertEqual(problems, [])
            self.assertIsNone(export._rust_io_config(block, reads, project, problems, 'b'))
            self.assertEqual(len(problems), 1)
            self.assertIn('json_value', problems[0])
