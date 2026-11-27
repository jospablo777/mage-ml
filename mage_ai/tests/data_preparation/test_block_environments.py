"""
Named R and Rust environments (ADR-0040): names, selection, usage, and blocks that run in
them. The Rust runs build real crates and are skipped where cargo is not installed.
"""
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml

from mage_ai.data_preparation import block_environments as envs
from mage_ai.data_preparation.models.block import plans
from mage_ai.data_preparation.models.constants import BlockLanguage, BlockType


def _block(language, configuration=None, pipeline_environment=None, uuid='b'):
    return SimpleNamespace(
        uuid=uuid,
        language=language,
        configuration=configuration or {},
        pipeline=SimpleNamespace(environment=pipeline_environment),
    )


class NamesTest(unittest.TestCase):
    def setUp(self):
        self.repo = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.repo, True)

    def make(self, language, name):
        folder = Path(self.repo, envs.FOLDERS[language], name)
        folder.mkdir(parents=True)
        (folder / envs.MANIFESTS[language]).write_text('')
        return folder

    def test_names_are_folder_words_and_default_means_the_project_environment(self):
        self.assertEqual(envs.normalized(None), 'default')
        self.assertEqual(envs.normalized(' Default '), 'default')
        self.assertEqual(envs.normalized('Torch_2'), 'torch_2')
        for bad in ('../x', 'a b', '-x', 'x' * 65):
            with self.assertRaises(envs.EnvironmentNameError):
                envs.normalized(bad)

    def test_a_missing_environment_names_the_ones_that_exist_and_how_to_create_it(self):
        self.make('r', 'models')
        self.assertEqual(envs.require(self.repo, 'r', 'models'), 'models')
        self.assertEqual(envs.require(self.repo, 'r', None), 'default')
        with self.assertRaises(envs.EnvironmentNameError) as context:
            envs.require(self.repo, 'r', 'old_models')
        message = str(context.exception)
        self.assertIn('There is no R environment named old_models', message)
        self.assertIn('default, models', message)
        self.assertIn('mage r init --environment old_models', message)

    def test_a_folder_without_its_manifest_is_not_an_environment(self):
        self.make('rust', 'torch')
        Path(self.repo, 'rust_environments', 'empty').mkdir()
        self.assertEqual(envs.named(self.repo, 'rust'), ['torch'])

    def test_the_block_selects_first_then_its_pipeline_then_default(self):
        self.assertEqual(envs.selection(_block('r')), 'default')
        self.assertEqual(envs.selection(_block('r', pipeline_environment=dict(r='p'))), 'p')
        self.assertEqual(
            envs.selection(_block('r', dict(r_environment='b'), dict(r='p'))), 'b')
        # A Rust block reads the Rust settings only.
        self.assertEqual(
            envs.selection(_block(BlockLanguage.RUST, dict(r_environment='b'), dict(r='p'))),
            'default')
        self.assertEqual(
            envs.selection(_block(BlockLanguage.RUST, pipeline_environment=dict(rust='t'))), 't')

    def test_usage_lists_the_pipelines_and_blocks_of_each_environment(self):
        folder = Path(self.repo, 'pipelines', 'churn')
        folder.mkdir(parents=True)
        (folder / 'metadata.yaml').write_text(yaml.safe_dump(dict(
            name='Churn',
            environment=dict(r='models'),
            blocks=[
                dict(uuid='fit', language='r', configuration={}),
                dict(uuid='plot', language='r', configuration=dict(r_environment='default')),
                dict(uuid='load', language='python'),
                dict(uuid='score', language='rust', configuration=dict(rust_environment='t')),
            ],
        )))
        self.assertEqual(envs.usage(self.repo, 'r'), {
            'models': [dict(pipeline='churn', name='Churn', blocks=['fit'], default=True)],
            'default': [dict(pipeline='churn', name='Churn', blocks=['plot'], default=False)],
        })
        self.assertEqual(envs.of_block_files(self.repo, 'rust'), {'score': ['t']})


class ChainsAndPythonTest(unittest.TestCase):
    def test_blocks_chain_only_within_one_environment(self):
        first = _block(BlockLanguage.R, dict(r_environment='models'))
        same = _block(BlockLanguage.R, dict(r_environment='models'))
        other = _block(BlockLanguage.R)
        self.assertTrue(plans.chains_with(first, same))
        self.assertFalse(plans.chains_with(first, other))
        rust = _block(BlockLanguage.RUST, pipeline_environment=dict(rust='t'))
        self.assertTrue(plans.chains_with(rust, _block(BlockLanguage.RUST, dict(
            rust_environment='t'))))
        self.assertFalse(plans.chains_with(rust, _block(BlockLanguage.RUST)))

    def test_a_pipeline_that_sets_only_r_or_rust_builds_no_python_environment(self):
        from mage_ai.data_preparation import environments
        from mage_ai.data_preparation.models.pipeline import normalized_environment

        pipeline = SimpleNamespace(uuid='p', environment=normalized_environment(
            dict(r=' models ', rust='torch', worker='')))
        self.assertEqual(pipeline.environment, dict(r='models', rust='torch'))
        self.assertIsNone(environments.pipeline_environment(pipeline))


class RConfigTest(unittest.TestCase):
    def setUp(self):
        self.repo = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.repo, True)
        for folder in ('r', 'r_environments/models'):
            Path(self.repo, folder).mkdir(parents=True)
        Path(self.repo, 'r', 'rproject.toml').write_text('[project]\nr_version = "4.6"\n')
        Path(self.repo, 'r_environments', 'models', 'rproject.toml').write_text(
            '[project]\nr_version = "4.3"\n')

    @patch('mage_ai.data_preparation.models.block.r.runtime.choose_rscript')
    def test_a_named_environment_has_its_own_project_and_r_version(self, choose):
        from mage_ai.data_preparation.models.block.r import runtime

        choose.side_effect = lambda version: f'Rscript-{version}'
        with patch.dict(os.environ, {'MAGE_R_PROJECT_DIR': ''}):
            default = runtime.r_config(self.repo)
            named = runtime.r_config(self.repo, 'models')
        self.assertEqual(default.project_dir, Path(self.repo, 'r').resolve())
        self.assertEqual(default.rscript, 'Rscript-4.6')
        self.assertEqual(named.project_dir, Path(self.repo, 'r_environments', 'models').resolve())
        self.assertEqual(named.rscript, 'Rscript-4.3')
        with self.assertRaises(runtime.REnvironmentError) as context:
            runtime.r_config(self.repo, 'nope')
        self.assertIn('the project has default, models', str(context.exception))


class RustNamesTest(unittest.TestCase):
    def test_a_block_in_a_named_environment_gets_its_own_crate(self):
        from mage_ai.data_preparation.models.block.rust import build as rust_build
        from mage_ai.data_preparation.models.block.rust import workspace as ws

        self.assertEqual(ws.crate_name('b'), ws.crate_name('b', 'default'))
        self.assertNotEqual(ws.crate_name('b'), ws.crate_name('b', 'torch'))
        self.assertNotEqual(rust_build.chain_crate_name(['a', 'b']),
                            rust_build.chain_crate_name(['a', 'b'], 'torch'))
        self.assertEqual(ws.workspace_root('/p', 'torch'), Path('/p/rust_environments/torch'))

    def test_a_missing_rust_environment_fails_the_build_with_the_names(self):
        from mage_ai.data_preparation.models.block.rust import build as rust_build

        repo = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo, True)
        with self.assertRaises(rust_build.RustBuildError) as context:
            rust_build.prepare('fn transform(d: LazyFrame) -> LazyFrame { d }',
                               BlockType.TRANSFORMER, 'b', repo, environment='torch')
        self.assertIn('There is no Rust environment named torch', str(context.exception))
        self.assertFalse(Path(repo, 'rust_environments', 'torch').exists())


ITOA = '''use mage::prelude::*;

fn transform(data: LazyFrame) -> LazyFrame {
    let mut buffer = itoa::Buffer::new();
    let text = buffer.format(42).to_string();
    data.with_columns([lit(text).alias("answer")])
}
'''


@unittest.skipIf(shutil.which('cargo') is None, 'Rust is not installed')
class RustEnvironmentRunTest(unittest.TestCase):
    def test_a_crate_listed_in_a_named_environment_serves_only_its_blocks(self):
        import polars as pl

        from mage_ai.data_preparation.models.block.rust import (
            RustBlockError,
            execute_rust_code,
        )
        from mage_ai.data_preparation.models.block.rust import workspace as ws

        repo = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo, True)
        created = ws.init(repo, 'fmt')
        manifest = created.cargo_toml.read_text()
        created.cargo_toml.write_text(
            manifest.replace('rayon = "1"\n', 'rayon = "1"\nitoa = "1"\n'))

        def run(environment):
            return execute_rust_code(
                BlockType.TRANSFORMER, ITOA, block_uuid='answer', repo_path=repo,
                input_vars=[pl.DataFrame(dict(id=[1, 2]))], label='transformers/answer.rs',
                environment=environment,
            )

        output = run('fmt').outputs[0]
        self.assertEqual(list(output['answer']), ['42', '42'])
        with self.assertRaises(RustBlockError) as context:
            run(None)
        message = str(context.exception)
        self.assertIn('Add itoa there', message)
        self.assertIn('Or run the block in the fmt environment, which lists it.', message)
