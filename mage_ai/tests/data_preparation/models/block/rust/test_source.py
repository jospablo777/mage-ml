import json
import os
import tempfile
from pathlib import Path

import pandas as pd
import polars as pl

from mage_ai.data_preparation.models.block.rust import exchange
from mage_ai.data_preparation.models.block.rust import source as block_source
from mage_ai.data_preparation.models.block.rust import workspace as ws
from mage_ai.data_preparation.models.constants import BlockType
from mage_ai.tests.base_test import TestCase

TRANSFORMER = '''use mage::prelude::*;

// fn load_data() in a comment is not a function.
fn transform(data: LazyFrame) -> Result<LazyFrame> {
    let text = "fn export_data(";
    Ok(data)
}

fn test_rows(output: &DataFrame) -> Result<()> {
    Ok(())
}

pub fn test_columns(output: &DataFrame) -> bool {
    true
}
'''


class SourceTest(TestCase):
    def test_the_block_function_and_tests_are_found(self):
        source = block_source.analyze(TRANSFORMER, BlockType.TRANSFORMER)
        self.assertEqual(source.function, 'transform')
        self.assertEqual(source.tests, ['test_rows', 'test_columns'])
        self.assertEqual(block_source.function_line(TRANSFORMER, 'transform'), 4)

    def test_functions_in_comments_and_strings_do_not_count(self):
        self.assertEqual(
            block_source.function_names(TRANSFORMER),
            ['transform', 'test_rows', 'test_columns'],
        )
        with self.assertRaisesRegex(block_source.RustSourceError, 'no `load_data` function'):
            block_source.analyze(TRANSFORMER, BlockType.DATA_LOADER)

    def test_a_missing_function_names_the_ones_defined(self):
        with self.assertRaisesRegex(
            block_source.RustSourceError, r'defines `fn transform\(\.\.\.\)`; it defines helper',
        ):
            block_source.analyze('fn helper() {}\n', BlockType.TRANSFORMER)

    def test_a_main_function_is_rejected(self):
        with self.assertRaisesRegex(block_source.RustSourceError, 'Mage generates it'):
            block_source.analyze('fn main() {}\nfn transform() {}\n', BlockType.TRANSFORMER)

    def test_the_generated_main_keeps_the_block_lines(self):
        source = block_source.analyze(TRANSFORMER, BlockType.TRANSFORMER)
        main = block_source.main_rs(source)
        self.assertTrue(main.startswith(TRANSFORMER))
        self.assertEqual(main[len(TRANSFORMER):], 'include!("mage_main.rs");\n')
        generated = block_source.generated_main(source)
        self.assertIn('mage::main(transform, vec![', generated)
        self.assertIn('mage::test("test_rows", test_rows),', generated)


class WorkspaceTest(TestCase):
    def test_init_creates_the_environment_and_keeps_edits(self):
        repo = tempfile.mkdtemp()
        workspace = ws.init(repo)
        self.assertTrue(workspace.cargo_toml.exists())
        self.assertIn('1.98.0', workspace.toolchain_toml.read_text())
        self.assertEqual((workspace.root / '.gitignore').read_text(), '.mage/\n')
        self.assertTrue((workspace.sdk / 'src' / 'lib.rs').exists())
        self.assertFalse((workspace.sdk / 'target').exists())
        self.assertIn('rayon', ws.dependencies(workspace))
        # New workspaces start from the SDK's tested versions.
        self.assertIn('name = "polars"', workspace.cargo_lock.read_text())

        workspace.cargo_toml.write_text(
            workspace.cargo_toml.read_text().replace('rayon = "1"', 'rayon = "1"\nregex = "1"'),
        )
        ws.init(repo)
        self.assertIn('regex', ws.dependencies(workspace))

    def test_the_sdk_is_copied_again_only_when_it_changes(self):
        workspace = ws.init(tempfile.mkdtemp())
        lib = workspace.sdk / 'src' / 'lib.rs'
        modified = lib.stat().st_mtime_ns
        ws.sync_sdk(workspace)
        self.assertEqual(lib.stat().st_mtime_ns, modified)

    def test_a_workspace_without_the_sdk_dependency_is_explained(self):
        workspace = ws.init(tempfile.mkdtemp())
        workspace.cargo_toml.write_text('[workspace]\nmembers = []\n')
        with self.assertRaisesRegex(ws.RustWorkspaceError, 'must list mage-block'):
            ws.dependencies(workspace)

    def test_crate_names_are_valid_and_distinct(self):
        self.assertRegex(ws.crate_name('Load Orders!'), r'^block_load_orders_[0-9a-f]{8}$')
        self.assertNotEqual(ws.crate_name('a-b'), ws.crate_name('a_b'))

    def test_block_crates_depend_on_the_workspace_crates(self):
        manifest = ws.block_manifest('block_x', ['mage-block', 'regex'])
        self.assertIn('mage-block = { workspace = true }', manifest)
        self.assertIn('regex = { workspace = true }', manifest)
        self.assertIn('dead_code = "allow"', manifest)

    def test_files_are_rewritten_only_when_they_change(self):
        workspace = ws.init(tempfile.mkdtemp())
        directory = ws.write_block_crate(workspace, 'block_x', {'main.rs': 'fn x() {}\n'})
        main = directory / 'src' / 'main.rs'
        modified = main.stat().st_mtime_ns
        ws.write_block_crate(workspace, 'block_x', {'main.rs': 'fn x() {}\n'})
        self.assertEqual(main.stat().st_mtime_ns, modified)


class ExchangeTest(TestCase):
    def setUp(self):
        super().setUp()
        self.directory = tempfile.mkdtemp()

    def test_tables_cross_as_arrow_files(self):
        entries = exchange.write_inputs([
            pl.DataFrame({'a': [1, None]}),
            pd.DataFrame({'b': ['x', None]}),
            None,
            {'rate': 2},
        ], self.directory)
        self.assertEqual([entry['kind'] for entry in entries], ['frame', 'frame', 'empty', 'json'])
        self.assertEqual(
            pl.read_ipc(os.path.join(self.directory, entries[1]['path'])).to_dicts(),
            [{'b': 'x'}, {'b': None}],
        )
        with open(os.path.join(self.directory, entries[3]['path'])) as file:
            self.assertEqual(json.load(file), {'rate': 2})

    def test_unsupported_values_name_their_type(self):
        with self.assertRaisesRegex(exchange.RustExchangeError, 'is of type object'):
            exchange.write_inputs([object()], self.directory)

    def test_variables_that_json_cannot_hold_are_skipped(self):
        values, skipped = exchange.variables_for_rust({'a': 1, 'b': float('nan'), 'c': {'d': 2}})
        self.assertEqual(values, {'a': 1, 'c': {'d': 2}})
        self.assertEqual(skipped, ['b'])

    def test_outputs_are_read_back(self):
        frame = pl.DataFrame({'a': [1, 2]})
        frame.write_ipc(os.path.join(self.directory, 'out.arrow'), compression='uncompressed')
        Path(self.directory, 'out.json').write_text('[1, 2]')
        outputs, tests = exchange.read_outputs(dict(outputs=[
            dict(kind='frame', format='ipc', path='out.arrow'),
            dict(kind='json', path='out.json'),
            dict(kind='decision', value=True),
        ], tests=[dict(name='t', passed=True)]), self.directory)
        self.assertEqual(outputs[0].to_dicts(), [{'a': 1}, {'a': 2}])
        self.assertEqual(outputs[1:], [[1, 2], True])
        self.assertEqual(tests, [dict(name='t', passed=True)])


class ProjectFilesTest(TestCase):
    def test_rust_blocks_are_found_with_their_types_and_uuids(self):
        from mage_ai.data_preparation.models.block.rust import project

        repo = Path(tempfile.mkdtemp())
        for relative in ('transformers/clean.rs', 'data_loaders/files/read.rs',
                         'transformers/other.py', 'rust/.mage/blocks/x/src/main.rs'):
            (repo / relative).parent.mkdir(parents=True, exist_ok=True)
            (repo / relative).write_text('')
        found = [(item.block_type, item.uuid, item.label) for item in project.block_files(repo)]
        self.assertEqual(found, [
            (BlockType.DATA_LOADER, 'files/read', 'data_loaders/files/read.rs'),
            (BlockType.TRANSFORMER, 'clean', 'transformers/clean.rs'),
        ])
