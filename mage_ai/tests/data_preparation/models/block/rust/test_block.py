"""
Rust blocks built and run with cargo. Skipped where Rust is not installed; the first run
compiles the dependencies, a few minutes, into the shared target directory.
"""
import glob
import os
import re
import shutil
import tempfile
import time
import unittest
from unittest.mock import patch

import pandas as pd
import polars as pl
import psutil

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.block.rust import (
    RustBlock,
    RustBlockError,
    RustConditionalBlock,
)
from mage_ai.data_preparation.models.block.rust import build as rust_build
from mage_ai.data_preparation.models.block.rust import execute_rust_code
from mage_ai.data_preparation.models.constants import BlockLanguage, BlockType
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.data_preparation.models.variables.constants import VariableType
from mage_ai.data_preparation.templates.template import template_env
from mage_ai.tests.base_test import AsyncDBTestCase, DBTestCase
from mage_ai.tests.factory import create_user

ANSI = re.compile(r'\x1b\[[0-9;]*m')
TEMPLATES = os.path.join(
    os.path.dirname(__file__), '..', '..', '..', '..', '..', 'data_preparation', 'templates',
)

FILTER = '''use mage::prelude::*;

fn transform(orders: LazyFrame, vars: Vars) -> Result<LazyFrame> {
    let minimum: f64 = vars.get_or("minimum", 0.0)?;
    Ok(orders
        .filter(col("amount").gt(lit(minimum)))
        .with_columns([(col("amount") * lit(2.0)).alias("doubled")]))
}

fn test_positive(output: &DataFrame) -> Result<()> {
    ensure!(output.column("amount")?.f64()?.min().unwrap_or(1.0) > 0.0, "negative amount");
    Ok(())
}
'''


def plain(text: str) -> str:
    return ANSI.sub('', text)


@unittest.skipIf(shutil.which('cargo') is None, 'Rust is not installed')
class RustBlockTest(DBTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # One project for the class: blocks share its workspace and compiled crates.
        cls.rust_repo = tempfile.mkdtemp()

    def run_code(self, code, block_type=BlockType.TRANSFORMER, inputs=None, variables=None,
                 uuid='block'):
        return execute_rust_code(
            block_type,
            code,
            block_uuid=uuid,
            global_vars=variables or {},
            input_vars=inputs or [],
            label=f'transformers/{uuid}.rs',
            repo_path=self.rust_repo,
        )

    def test_every_template_compiles_and_runs(self):
        frame = pl.DataFrame({
            'amount': [3.0, 4.0, 4.0, 1.0],
            'category': ['a', 'b', 'b', 'a'],
            'customer_id': [1, 2, 2, 3],
            'id': [1, 2, 2, None],
            'name': [' x ', 'y', 'y', None],
            'value': [1.0, None, 2.5, 4.0],
        })
        customers = pl.DataFrame({'customer_id': [1, 2], 'segment': ['s', 't']})
        data_file = os.path.join(self.rust_repo, 'data.parquet')
        frame.write_parquet(data_file)
        folders = {
            'conditionals': BlockType.CONDITIONAL,
            'custom': BlockType.CUSTOM,
            'data_exporters': BlockType.DATA_EXPORTER,
            'data_loaders': BlockType.DATA_LOADER,
            'transformers': BlockType.TRANSFORMER,
        }
        paths = sorted(glob.glob(os.path.join(TEMPLATES, '*', 'rust', '*')))
        self.assertGreaterEqual(len(paths), 13)
        url = self.serve_records(frame)
        for path in paths:
            folder = path.split(os.sep)[-3]
            name = os.path.basename(path)
            with self.subTest(template=f'{folder}/{name}'):
                code = template_env.get_template(f'{folder}/rust/{name}').render(code='')
                block_type = folders[folder]
                if block_type in (BlockType.DATA_LOADER, BlockType.CUSTOM, BlockType.CONDITIONAL):
                    inputs = []
                elif name == 'join.rs':
                    inputs = [frame, customers]
                else:
                    inputs = [frame]
                run = self.run_code(
                    code,
                    block_type=block_type,
                    inputs=inputs,
                    uuid=f'template_{folder}_{name}'.replace('.', '_'),
                    variables=dict(
                        output_path=os.path.join(self.rust_repo, 'exported.parquet'),
                        path=data_file,
                        url=url,
                        batch_size=3,
                    ),
                )
                self.assertTrue(all(test['passed'] for test in run.tests), run.tests)
                if block_type == BlockType.CONDITIONAL:
                    self.assertEqual(run.outputs, [True])
        # The API exporter sent every row, in batches of 3.
        self.assertEqual([len(batch) for batch in self.received], [3, 1])

    def serve_records(self, frame) -> str:
        """An HTTP API on localhost: GET returns the frame's rows, POST records them."""
        import http.server
        import json as json_module
        import threading

        rows = frame.to_dicts()
        received = self.received = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = json_module.dumps({'data': rows}).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                length = int(self.headers['Content-Length'])
                received.append(json_module.loads(self.rfile.read(length)))
                self.send_response(204)
                self.end_headers()

            def log_message(self, *args):
                pass

        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        return f'http://127.0.0.1:{server.server_address[1]}/records'

    def test_a_transformer_reads_tables_and_variables_and_runs_its_tests(self):
        orders = pd.DataFrame({'id': [1, 2, 3], 'amount': [10.0, -2.0, 7.5]})
        run = self.run_code(FILTER, inputs=[orders], variables={'minimum': 5.0}, uuid='filter')
        self.assertIsInstance(run.outputs[0], pl.DataFrame)
        self.assertEqual(run.outputs[0].to_dicts(), [
            {'id': 1, 'amount': 10.0, 'doubled': 20.0},
            {'id': 3, 'amount': 7.5, 'doubled': 15.0},
        ])
        self.assertEqual(run.tests, [{'name': 'test_positive', 'passed': True, 'message': None}])

        # Unchanged code runs its cached binary, without cargo.
        with patch.object(rust_build, '_run_cargo', side_effect=AssertionError('cargo ran')):
            again = self.run_code(FILTER, inputs=[orders], uuid='filter')
        self.assertTrue(again.cached_build)

    def test_a_compile_error_points_at_the_block_line(self):
        code = (
            'use mage::prelude::*;\n'
            '\n'
            'fn transform(data: LazyFrame) -> LazyFrame {\n'
            '    let x: i64 = "text";\n'
            '    data\n'
            '}\n'
        )
        with self.assertRaises(RustBlockError) as context:
            self.run_code(code, uuid='type_error')
        message = plain(str(context.exception))
        self.assertIn('The Rust block does not compile', message)
        self.assertIn('--> transformers/type_error.rs:4:18', message)
        self.assertIn('expected `i64`, found `&str`', message)
        self.assertNotIn('.mage/blocks', message)

    def test_a_wrong_signature_is_explained(self):
        code = (
            'use mage::prelude::*;\n'
            '\n'
            'fn transform(data: String) -> LazyFrame {\n'
            '    todo!()\n'
            '}\n'
        )
        with self.assertRaises(RustBlockError) as context:
            self.run_code(code, uuid='signature')
        message = plain(str(context.exception))
        self.assertIn('this function cannot be a Mage block function', message)
        self.assertIn('(main generated by Mage)', message)
        self.assertIn('parameters can be LazyFrame, DataFrame', message)

    def test_a_panic_reports_its_location_once(self):
        code = (
            'use mage::prelude::*;\n\nfn transform(data: DataFrame) -> DataFrame {\n'
            '    let values: Vec<i64> = vec![];\n    let _first = values[3];\n    data\n}\n'
        )
        with patch('sys.stdout.write') as written:
            with self.assertRaises(RustBlockError) as context:
                self.run_code(code, inputs=[pl.DataFrame({'a': [1]})], uuid='panics')
        self.assertEqual(
            str(context.exception),
            'Rust block panics panicked at transformers/panics.rs:5:24: index out of bounds: '
            'the len is 0 but the index is 3',
        )
        printed = ''.join(call.args[0] for call in written.call_args_list)
        self.assertNotIn('panicked', printed)

    def test_an_error_names_the_upstream_output_not_a_temporary_file(self):
        code = (
            'use mage::prelude::*;\n\nfn transform(data: LazyFrame) -> Result<DataFrame> {\n'
            '    data.select([col("missing")]).collect().context("Selecting")\n}\n'
        )
        with self.assertRaises(RustBlockError) as context:
            self.run_code(code, inputs=[pl.DataFrame({'a': [1]})], uuid='missing_column')
        message = str(context.exception)
        self.assertIn('Rust block missing_column failed: Selecting', message)
        self.assertIn('unable to find column "missing"', message)
        self.assertIn('<upstream output 1>', message)
        self.assertNotIn('mage_rust_', message)

    def test_a_block_past_its_timeout_is_stopped(self):
        code = (
            'use mage::prelude::*;\n\nfn transform() -> Result<()> {\n'
            '    std::thread::sleep(std::time::Duration::from_secs(60));\n    Ok(())\n}\n'
        )
        # Built first, so the clock measures the run, not the compile.
        rust_build.build(rust_build.prepare(
            code, BlockType.TRANSFORMER, 'sleeps', self.rust_repo, label='transformers/sleeps.rs',
        ), stream=False)
        before = {process.pid for process in psutil.Process().children(recursive=True)}
        started = time.monotonic()
        with patch.dict(os.environ, {'MAGE_RUST_TIMEOUT_SECONDS': '2'}):
            with self.assertRaisesRegex(RustBlockError, 'longer than 2 seconds'):
                self.run_code(code, uuid='sleeps')
        self.assertLess(time.monotonic() - started, 20)
        after = {process.pid for process in psutil.Process().children(recursive=True)}
        self.assertEqual(after - before, set())

    def test_check_returns_diagnostics_for_the_editor(self):
        code = (
            'use mage::prelude::*;\n'
            '\n'
            'fn transform(data: LazyFrame) -> LazyFrame {\n'
            '    let unused = 1;\n'
            '    undefined_function(data)\n'
            '}\n'
        )
        prepared = rust_build.prepare(
            code, BlockType.TRANSFORMER, 'checked', self.rust_repo, label='transformers/c.rs',
        )
        diagnostics = rust_build.check(prepared)
        errors = [item for item in diagnostics if item.level == 'error']
        warnings = [item for item in diagnostics if item.level == 'warning']
        self.assertEqual(errors[0].line, 5)
        self.assertEqual(errors[0].column, 5)
        self.assertIn('undefined_function', errors[0].message)
        # rustc reports lints such as unused variables once the code type-checks.
        self.assertEqual(warnings, [])

        code = (
            'use mage::prelude::*;\n'
            '\n'
            'fn transform(data: LazyFrame) -> LazyFrame {\n'
            '    let unused = 1;\n'
            '    data\n'
            '}\n'
        )
        prepared = rust_build.prepare(
            code, BlockType.TRANSFORMER, 'checked', self.rust_repo, label='transformers/c.rs',
        )
        diagnostics = rust_build.check(prepared)
        self.assertEqual([item.level for item in diagnostics], ['warning'])
        self.assertEqual((diagnostics[0].line, diagnostics[0].column), (4, 9))
        self.assertIn('unused variable', diagnostics[0].message)

    def test_build_all_builds_every_block_and_reports_the_broken_ones(self):
        from mage_ai.data_preparation.models.block.rust import project

        repo = tempfile.mkdtemp()
        os.makedirs(os.path.join(repo, 'transformers'))
        with open(os.path.join(repo, 'transformers', 'good.rs'), 'w') as file:
            file.write(FILTER)
        with open(os.path.join(repo, 'transformers', 'broken.rs'), 'w') as file:
            file.write('use mage::prelude::*;\n\nfn transform() -> i64 { "text" }\n')
        reports = {report.block.uuid: report for report in project.build_all(repo, stream=False)}
        self.assertTrue(reports['good'].ok)
        self.assertFalse(reports['broken'].ok)
        self.assertIn('transformers/broken.rs:3', plain(reports['broken'].error))
        again = project.build_all(repo, stream=False)
        self.assertTrue([report.cached for report in again if report.ok] == [True])

    def test_a_rust_condition_decides_whether_its_block_runs(self):
        pipeline = Pipeline.create('rust condition', repo_path=self.repo_path)
        loader = Block.create('rows', BlockType.DATA_LOADER, self.repo_path, pipeline=pipeline)
        with open(loader.file_path, 'w') as file:
            file.write(
                'import polars as pl\n'
                '@data_loader\n'
                'def load(**kwargs):\n'
                "    return pl.DataFrame({'value': [1.0, 2.0, 3.0]})\n",
            )
        Block.create(
            'consumer',
            BlockType.TRANSFORMER,
            self.repo_path,
            pipeline=pipeline,
            upstream_block_uuids=['rows'],
        )
        condition = Block.create(
            'enough_rows',
            BlockType.CONDITIONAL,
            self.repo_path,
            language=BlockLanguage.RUST,
            pipeline=pipeline,
        )
        self.assertIsInstance(condition, RustConditionalBlock)
        self.assertTrue(condition.file_path.endswith('.rs'))
        self.assertIn('fn condition(vars: Vars) -> Result<bool>', open(condition.file_path).read())
        with open(condition.file_path, 'w') as file:
            file.write(
                'use mage::prelude::*;\n\n'
                'fn condition(data: DataFrame, vars: Vars) -> Result<bool> {\n'
                '    let minimum: usize = vars.get_or("minimum", 1)?;\n'
                '    println!("{} rows", data.height());\n'
                '    Ok(data.height() >= minimum)\n'
                '}\n',
            )
        pipeline.add_block(condition)
        pipeline = Pipeline.get(pipeline.uuid, repo_path=self.repo_path)
        pipeline.get_block('rows').execute_sync()
        condition = pipeline.get_block('enough_rows', block_type=BlockType.CONDITIONAL)
        self.assertIsInstance(condition, RustConditionalBlock)
        parent = pipeline.get_block('consumer')
        self.assertTrue(condition.execute_conditional(parent, global_vars={'minimum': 3}))
        self.assertFalse(condition.execute_conditional(parent, global_vars={'minimum': 4}))

        with open(condition.file_path, 'w') as file:
            file.write('use mage::prelude::*;\n\nfn condition() -> Result<()> {\n    Ok(())\n}\n')
        condition = Pipeline.get(pipeline.uuid, repo_path=self.repo_path).get_block(
            'enough_rows', block_type=BlockType.CONDITIONAL,
        )
        with self.assertRaisesRegex(RustBlockError, 'A conditional block returns a bool'):
            condition.execute_conditional(parent, global_vars={})

    def test_a_python_to_rust_to_python_pipeline(self):
        pipeline = Pipeline.create('rust pipeline', repo_path=self.repo_path)
        loader = Block.create('orders', BlockType.DATA_LOADER, self.repo_path, pipeline=pipeline)
        with open(loader.file_path, 'w') as file:
            file.write(
                'import pandas as pd\n'
                '@data_loader\n'
                'def load(**kwargs):\n'
                "    return pd.DataFrame({'id': pd.array([1, 2, 3], dtype='Int64'), "
                "'amount': [10.0, -2.0, 7.5]})\n",
            )
        rust = Block.create(
            'filter_orders',
            BlockType.TRANSFORMER,
            self.repo_path,
            language=BlockLanguage.RUST,
            pipeline=pipeline,
            upstream_block_uuids=['orders'],
        )
        self.assertIsInstance(rust, RustBlock)
        self.assertTrue(rust.file_path.endswith('.rs'))
        self.assertIn('fn transform(data: LazyFrame)', open(rust.file_path).read())
        with open(rust.file_path, 'w') as file:
            file.write(FILTER)
        consumer = Block.create(
            'count',
            BlockType.TRANSFORMER,
            self.repo_path,
            pipeline=pipeline,
            upstream_block_uuids=['filter_orders'],
        )
        with open(consumer.file_path, 'w') as file:
            file.write(
                '@transformer\n'
                'def count(frame, **kwargs):\n'
                '    import polars as pl\n'
                '    assert isinstance(frame, pl.DataFrame), type(frame)\n'
                "    return {'rows': frame.height, 'total': frame['doubled'].sum()}\n",
            )

        pipeline = Pipeline.get(pipeline.uuid, repo_path=self.repo_path)
        for uuid in ('orders', 'filter_orders', 'count'):
            pipeline.get_block(uuid).execute_sync(global_vars={'minimum': 5.0})

        variable = pipeline.variable_manager.get_variable_object(
            pipeline.uuid, 'filter_orders', 'output_0',
        )
        self.assertEqual(variable.variable_type, VariableType.POLARS_DATAFRAME)
        self.assertEqual(
            pipeline.variable_manager.get_variable(pipeline.uuid, 'count', 'output_0'),
            {'rows': 2, 'total': 35.0},
        )


@unittest.skipIf(shutil.which('cargo') is None, 'Rust is not installed')
class RustCheckEndpointTest(AsyncDBTestCase):
    async def check(self, user, **payload):
        return await BaseOperation(
            action=OperationType.CREATE,
            payload=dict(rust_check=payload),
            resource='rust_checks',
            user=user,
        ).execute()

    async def test_the_editor_gets_the_compiler_diagnostics(self):
        pipeline = Pipeline.create('rust checks', repo_path=self.repo_path)
        block = Block.create(
            'checked_block', BlockType.TRANSFORMER, self.repo_path,
            language=BlockLanguage.RUST, pipeline=pipeline,
        )
        code = (
            'use mage::prelude::*;\n'
            '\n'
            'fn transform(data: LazyFrame) -> LazyFrame {\n'
            '    undefined_function(data)\n'
            '}\n'
        )
        owner = create_user(_owner=True)
        response = await self.check(
            owner, block_uuid=block.uuid, content=code, pipeline_uuid=pipeline.uuid,
        )
        diagnostics = response['rust_check']['diagnostics']
        self.assertEqual(diagnostics[0]['level'], 'error')
        self.assertEqual((diagnostics[0]['line'], diagnostics[0]['column']), (4, 5))

        missing = await self.check(
            owner, block_uuid=block.uuid, content='fn helper() {}\n',
            pipeline_uuid=pipeline.uuid,
        )
        self.assertIn('no `transform` function', missing['rust_check']['diagnostics'][0]['message'])

        viewer = create_user()
        denied = await self.check(
            viewer, block_uuid=block.uuid, content=code, pipeline_uuid=pipeline.uuid,
        )
        self.assertEqual(denied['error']['code'], 403)


@unittest.skipIf(shutil.which('cargo') is None, 'Rust is not installed')
class StoredInputTest(DBTestCase):
    """Upstream tables stored as Parquet reach a Rust block by path, with the same result."""

    CODE = '''use mage::prelude::*;

fn transform(data: LazyFrame) -> Result<LazyFrame> {
    Ok(data.with_columns([len().alias("rows")]))
}
'''

    def pipeline_with(self, loader_code: str):
        pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        loader = Block.create(
            f'{pipeline.uuid}_load', BlockType.DATA_LOADER, self.repo_path, pipeline=pipeline,
        )
        with open(loader.file_path, 'w') as file:
            file.write(loader_code)
        rust = Block.create(
            f'{pipeline.uuid}_rust', BlockType.TRANSFORMER, self.repo_path,
            language=BlockLanguage.RUST, pipeline=pipeline, upstream_block_uuids=[loader.uuid],
        )
        with open(rust.file_path, 'w') as file:
            file.write(self.CODE)
        pipeline = Pipeline.get(pipeline.uuid, repo_path=self.repo_path)
        pipeline.get_block(loader.uuid).execute_sync()
        return pipeline, pipeline.get_block(loader.uuid), pipeline.get_block(rust.uuid)

    def output(self, pipeline, block):
        block.execute_sync()
        return pipeline.variable_manager.get_variable(pipeline.uuid, block.uuid, 'output_0')

    def assert_same_with_and_without_paths(self, loader_code, expect_path: bool):
        from mage_ai.data_preparation.models.block.rust import inputs as rust_inputs

        pipeline, loader, rust = self.pipeline_with(loader_code)
        tables = rust_inputs.stored_tables(rust, [loader.uuid], None)
        self.assertEqual(tables is not None, expect_path, tables)
        direct = self.output(pipeline, rust)
        with patch.dict(os.environ, {'MAGE_RUST_DIRECT_INPUTS': '0'}):
            loaded = self.output(pipeline, rust)
        self.assertEqual(direct.to_dicts(), loaded.to_dicts())
        self.assertEqual(direct.schema, loaded.schema)
        return direct

    def test_polars_outputs_go_by_path(self):
        output = self.assert_same_with_and_without_paths(
            'import polars as pl\n@data_loader\ndef load(**kwargs):\n'
            "    return pl.DataFrame({'a': [1, None, 3], 'b': ['x', 'y', None], "
            "'c': [[1], [], None]})\n",
            expect_path=True,
        )
        self.assertEqual(output['rows'].to_list(), [3, 3, 3])

    def test_plain_pandas_outputs_go_by_path(self):
        self.assert_same_with_and_without_paths(
            'import pandas as pd\n@data_loader\ndef load(**kwargs):\n'
            "    return pd.DataFrame({'a': pd.array([1, None, 3], dtype='Int64'), "
            "'b': ['x', 'y', None], 'c': pd.to_datetime(['2024-01-01'] * 3).tz_localize('UTC'), "
            "'d': [1.5, float('nan'), 2.0]})\n",
            expect_path=True,
        )

    def test_pandas_outputs_that_mage_converts_are_loaded(self):
        # Lists are stored as JSON text and categories as codes; loading converts them.
        self.assert_same_with_and_without_paths(
            'import pandas as pd\n@data_loader\ndef load(**kwargs):\n'
            "    return pd.DataFrame({'tags': [['x'], ['y', 'z'], []], "
            "'level': pd.Categorical([1, 2, 1])})\n",
            expect_path=False,
        )

    def test_inputs_given_in_memory_are_passed_as_given(self):
        pipeline, loader, rust = self.pipeline_with(
            'import polars as pl\n@data_loader\ndef load(**kwargs):\n'
            "    return pl.DataFrame({'a': [1]})\n",
        )
        frame = pl.DataFrame({'a': [7, 8]})
        variables, _, _ = rust.fetch_input_variables([frame])
        self.assertIs(variables[0], frame)
        cached, _, _ = rust.fetch_input_variables(
            None, block_run_outputs_cache={loader.uuid: [frame]},
        )
        from mage_ai.data_preparation.models.block.rust.inputs import StoredTable
        self.assertFalse(any(isinstance(item, StoredTable) for item in cached))
        stored, _, _ = rust.fetch_input_variables(None)
        self.assertIsInstance(stored[0], StoredTable)
