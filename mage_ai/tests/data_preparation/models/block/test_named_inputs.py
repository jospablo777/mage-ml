"""
Blocks can name their inputs (keyword-only parameters named after upstream blocks):
upstream outputs arrived only by position, so reordering upstream_blocks swapped two
inputs of the same type without an error.
"""
import textwrap
from pathlib import Path

import pandas as pd

from mage_ai.data_preparation.executors.pipeline_executor import PipelineExecutor
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.block.inputs import InputBindingError, bind_inputs
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.tests.base_test import DBTestCase, TestCase


class BindInputsTest(TestCase):
    def test_keyword_only_names_take_their_upstreams_output(self):
        def block(*, customers, orders, **kwargs):
            return customers, orders, kwargs

        args, kwargs = bind_inputs(block, ['O', 'C'], ['orders', 'customers'], {'env': 'x'})
        self.assertEqual(args, [])
        self.assertEqual(block(*args, **kwargs), ('C', 'O', {'env': 'x'}))

    def test_unnamed_inputs_stay_positional_in_order(self):
        def block(first, *args, second_one, **kwargs):
            return first, args, second_one

        args, kwargs = bind_inputs(block, ['a', 'b', 'c'], ['one', 'folder/second-one', 'three'])
        self.assertEqual(block(*args, **kwargs), ('a', ('c',), 'b'))

    def test_a_required_name_takes_a_run_variable_or_fails_with_the_choices(self):
        def block(df, *, threshold):
            return df, threshold

        args, kwargs = bind_inputs(block, ['df'], ['load'], {'threshold': 3})
        self.assertEqual(block(*args, **kwargs), ('df', 3))
        with self.assertRaises(InputBindingError) as caught:
            bind_inputs(block, ['df'], ['load'], {}, block_uuid='score')
        self.assertIn('`threshold`', str(caught.exception))
        self.assertIn('Its upstream blocks: load', str(caught.exception))

    def test_a_function_without_keyword_only_parameters_is_unchanged(self):
        def block(a, b, **kwargs):
            return a, b

        self.assertEqual(bind_inputs(block, [1, 2], ['x', 'y'], {'v': 1}), ([1, 2], {'v': 1}))

        def plain(a, b):
            return a, b

        self.assertEqual(bind_inputs(plain, [1, 2], ['x', 'y'], {'v': 1}), ([1, 2], {}))


class NamedInputsPipelineTest(DBTestCase):
    def test_named_inputs_survive_a_reordered_upstream_list(self):
        pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(pipeline.delete)

        def add(name, kind, code, upstream=()):
            block = Block.create(f'{pipeline.uuid}_{name}', kind, self.repo_path,
                                 language='python')
            Path(block.file_path).write_text(textwrap.dedent(code))
            pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
            return block

        orders = add('orders', 'data_loader', '''
            import pandas as pd
            @data_loader
            def load(**kwargs):
                return pd.DataFrame({'kind': ['order']})
        ''')
        customers = add('customers', 'data_loader', '''
            import pandas as pd
            @data_loader
            def load(**kwargs):
                return pd.DataFrame({'kind': ['customer']})
        ''')
        join = add('join', 'transformer', f'''
            import pandas as pd
            @transformer
            def join(*, {customers.uuid}, {orders.uuid}, **kwargs):
                return pd.DataFrame({{
                    'customers': {customers.uuid}['kind'],
                    'orders': {orders.uuid}['kind'],
                }})
        ''', upstream=[orders, customers])
        PipelineExecutor(pipeline).execute(global_vars={}, update_status=False)
        from mage_ai.data_preparation.variable_manager import VariableManager

        manager = VariableManager.get_manager(self.repo_path, pipeline.variables_dir)
        frame = manager.get_variable(pipeline.uuid, join.uuid, 'output_0')
        self.assertEqual(frame.to_dict('records'), [{'customers': 'customer', 'orders': 'order'}])
        self.assertIsInstance(frame, pd.DataFrame)
