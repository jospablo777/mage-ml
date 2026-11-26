"""
Column checks at compile time (orchestration/columns.py): a block that reads a column its
upstream block does not give is found before the run, with nothing declared; a column the
upstream block's contract leaves out fails the compilation.
"""
import os
import textwrap
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import columns, compilation
from mage_ai.tests.base_test import DBTestCase, TestCase

LOAD = '''
@data_loader
def load(*args, **kwargs):
    import pandas as pd
    return pd.DataFrame({'amount': [1], 'qty': [2]})
'''
READS = '''
@transformer
def total(orders, *args, **kwargs):
    orders['total'] = orders['amount'] * orders['qty']
    return orders[['total', 'discount']]
'''


class ColumnReadsTest(TestCase):
    def test_reads_are_found_and_columns_the_block_makes_are_not(self):
        code = textwrap.dedent('''
            import polars as pl

            @transformer
            def t(orders, customers, *args, **kwargs):
                orders['total'] = orders['amount'] * orders['qty']
                x = orders['total'] + orders["discount"]
                y = customers[['id', 'region']]
                customers = customers.rename(columns={'id': 'cid'})
                z = customers['cid']
                return orders.assign(score=1)['score']
        ''')
        self.assertEqual(columns.column_reads(code), {
            'orders': {'amount': 6, 'qty': 6, 'discount': 7},
            'customers': {'id': 8, 'region': 8},
        })

    def test_polars_columns_of_a_block_with_one_input(self):
        code = textwrap.dedent('''
            import polars as pl

            @transformer
            def t(df, *args, **kwargs):
                doubled = df.with_columns((pl.col('amount') * 2).alias('double'))
                return doubled.filter(pl.col('double') > 1).select(pl.col('nope'))
        ''')
        self.assertEqual(columns.column_reads(code), {'df': {'amount': 6, 'nope': 7}})

    def test_code_that_does_not_parse_or_has_no_block_function_reads_nothing(self):
        self.assertEqual(columns.column_reads('def broken(:'), {})
        self.assertEqual(columns.column_reads('x = 1\n'), {})


class ColumnUsesTest(TestCase):
    def test_only_subscripts_of_the_input_are_known_uses(self):
        code = textwrap.dedent('''
            @transformer
            def t(orders, customers, *args, **kwargs):
                orders['total'] = orders['amount'] * orders['qty']
                ids = customers[['id', 'region']]
                customers = customers.rename(columns={'id': 'cid'})
                return customers
        ''')
        # customers.rename(...) reads the input whole; what follows is not the input.
        self.assertEqual(columns.column_uses(code), {'orders': {'amount', 'qty'},
                                                     'customers': None})

    def test_an_input_returned_or_passed_on_may_have_any_column_read(self):
        for body in ('return df', 'return helper(df)', 'return df.head()', 'return len(df)'):
            code = f'@transformer\ndef t(df, *args, **kwargs):\n    x = df["a"]\n    {body}\n'
            self.assertEqual(columns.column_uses(code), {'df': None}, body)


class ColumnCheckTest(DBTestCase):
    def setUp(self):
        super().setUp()
        compilation._compilations.clear()
        columns._cache.clear()
        columns._files.clear()

    def pipeline(self, contract=None):
        pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(pipeline.delete)
        load = Block.create(f'{self._testMethodName}_load', 'data_loader', self.repo_path,
                            language='python',
                            configuration=dict(contract=contract) if contract else None)
        Path(load.file_path).write_text(LOAD)
        total = Block.create(f'{self._testMethodName}_total', 'transformer', self.repo_path,
                             language='python')
        Path(total.file_path).write_text(READS)
        pipeline.add_block(load)
        pipeline.add_block(total, upstream_block_uuids=[load.uuid])
        return Pipeline.get(pipeline.uuid, repo_path=self.repo_path), load

    def store(self, pipeline, block, names, partition=None):
        root = Path(pipeline.variable_manager.pipeline_path(pipeline.uuid), '.variables')
        directory = root / (partition or '') / block.uuid / 'output_0'
        directory.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({n: [1] for n in names}), directory / 'data.parquet')
        return directory / 'data.parquet'

    def test_nothing_is_reported_before_the_upstream_block_ran(self):
        pipeline, _ = self.pipeline()
        self.assertEqual(columns.check(pipeline), [])
        self.assertNotIn('columns', compilation.summary_for(pipeline))

    def test_a_column_the_last_output_lacks_is_a_warning(self):
        pipeline, load = self.pipeline()
        old = self.store(pipeline, load, ['amount', 'qty', 'discount'], partition='1/20260101')
        os.utime(old, (1, 1))
        # The newest output is the one compared: discount is gone from it.
        self.store(pipeline, load, ['amount', 'qty'])

        [finding] = compilation.summary_for(pipeline)['columns']

        self.assertEqual(finding['severity'], 'warning')
        self.assertEqual(finding['missing'], ['discount'])
        self.assertEqual(finding['line'], 5)
        self.assertIn('reads discount from', finding['message'])
        self.assertIn('its last stored output has no column of that name', finding['message'])

    def wide(self, pipeline, block, partition=None):
        """An output with two read columns and a wide one no block reads (about 2 MB)."""
        root = Path(pipeline.variable_manager.pipeline_path(pipeline.uuid), '.variables')
        directory = root / (partition or '') / block.uuid / 'output_0'
        directory.mkdir(parents=True, exist_ok=True)
        rows = 20000
        pq.write_table(pa.table({
            'amount': list(range(rows)),
            'qty': list(range(rows)),
            'notes': [os.urandom(48).hex() for _ in range(rows)],
        }), directory / 'data.parquet')

    def test_columns_no_downstream_block_reads_are_measured_and_reported(self):
        pipeline, load = self.pipeline()
        total = pipeline.get_block(f'{self._testMethodName}_total')
        Path(total.file_path).write_text(textwrap.dedent('''
            @transformer
            def total(orders, *args, **kwargs):
                return orders['amount'] * orders['qty']
        '''))
        self.wide(pipeline, load)

        [finding] = compilation.summary_for(pipeline)['unused_columns']

        self.assertEqual(finding['upstream'], load.uuid)
        self.assertEqual(finding['read'], ['amount', 'qty'])
        self.assertEqual(finding['columns'], 3)
        self.assertGreater(finding['unused_bytes'] / finding['output_bytes'], 0.8)
        self.assertIn('reads 2 of 3 columns', finding['message'])
        self.assertRegex(finding['message'], r'the other 1 are 8\d% of its last stored output')

    def test_a_reader_that_may_use_any_column_leaves_the_output_alone(self):
        pipeline, load = self.pipeline()
        total = pipeline.get_block(f'{self._testMethodName}_total')
        Path(total.file_path).write_text(textwrap.dedent('''
            @transformer
            def total(orders, *args, **kwargs):
                orders['total'] = orders['amount'] * orders['qty']
                return orders
        '''))
        self.wide(pipeline, load)

        self.assertNotIn('unused_columns', compilation.summary_for(pipeline))

    def test_a_column_the_contract_lacks_fails_the_compilation(self):
        contracts = Path(self.repo_path, 'contracts')
        contracts.mkdir(exist_ok=True)
        (contracts / 'orders.yaml').write_text(
            'columns:\n  amount: {type: integer}\n  qty: {type: integer}\n')
        self.addCleanup((contracts / 'orders.yaml').unlink)
        pipeline, _ = self.pipeline(contract='orders')

        done = compilation.start(pipeline, wait=True)

        [unit] = [u for u in done.units if u.kind == 'column_check']
        self.assertEqual(unit.state, 'failed')
        self.assertIn('its contract lists no column of that name', unit.error)
        self.assertEqual(done.state, 'failed')
