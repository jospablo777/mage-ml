"""
The tables SQL blocks read and write, named as OpenLineage's naming spec names them
(orchestration/lineage_tables.py).
"""
import textwrap
from pathlib import Path

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import lineage_tables
from mage_ai.tests.base_test import DBTestCase, TestCase


class TablesTest(TestCase):
    def test_reads_leave_out_ctes_upstream_blocks_and_written_tables(self):
        reads, writes = lineage_tables.tables(textwrap.dedent('''
            WITH recent AS (SELECT * FROM sales.orders WHERE day > '2026-01-01')
            INSERT INTO marts.daily
            SELECT r.day, c.region, SUM(r.amount)
            FROM recent r
            JOIN crm.customers c ON c.id = r.customer_id
            JOIN {{ df_1 }} u ON u.id = r.id
            GROUP BY 1, 2
        '''), 'postgres')

        self.assertEqual(reads, [['', 'crm', 'customers'], ['', 'sales', 'orders']])
        self.assertEqual(writes, [['', 'marts', 'daily']])

    def test_names_follow_each_databases_form(self):
        name = lineage_tables._name
        self.assertEqual(name('postgres', ['', '', 'orders'],
                              dict(database='shop', schema='public')), 'shop.public.orders')
        self.assertEqual(name('postgres', ['', 'sales', 'orders'], dict(database='shop')),
                         'shop.sales.orders')
        self.assertEqual(name('mysql', ['', '', 'orders'], dict(database='shop')),
                         'shop.orders')
        self.assertEqual(name('bigquery', ['proj', 'ds', 't'], {}), 'proj.ds.t')
        self.assertIsNone(name('bigquery', ['', 'ds', 't'], {}))
        self.assertEqual(name('duckdb', ['', '', 't'], dict(schema='main')), 'main.t')

    def test_a_query_that_does_not_parse_names_nothing(self):
        self.assertEqual(lineage_tables.tables('SELEKT FROM WHERE (', 'postgres'), ([], []))


class SqlBlockDatasetsTest(DBTestCase):
    def test_a_sql_block_reads_its_source_tables_and_writes_its_own(self):
        Path(self.repo_path, 'io_config.yaml').write_text(textwrap.dedent('''
            lineage:
              POSTGRES_HOST: db.internal
              POSTGRES_PORT: 6543
              POSTGRES_DBNAME: shop
              POSTGRES_SCHEMA: analytics
        '''))
        self.addCleanup(Path(self.repo_path, 'io_config.yaml').unlink)
        pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(pipeline.delete)
        block = Block.create(
            f'{self._testMethodName}_orders', 'transformer', self.repo_path, language='sql',
            configuration=dict(data_provider='postgres', data_provider_profile='lineage',
                               data_provider_table='orders_clean'),
        )
        Path(block.file_path).write_text('SELECT * FROM raw.orders JOIN customers USING (id)')
        pipeline.add_block(block)

        reads, writes = lineage_tables.sql_datasets(pipeline.get_block(block.uuid))

        namespace = 'postgres://db.internal:6543'
        self.assertEqual(reads, [dict(namespace=namespace, name='shop.analytics.customers'),
                                 dict(namespace=namespace, name='shop.raw.orders')])
        self.assertEqual(writes, [dict(namespace=namespace, name='shop.analytics.orders_clean')])
