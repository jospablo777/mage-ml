"""
SQL Server SQL blocks name their tables with the schema they were written to. A name
alone is the login's default schema (dbo), not MSSQL_SCHEMA, so a profile or block with
another schema read its upstream tables and its own table from dbo (I-271). Run on a
stand-in for Mage's SQL Server io class over DuckDB, whose current schema is dbo; no SQL
Server runs here.
"""
import unittest.mock
from pathlib import Path

import duckdb

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.tests.base_test import DBTestCase

ORDERS = '''
import pandas as pd


@data_loader
def load(*args, **kwargs):
    return pd.DataFrame({'region': ['north', 'south', 'north'], 'amount': [10.0, 5.5, 2.5]})
'''


class DuckMssql:
    """Mage's SQL Server io class over DuckDB; dbo is where names alone resolve."""

    def __init__(self, schema):
        self.schema = schema
        self.connection = duckdb.connect()
        self.connection.execute("ATTACH ':memory:' AS shop")
        self.connection.execute('CREATE SCHEMA shop.dbo')
        self.connection.execute('USE shop.dbo')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def default_schema(self):
        return self.schema or 'dbo'

    def _full_table_name(self, schema_name, table_name):
        return f'{schema_name}.{table_name}' if schema_name else table_name

    def export(self, df, schema_name=None, table_name=None, query_string=None,
               if_exists='append', **kwargs):
        name = self._full_table_name(schema_name, table_name)
        self.connection.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')
        if df is not None:
            self.connection.register('upstream_frame', df)
            query_string = 'SELECT * FROM upstream_frame'
        self.connection.execute(f'CREATE OR REPLACE TABLE {name} AS {query_string}')

    def load(self, query_string, limit=None, verbose=True, **kwargs):
        top = 'SELECT TOP '
        if query_string.startswith(top):
            count, rest = query_string[len(top):].split(' ', 1)
            query_string = f'SELECT {rest} LIMIT {int(count)}'
        return self.connection.execute(query_string).df()


class MssqlNamesTest(DBTestCase):
    def setUp(self):
        super().setUp()
        io_config = Path(self.repo_path) / 'io_config.yaml'
        original = io_config.read_text() if io_config.exists() else None
        io_config.write_text(
            'sales_server:\n  MSSQL_DATABASE: shop\n  MSSQL_HOST: db\n  MSSQL_PORT: 1433\n'
            '  MSSQL_SCHEMA: sales\n'
        )
        self.addCleanup(
            lambda: io_config.write_text(original) if original is not None
            else io_config.unlink(),
        )
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)

    def block(self, name, code, language='sql', upstream=(), **configuration):
        block = Block.create(
            f'{self.pipeline.uuid}_{name}', 'transformer' if upstream else 'data_loader',
            self.repo_path, pipeline=self.pipeline, language=language,
            configuration=configuration or None, upstream_block_uuids=[b.uuid for b in upstream],
        )
        Path(block.file_path).write_text(code)
        self.pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        return self.pipeline.get_block(block.uuid)

    def test_a_profile_schema_other_than_dbo_reads_the_tables_it_wrote(self):
        from mage_ai.io.mssql import MSSQL

        server = dict(data_provider='sqlserver', data_provider_profile='sales_server',
                      export_write_policy='replace')
        orders = self.block('orders', ORDERS, language='python')
        totals = self.block(
            'totals', 'SELECT region, SUM(amount) AS total FROM {{ df_1 }} GROUP BY region',
            upstream=[orders], **server,
        )
        top = self.block(
            'top', 'SELECT MAX(total) AS top FROM {{ df_1 }}', upstream=[totals],
            **dict(server, data_provider_schema='reports'),
        )
        stand_in = DuckMssql('sales')

        with unittest.mock.patch.object(MSSQL, 'with_config', return_value=stand_in):
            for block in (orders, totals, top):
                self.pipeline.get_block(block.uuid).execute_sync()

        manager = self.pipeline.variable_manager
        output = manager.get_variable(self.pipeline.uuid, top.uuid, 'output_0')
        self.assertEqual(output['top'].tolist(), [12.5])
        tables = stand_in.connection.execute(
            'SELECT table_schema, count(*) FROM information_schema.tables '
            "WHERE table_catalog = 'shop' GROUP BY 1 ORDER BY 1",
        ).fetchall()
        # The upstream copy and the first block's table in sales, the last block's in reports.
        self.assertEqual(tables, [('reports', 1), ('sales', 2)])
