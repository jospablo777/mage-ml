"""
SQL Server SQL blocks in pipeline services, against Mage's SQL Server branch, on a stand-in
for Mage's MSSQL io class over DuckDB (integration_tests/mssql runs SQL-only pipelines on
Babelfish), and the export's pymssql client: services connect through FreeTDS, so the image
needs no ODBC driver.
"""
import re
import unittest.mock

import pandas as pd

from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.pipeline_services import export
from mage_ai.pipeline_services.runtime import sql
from mage_ai.tests.pipeline_services.test_db2_sql import ORDERS, TOP, TOTALS, DuckDb2
from mage_ai.tests.pipeline_services.test_export import ExportTestCase

RAW = '''CREATE TABLE shop.raw_totals (region VARCHAR(10), orders INT);
INSERT INTO shop.raw_totals SELECT region, COUNT(*) AS orders FROM {{ df_1 }} GROUP BY region'''
PROFILE = '''warehouse:
  MSSQL_DATABASE: sales
  MSSQL_DRIVER: ODBC Driver 18 for SQL Server
  MSSQL_HOST: sql.example.com
  MSSQL_PORT: 1433
  MSSQL_SCHEMA: shop
  MSSQL_USER: sa
  MSSQL_PASSWORD: secret
'''  # pragma: allowlist secret


def duckdb_sql(query: str) -> str:
    """SQL Server's TOP and SELECT INTO as DuckDB writes them."""
    query = re.sub(r'SELECT TOP\s*\(?(\d+)\)?\s+(.*)', r'SELECT \2 LIMIT \1', query,
                   flags=re.I | re.S)
    into = re.match(r'(SELECT .*?) INTO (\S+) (FROM .*)', query, flags=re.I | re.S)
    if into:
        return f'CREATE TABLE {into.group(2)} AS {into.group(1)} {into.group(3)}'
    return query


class DuckMssql(DuckDb2):
    """Mage's MSSQL io class over DuckDB, recording each request."""

    def __init__(self):
        super().__init__()
        self.settings = {}

    def load(self, query_string, limit=10_000_000, verbose=True, **kwargs):
        self.calls.append(('load', query_string, limit))
        return self.connection.execute(duckdb_sql(query_string)).df()

    def execute_queries(self, queries, commit=False, fetch_query_at_indexes=None, **kwargs):
        self.calls.append(('execute', list(queries), list(fetch_query_at_indexes or [])))
        results = []
        for query, fetch in zip(queries, fetch_query_at_indexes or [False] * len(queries)):
            result = self.connection.execute(duckdb_sql(query))
            results.append(result.df() if fetch else None)
        return results


class MssqlSqlTest(ExportTestCase):
    def setUp(self):
        super().setUp()
        io_config = self.project / 'io_config.yaml'
        previous = io_config.read_text() if io_config.exists() else None
        io_config.write_text(PROFILE)
        self.addCleanup(
            lambda: io_config.write_text(previous) if previous is not None
            else io_config.unlink(),
        )

    def sql_block(self, name, code, upstream, **configuration):
        return self.add(name, 'transformer', code, upstream, language='sql', configuration={
            **dict(
                data_provider='sqlserver',
                data_provider_profile='warehouse',
                export_write_policy='replace',
            ),
            **configuration,
        })

    def run_both(self, blocks, variables):
        """Runs the blocks in Mage, then in the service runtime, each on its own stand-in."""
        from mage_ai.io.mssql import MSSQL

        pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        mage = DuckMssql()
        with unittest.mock.patch.object(MSSQL, 'with_config', return_value=mage):
            for block in blocks:
                pipeline.get_block(block.uuid).execute_sync(global_vars=dict(variables))
        manager = pipeline.variable_manager
        mage_outputs = {
            b.uuid: manager.get_variable(pipeline.uuid, b.uuid, 'output_0') for b in blocks
        }

        with unittest.mock.patch.object(export, '_client_missing', return_value=False):
            manifest = export.capture(self.repo_path, [self.pipeline.uuid]).manifest
        entries = {b['uuid']: b for b in manifest['pipelines'][0]['blocks']}
        service = DuckMssql()
        outputs = {}
        with unittest.mock.patch.object(MSSQL, 'with_config', return_value=service):
            for block in blocks:
                entry = entries[block.uuid]
                if entry['language'] != 'sql':
                    outputs[block.uuid] = mage_outputs[block.uuid]
                    continue
                entry = dict(entry, file=str(self.project / entry['file']))
                inputs = [outputs[u['uuid']] for u in entry['sql']['upstream']]
                outputs[block.uuid] = sql.run(entry, inputs, dict(variables), self.repo_path)[0]
        return mage, mage_outputs, service, outputs

    def test_the_service_asks_sql_server_what_mage_asks_and_gets_the_same_rows(self):
        orders = self.add('orders', 'data_loader', ORDERS)
        totals = self.sql_block('totals', TOTALS, [orders])
        top = self.sql_block('top', TOP, [totals], export_write_policy='append')
        raw = self.sql_block('raw', RAW, [orders], use_raw_sql=True)

        mage, mage_outputs, service, outputs = self.run_both(
            [orders, totals, top, raw], dict(min_amount=5),
        )

        self.assertEqual(service.calls, mage.calls)
        uploads = [c for c in service.calls if c[0] == 'export' and c[4]]
        # The Python block's output goes to the profile's schema for each block reading it.
        self.assertEqual([u[1] for u in uploads], ['shop', 'shop'])
        for block in (totals, top, raw):
            # The raw query has no ORDER BY.
            pd.testing.assert_frame_equal(
                outputs[block.uuid].sort_values('region').reset_index(drop=True),
                mage_outputs[block.uuid].sort_values('region').reset_index(drop=True),
            )
        self.assertEqual(outputs[totals.uuid].to_dict('list'), dict(
            region=['east', 'north', 'south'], total=[7.0, 10.0, 5.5],
        ))
        self.assertEqual(outputs[top.uuid]['region'].tolist(), ['north'])
        # Raw SQL reads the table it wrote with TOP, not LIMIT.
        raw_queries = [c for c in service.calls if c[0] == 'execute'][-1][1]
        self.assertTrue(raw_queries[-1].startswith('SELECT TOP('), raw_queries)
        self.assertEqual(sorted(outputs[raw.uuid]['orders'].tolist()), [1, 1, 2])

    def test_services_connect_with_pymssql(self):
        from mage_ai.io.config import ConfigFileLoader

        loader = sql._loader('sqlserver', ConfigFileLoader(
            str(self.project / 'io_config.yaml'), 'warehouse',
        ))

        self.assertTrue(loader.uses_pymssql)
        self.assertEqual(loader.placeholder, '%s')
        with unittest.mock.patch('pymssql.connect') as connect:
            loader.open()
        connect.assert_called_once_with(
            server='sql.example.com', user='sa', password='secret',  # pragma: allowlist secret
            database='sales', port='1433',
        )

    def test_an_export_with_sql_server_blocks_needs_pymssql(self):
        orders = self.add('orders', 'data_loader', ORDERS)
        totals = self.sql_block('totals', TOTALS, [orders])

        with unittest.mock.patch.object(export, '_client_missing', return_value=True):
            reason = export._sql_unsupported(
                Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path).get_block(
                    totals.uuid,
                ),
            )
        self.assertIn('pymssql', reason)
        self.assertIn('mage-ml[mssql]', reason)
