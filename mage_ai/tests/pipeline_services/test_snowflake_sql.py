"""
Snowflake SQL blocks in pipeline services, against Mage's Snowflake branch. No Snowflake
runs here: both run on a stand-in for Mage's Snowflake io class over DuckDB, so the test
compares what they ask of Snowflake (the database and schema the client opens on, which
upstream outputs become tables and under which names, the write policy) and the rows that
come back.
"""
import unittest.mock

import duckdb
import pandas as pd

from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.pipeline_services import export
from mage_ai.pipeline_services.runtime import sql
from mage_ai.tests.pipeline_services.test_export import ExportTestCase

ORDERS = '''
    import pandas as pd


    @data_loader
    def load(*args, **kwargs):
        return pd.DataFrame({
            'region': ['north', 'south', 'north', 'east'],
            'amount': [10.0, 5.5, 2.5, 7.0],
        })
'''
TOTALS = '''SELECT region, SUM(amount) AS total
FROM {{ df_1 }}
WHERE amount >= {{ min_amount }}
GROUP BY region
ORDER BY region'''
TOP = 'SELECT region, total FROM {{ df_1 }} WHERE total > 7 ORDER BY region'
PROFILE = '''warehouse:
  SNOWFLAKE_ACCOUNT: shop-account
  SNOWFLAKE_DEFAULT_WH: compute_wh
  SNOWFLAKE_DEFAULT_DB: shop
  SNOWFLAKE_DEFAULT_SCHEMA: public
  SNOWFLAKE_USER: loader
  SNOWFLAKE_PASSWORD: secret
other_warehouse:
  SNOWFLAKE_ACCOUNT: shop-account
  SNOWFLAKE_DEFAULT_WH: reporting_wh
  SNOWFLAKE_DEFAULT_DB: shop
  SNOWFLAKE_DEFAULT_SCHEMA: public
  SNOWFLAKE_USER: loader
  SNOWFLAKE_PASSWORD: secret
'''  # pragma: allowlist secret


class DuckSnowflake:
    """Mage's Snowflake io class over DuckDB, recording each request."""

    def __init__(self):
        self.connection = duckdb.connect()
        self.connection.execute("ATTACH ':memory:' AS \"SHOP\"")
        self.calls = []

    def open_on(self, config, database=None, schema=None, **kwargs):
        self.calls.append(('open', database, schema))
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def default_database(self) -> str:
        return 'shop'

    def default_schema(self) -> str:
        return 'public'

    def export(self, df, table_name, database=None, schema=None, if_exists='replace',
               query_string=None, verbose=True, **kwargs):
        self.calls.append(
            ('export', database, schema, table_name, if_exists, df is not None, query_string),
        )
        name = f'"{database}"."{schema}"."{table_name}"'
        self.connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{database}"."{schema}"')
        source = query_string
        if df is not None:
            self.connection.register('upstream_frame', df)
            source = 'SELECT * FROM upstream_frame'
        exists = self.connection.execute(
            'SELECT count(*) FROM information_schema.tables '
            'WHERE table_catalog = ? AND table_schema = ? AND table_name = ?',
            [database, schema, table_name],
        ).fetchone()[0]
        if exists and if_exists == 'append':
            self.connection.execute(f'INSERT INTO {name} {source}')
        elif exists and if_exists == 'fail':
            raise ValueError(f'Table {name} already exists.')
        else:
            self.connection.execute(f'CREATE OR REPLACE TABLE {name} AS {source}')

    def load(self, query_string, limit=10_000_000, verbose=True, database=None, schema=None,
             table_name=None, **kwargs):
        self.calls.append(('load', query_string, database, schema, table_name))
        return self.connection.execute(
            f'SELECT * FROM ({query_string}) LIMIT {int(limit)}',
        ).df()


class SnowflakeSqlTest(ExportTestCase):
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
                data_provider='snowflake',
                data_provider_profile='warehouse',
                data_provider_schema='sales',
                export_write_policy='replace',
            ),
            **configuration,
        })

    def run_both(self, blocks, variables):
        """Runs the blocks in Mage, then in the service runtime, each on its own stand-in."""
        from mage_ai.io.snowflake import Snowflake

        pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        mage = DuckSnowflake()
        with unittest.mock.patch.object(Snowflake, 'with_config', side_effect=mage.open_on):
            for block in blocks:
                pipeline.get_block(block.uuid).execute_sync(global_vars=dict(variables))
        manager = pipeline.variable_manager
        mage_outputs = {
            b.uuid: manager.get_variable(pipeline.uuid, b.uuid, 'output_0') for b in blocks
        }

        manifest = export.capture(self.repo_path, [self.pipeline.uuid]).manifest
        entries = {b['uuid']: b for b in manifest['pipelines'][0]['blocks']}
        service = DuckSnowflake()
        outputs = {}
        with unittest.mock.patch.object(Snowflake, 'with_config', side_effect=service.open_on):
            for block in blocks:
                entry = entries[block.uuid]
                if entry['language'] != 'sql':
                    outputs[block.uuid] = mage_outputs[block.uuid]
                    continue
                entry = dict(entry, file=str(self.project / entry['file']))
                inputs = [outputs[u['uuid']] for u in entry['sql']['upstream']]
                outputs[block.uuid] = sql.run(entry, inputs, dict(variables), self.repo_path)[0]
        return mage, mage_outputs, service, outputs

    def test_the_service_asks_snowflake_what_mage_asks_and_gets_the_same_rows(self):
        orders = self.add('orders', 'data_loader', ORDERS)
        totals = self.sql_block('totals', TOTALS, [orders])
        top = self.sql_block('top', TOP, [totals], export_write_policy='append')

        mage, mage_outputs, service, outputs = self.run_both(
            [orders, totals, top], dict(min_amount=5),
        )

        self.assertEqual(service.calls, mage.calls)
        uploads = [c for c in service.calls if c[0] == 'export' and c[5]]
        # Only the Python block's output is written, in capitals; the SQL block's table is
        # read as it is.
        self.assertEqual(len(uploads), 1, uploads)
        self.assertEqual(uploads[0][1:3], ('SHOP', 'SALES'))
        self.assertEqual(uploads[0][3], uploads[0][3].upper())
        first_load = next(c[1] for c in service.calls if c[0] == 'load')
        self.assertTrue(first_load.startswith('SELECT * FROM "SHOP"."SALES"."'), first_load)
        for block in (totals, top):
            pd.testing.assert_frame_equal(
                outputs[block.uuid].reset_index(drop=True),
                mage_outputs[block.uuid].reset_index(drop=True),
            )
        self.assertEqual(outputs[totals.uuid].to_dict('list'), dict(
            region=['east', 'north', 'south'], total=[7.0, 10.0, 5.5],
        ))
        self.assertEqual(outputs[top.uuid]['region'].tolist(), ['north'])

    def test_another_warehouse_copies_the_upstream_table(self):
        orders = self.add('orders', 'data_loader', ORDERS)
        totals = self.sql_block('totals', TOTALS, [orders])
        other = self.sql_block('other', TOP, [totals], data_provider_profile='other_warehouse')

        mage, _, service, outputs = self.run_both([orders, totals, other], dict(min_amount=0))

        self.assertEqual(service.calls, mage.calls)
        uploads = [c for c in service.calls if c[0] == 'export' and c[5]]
        self.assertEqual(len(uploads), 2, uploads)
        self.assertEqual(outputs[other.uuid]['region'].tolist(), ['north'])

    def test_a_snowflake_sql_block_exports_with_its_client(self):
        orders = self.add('orders', 'data_loader', ORDERS)
        self.sql_block('totals', TOTALS, [orders])

        captured = export.capture(self.repo_path, [self.pipeline.uuid])

        packages = {p.lower().replace('_', '-') for p in captured.requirements}
        self.assertIn('snowflake-connector-python', packages)
