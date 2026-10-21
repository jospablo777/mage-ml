"""
Redshift SQL blocks in pipeline services, against Mage's Redshift branch. No Redshift runs
here (redshift_connector sends startup settings PostgreSQL refuses): both run on a
stand-in for Mage's Redshift io class over DuckDB, so the test compares what they ask of
Redshift (which upstream outputs become tables, the query text, the write policy) and the
rows that come back.
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
# No REDSHIFT_CLUSTER_ID: password login, which Mage's comparison of profiles read as
# another database, so the upstream SQL table was copied over itself (I-267).
PROFILE = '''warehouse:
  REDSHIFT_HOST: cluster.example.com
  REDSHIFT_PORT: 5439
  REDSHIFT_DBNAME: dev
  REDSHIFT_SCHEMA: public
  REDSHIFT_TEMP_CRED_USER: loader
  REDSHIFT_TEMP_CRED_PASSWORD: secret
'''  # pragma: allowlist secret


class DuckRedshift:
    """Mage's Redshift io class over DuckDB, recording each request."""

    def __init__(self):
        self.connection = duckdb.connect()
        self.connection.execute("ATTACH ':memory:' AS dev")
        self.connection.execute('USE dev')
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def default_database(self) -> str:
        return 'dev'

    def default_schema(self) -> str:
        return 'public'

    def export(self, df, table_name=None, schema=None, schema_name=None, if_exists='append',
               index=False, verbose=True, query_string=None, drop_table_on_replace=False,
               **kwargs):
        schema = schema or schema_name
        self.calls.append(('export', schema, table_name, if_exists, df is not None, query_string))
        name = f'{schema}.{table_name}'
        self.connection.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
        source = query_string
        if df is not None:
            self.connection.register('upstream_frame', df)
            source = 'SELECT * FROM upstream_frame'
        exists = self.connection.execute(
            'SELECT count(*) FROM information_schema.tables '
            "WHERE table_catalog = 'dev' AND table_schema = ? AND table_name = ?",
            [schema, table_name],
        ).fetchone()[0]
        if exists and if_exists == 'append':
            self.connection.execute(f'INSERT INTO {name} {source}')
        elif exists and if_exists == 'fail':
            raise ValueError(f'Table {name} already exists.')
        else:
            self.connection.execute(f'CREATE OR REPLACE TABLE {name} AS {source}')

    def load(self, query_string, limit=10_000_000, verbose=True, **kwargs):
        self.calls.append(('load', query_string, limit))
        return self.connection.execute(
            f'SELECT * FROM ({query_string}) LIMIT {int(limit)}',
        ).df()


class RedshiftSqlTest(ExportTestCase):
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
                data_provider='redshift',
                data_provider_profile='warehouse',
                data_provider_schema='sales',
                export_write_policy='replace',
            ),
            **configuration,
        })

    def run_both(self, blocks, variables):
        """Runs the blocks in Mage, then in the service runtime, each on its own stand-in."""
        from mage_ai.io.redshift import Redshift

        pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        mage = DuckRedshift()
        with unittest.mock.patch.object(Redshift, 'with_config', return_value=mage):
            for block in blocks:
                pipeline.get_block(block.uuid).execute_sync(global_vars=dict(variables))
        manager = pipeline.variable_manager
        mage_outputs = {
            b.uuid: manager.get_variable(pipeline.uuid, b.uuid, 'output_0') for b in blocks
        }

        manifest = export.capture(self.repo_path, [self.pipeline.uuid]).manifest
        entries = {b['uuid']: b for b in manifest['pipelines'][0]['blocks']}
        service = DuckRedshift()
        outputs = {}
        with unittest.mock.patch.object(Redshift, 'with_config', return_value=service):
            for block in blocks:
                entry = entries[block.uuid]
                if entry['language'] != 'sql':
                    outputs[block.uuid] = mage_outputs[block.uuid]
                    continue
                entry = dict(entry, file=str(self.project / entry['file']))
                inputs = [outputs[u['uuid']] for u in entry['sql']['upstream']]
                outputs[block.uuid] = sql.run(entry, inputs, dict(variables), self.repo_path)[0]
        return mage, mage_outputs, service, outputs

    def test_the_service_asks_redshift_what_mage_asks_and_gets_the_same_rows(self):
        orders = self.add('orders', 'data_loader', ORDERS)
        totals = self.sql_block('totals', TOTALS, [orders])
        top = self.sql_block('top', TOP, [totals], export_write_policy='append')

        mage, mage_outputs, service, outputs = self.run_both(
            [orders, totals, top], dict(min_amount=5),
        )

        self.assertEqual(service.calls, mage.calls)
        uploads = [c for c in service.calls if c[0] == 'export' and c[4]]
        # Only the Python block's output is written; the SQL block's table is read as it is.
        self.assertEqual(len(uploads), 1, uploads)
        self.assertEqual(uploads[0][1], 'sales')
        queries = [c[5] for c in service.calls if c[0] == 'export' and not c[4]]
        self.assertIn('FROM dev.sales.', queries[0])
        for block in (totals, top):
            pd.testing.assert_frame_equal(
                outputs[block.uuid].reset_index(drop=True),
                mage_outputs[block.uuid].reset_index(drop=True),
            )
        self.assertEqual(outputs[totals.uuid].to_dict('list'), dict(
            region=['east', 'north', 'south'], total=[7.0, 10.0, 5.5],
        ))
        self.assertEqual(outputs[top.uuid]['region'].tolist(), ['north'])

    def test_a_redshift_sql_block_exports_with_its_client(self):
        orders = self.add('orders', 'data_loader', ORDERS)
        self.sql_block('totals', TOTALS, [orders])

        captured = export.capture(self.repo_path, [self.pipeline.uuid])

        packages = {p.lower().replace('_', '-') for p in captured.requirements}
        self.assertIn('redshift-connector', packages)
