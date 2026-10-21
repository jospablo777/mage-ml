"""
Db2 SQL blocks in pipeline services, against Mage's Db2 branch, on a stand-in for Mage's Db2
io class over DuckDB (integration_tests/db2 runs them on Db2 in CI), and the image of an
export with Db2 blocks, which builds for linux/amd64 only: IBM builds no Db2 client for
Linux on arm64.
"""
import json
import re
import unittest.mock

import duckdb
import pandas as pd
import yaml

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
RAW = '''CREATE TABLE shop.raw_totals AS
SELECT region, COUNT(*) AS orders FROM {{ df_1 }} GROUP BY region ORDER BY region'''
PROFILE = '''warehouse:
  DB2_DATABASE: testdb
  DB2_HOST: db2.example.com
  DB2_PORT: 50000
  DB2_USER: db2inst1
  DB2_PASSWORD: secret
'''  # pragma: allowlist secret


class DuckDb2:
    """Mage's Db2 io class over DuckDB, recording each request; shop is the current schema."""

    def __init__(self):
        self.connection = duckdb.connect()
        self.connection.execute("ATTACH ':memory:' AS testdb")
        self.connection.execute('CREATE SCHEMA testdb.shop')
        self.connection.execute('USE testdb.shop')
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def default_schema(self) -> str:
        return 'shop'

    def _full_table_name(self, schema_name, table_name):
        return f'{schema_name}.{table_name}' if schema_name else table_name

    def export(self, df, schema_name=None, table_name=None, query_string=None,
               if_exists='append', **kwargs):
        self.calls.append(('export', schema_name, table_name, if_exists, df is not None,
                           query_string))
        name = f'{schema_name}.{table_name}'
        self.connection.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')
        source = query_string
        if df is not None:
            self.connection.register('upstream_frame', df)
            source = 'SELECT * FROM upstream_frame'
        exists = self.connection.execute(
            'SELECT count(*) FROM information_schema.tables '
            "WHERE table_catalog = 'testdb' AND table_schema = ? AND table_name = ?",
            [schema_name, table_name],
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

    def execute_queries(self, queries, commit=False, fetch_query_at_indexes=None, **kwargs):
        self.calls.append(('execute', list(queries), list(fetch_query_at_indexes or [])))
        results = []
        for query, fetch in zip(queries, fetch_query_at_indexes or [False] * len(queries)):
            query = re.sub(r'FETCH FIRST (\d+) ROWS ONLY', r'LIMIT \1', query)
            result = self.connection.execute(query)
            results.append(result.df() if fetch else None)
        return results


class Db2SqlTest(ExportTestCase):
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
                data_provider='db2',
                data_provider_profile='warehouse',
                export_write_policy='replace',
            ),
            **configuration,
        })

    def run_both(self, blocks, variables):
        """Runs the blocks in Mage, then in the service runtime, each on its own stand-in."""
        from mage_ai.io.db2 import Db2

        pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        mage = DuckDb2()
        with unittest.mock.patch.object(Db2, 'with_config', return_value=mage):
            for block in blocks:
                pipeline.get_block(block.uuid).execute_sync(global_vars=dict(variables))
        manager = pipeline.variable_manager
        mage_outputs = {
            b.uuid: manager.get_variable(pipeline.uuid, b.uuid, 'output_0') for b in blocks
        }

        # Whether or not ibm_db is installed where the tests run.
        with unittest.mock.patch.object(export, '_client_missing', return_value=False):
            manifest = export.capture(self.repo_path, [self.pipeline.uuid]).manifest
        entries = {b['uuid']: b for b in manifest['pipelines'][0]['blocks']}
        service = DuckDb2()
        outputs = {}
        with unittest.mock.patch.object(Db2, 'with_config', return_value=service):
            for block in blocks:
                entry = entries[block.uuid]
                if entry['language'] != 'sql':
                    outputs[block.uuid] = mage_outputs[block.uuid]
                    continue
                entry = dict(entry, file=str(self.project / entry['file']))
                inputs = [outputs[u['uuid']] for u in entry['sql']['upstream']]
                outputs[block.uuid] = sql.run(entry, inputs, dict(variables), self.repo_path)[0]
        return mage, mage_outputs, service, outputs

    def test_the_service_asks_db2_what_mage_asks_and_gets_the_same_rows(self):
        orders = self.add('orders', 'data_loader', ORDERS)
        totals = self.sql_block('totals', TOTALS, [orders])
        top = self.sql_block('top', TOP, [totals], export_write_policy='append')
        raw = self.sql_block('raw', RAW, [orders], use_raw_sql=True)

        mage, mage_outputs, service, outputs = self.run_both(
            [orders, totals, top, raw], dict(min_amount=5),
        )

        self.assertEqual(service.calls, mage.calls)
        uploads = [c for c in service.calls if c[0] == 'export' and c[4]]
        # The Python block's output is written for each block that reads it, in the current
        # schema; the SQL block's table is read as it is.
        self.assertEqual([u[1] for u in uploads], ['shop', 'shop'])
        for block in (totals, top, raw):
            pd.testing.assert_frame_equal(
                outputs[block.uuid].reset_index(drop=True),
                mage_outputs[block.uuid].reset_index(drop=True),
            )
        self.assertEqual(outputs[totals.uuid].to_dict('list'), dict(
            region=['east', 'north', 'south'], total=[7.0, 10.0, 5.5],
        ))
        self.assertEqual(outputs[top.uuid]['region'].tolist(), ['north'])
        self.assertEqual(sorted(outputs[raw.uuid]['orders'].tolist()), [1, 1, 2])

    def test_an_export_with_db2_blocks_builds_for_amd64_only(self):
        orders = self.add('orders', 'data_loader', ORDERS)
        self.sql_block('totals', TOTALS, [orders])
        out = self.project.parent / f'{self._testMethodName}_out'
        self.addCleanup(__import__('shutil').rmtree, out, True)

        # Whether or not ibm_db is installed where the tests run.
        with unittest.mock.patch.object(export, '_client_missing', return_value=False):
            captured = export.capture(self.repo_path, [self.pipeline.uuid])
            export.write(captured, str(out), force=True)

        self.assertTrue(captured.needs_db2_client)
        self.assertEqual(captured.platforms, ['linux/amd64'])
        self.assertIn("IBM's Db2 client (ibm_db): IBM publishes it for linux/amd64 only",
                      '\n'.join(captured.notes))
        manifest = json.loads((out / 'manifest.json').read_text())
        self.assertEqual(manifest['platforms'], ['linux/amd64'])
        self.assertIn('--platform linux/amd64 ', manifest['build'])
        self.assertIn('if [ "$TARGETARCH" != "amd64" ]', (out / 'Dockerfile').read_text())
        workflow = yaml.safe_load((out / '.github/workflows/build-image.yml').read_text())
        self.assertEqual(
            [e['arch'] for e in workflow['jobs']['image']['strategy']['matrix']['include']],
            ['amd64'],
        )
        readme = (out / 'README.md').read_text()
        self.assertIn('--platform linux/amd64 -t', readme)
        self.assertNotIn('linux/arm64', readme)

    def test_a_block_with_its_own_schema_reads_its_upstream_and_its_table_there(self):
        """
        A block whose schema is not the profile's wrote its upstream table to its schema and
        its own table there, then read both from the current schema (I-269).
        """
        orders = self.add('orders', 'data_loader', ORDERS)
        totals = self.sql_block('totals', TOTALS, [orders], data_provider_schema='reports')
        top = self.sql_block('top', TOP, [totals])

        mage, mage_outputs, service, outputs = self.run_both(
            [orders, totals, top], dict(min_amount=5),
        )

        self.assertEqual(service.calls, mage.calls)
        self.assertEqual(mage_outputs[totals.uuid]['region'].tolist(),
                         ['east', 'north', 'south'])
        written = [c for c in mage.calls if c[0] == 'export' and not c[4]]
        self.assertEqual(written[0][1], 'reports')
        self.assertEqual(mage_outputs[top.uuid]['region'].tolist(), ['north'])
        for block in (totals, top):
            pd.testing.assert_frame_equal(
                outputs[block.uuid].reset_index(drop=True),
                mage_outputs[block.uuid].reset_index(drop=True),
            )

    def test_an_export_without_the_db2_client_installed_is_refused(self):
        orders = self.add('orders', 'data_loader', ORDERS)
        self.sql_block('totals', TOTALS, [orders])

        with unittest.mock.patch.object(export, '_client_missing', return_value=True):
            with self.assertRaises(export.ExportError) as caught:
                export.capture(self.repo_path, [self.pipeline.uuid])

        self.assertIn('whose client (ibm_db) is not installed here', str(caught.exception.problems))
        self.assertIn('mage-ml[db2]', str(caught.exception.problems))
