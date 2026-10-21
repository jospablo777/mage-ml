"""
BigQuery SQL blocks in pipeline services, against Mage's BigQuery branch. No BigQuery runs
here (its emulator does not start under emulation on arm64): both run on a stand-in for
Mage's BigQuery io class over DuckDB, so the test compares what they ask of BigQuery
(which upstream outputs become tables, under which names, with which write policy) and
the rows that come back.
"""
import json
import unittest.mock
from pathlib import Path

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
EMPTY = '''
    import pandas as pd


    @data_loader
    def load(*args, **kwargs):
        return pd.DataFrame({'region': pd.Series([], dtype='string')})
'''
# Reads the Python block's output (written to a table first) and filters by a variable.
TOTALS = '''SELECT region, SUM(amount) AS total
FROM {{ df_1 }}
WHERE amount >= {{ min_amount }}
GROUP BY region
ORDER BY region'''
# Reads the SQL block's table as it is: same profile, no copy.
TOP = 'SELECT region, total FROM {{ df_1 }} WHERE total > 7 ORDER BY region'
KEY = dict(type='service_account', project_id='shop_analytics', client_email='x@y.z')


class DuckBigQuery:
    """
    Mage's BigQuery io class over DuckDB, which names tables catalog.schema.table as
    BigQuery names them project.dataset.table. It records each request.
    """

    def __init__(self, project: str):
        self.project = project
        self.connection = duckdb.connect()
        self.connection.execute(f"ATTACH ':memory:' AS \"{project}\"")
        self.calls = []

    def default_database(self) -> str:
        return self.project

    def export(self, df, table_id, database=None, if_exists='replace', query_string=None,
               verbose=True, **kwargs):
        self.calls.append(('export', database, table_id, if_exists, df is not None, query_string))
        name = f'{database}.{table_id}' if database else table_id
        catalog, schema, table = name.split('.')
        self.connection.execute(f'CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}')
        source = query_string
        if df is not None:
            self.connection.register('upstream_frame', df)
            source = 'SELECT * FROM upstream_frame'
        exists = self.connection.execute(
            'SELECT count(*) FROM information_schema.tables '
            'WHERE table_catalog = ? AND table_schema = ? AND table_name = ?',
            [catalog, schema, table],
        ).fetchone()[0]
        if exists and if_exists == 'append':
            self.connection.execute(f'INSERT INTO {name} {source}')
        elif exists and if_exists == 'fail':
            raise ValueError(f'Table {name} already exists.')
        else:
            self.connection.execute(f'CREATE OR REPLACE TABLE {name} AS {source}')

    def load(self, query_string, limit=10_000_000, verbose=True, **kwargs):
        self.calls.append(('load', query_string))
        return self.connection.execute(
            f'SELECT * FROM ({query_string}) LIMIT {int(limit)}',
        ).df()


class BigQuerySqlTest(ExportTestCase):
    def setUp(self):
        super().setUp()
        io_config = self.project / 'io_config.yaml'
        previous = io_config.read_text() if io_config.exists() else None
        io_config.write_text(
            f'warehouse:\n  GOOGLE_SERVICE_ACC_KEY: {json.dumps(KEY)}\n'
            '  GOOGLE_LOCATION: US\n'
        )
        self.addCleanup(
            lambda: io_config.write_text(previous) if previous is not None
            else io_config.unlink(),
        )

    def sql_block(self, name, code, upstream, **configuration):
        return self.add(name, 'transformer', code, upstream, language='sql', configuration={
            **dict(
                data_provider='bigquery',
                data_provider_profile='warehouse',
                data_provider_schema='sales',
                export_write_policy='replace',
            ),
            **configuration,
        })

    def run_both(self, blocks, variables):
        """Runs the blocks in Mage, then in the service runtime, each on its own stand-in."""
        from mage_ai.io.bigquery import BigQuery

        pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        mage = DuckBigQuery(KEY['project_id'])
        with unittest.mock.patch.object(BigQuery, 'with_config', return_value=mage):
            for block in blocks:
                pipeline.get_block(block.uuid).execute_sync(global_vars=dict(variables))
        manager = pipeline.variable_manager
        mage_outputs = {
            b.uuid: manager.get_variable(pipeline.uuid, b.uuid, 'output_0') for b in blocks
        }

        manifest = export.capture(self.repo_path, [self.pipeline.uuid]).manifest
        entries = {b['uuid']: b for b in manifest['pipelines'][0]['blocks']}
        service = DuckBigQuery(KEY['project_id'])
        outputs = {}
        with unittest.mock.patch.object(BigQuery, 'with_config', return_value=service):
            for block in blocks:
                entry = entries[block.uuid]
                if entry['language'] != 'sql':
                    outputs[block.uuid] = mage_outputs[block.uuid]
                    continue
                entry = dict(entry, file=str(self.project / entry['file']))
                inputs = [outputs[u['uuid']] for u in entry['sql']['upstream']]
                outputs[block.uuid] = sql.run(entry, inputs, dict(variables), self.repo_path)[0]
        return mage, mage_outputs, service, outputs

    def test_the_service_asks_bigquery_what_mage_asks_and_gets_the_same_rows(self):
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
        self.assertEqual(uploads[0][1], 'shop_analytics')
        self.assertTrue(uploads[0][2].startswith(f'sales.{self.pipeline.uuid}_'), uploads)
        self.assertEqual(uploads[0][3], 'replace')
        self.assertIn(f'FROM shop_analytics.sales.{self.pipeline.uuid}_',
                      next(c for c in service.calls if c[0] == 'load')[1])
        for block in (totals, top):
            pd.testing.assert_frame_equal(
                outputs[block.uuid].reset_index(drop=True),
                mage_outputs[block.uuid].reset_index(drop=True),
            )
        self.assertEqual(outputs[totals.uuid].to_dict('list'), dict(
            region=['east', 'north', 'south'], total=[7.0, 10.0, 5.5],
        ))
        self.assertEqual(outputs[top.uuid]['region'].tolist(), ['north'])

    def test_an_empty_upstream_frame_is_not_written_as_in_mage(self):
        from mage_ai.io.bigquery import BigQuery

        empty = self.add('empty', 'data_loader', EMPTY)
        reader = self.sql_block('reader', 'SELECT COUNT(*) AS n FROM {{ df_1 }}', [empty])
        pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        pipeline.get_block(empty.uuid).execute_sync()
        frame = pipeline.variable_manager.get_variable(pipeline.uuid, empty.uuid, 'output_0')
        entry = next(
            b for b in export.capture(self.repo_path, [self.pipeline.uuid])
            .manifest['pipelines'][0]['blocks'] if b['uuid'] == reader.uuid
        )
        entry = dict(entry, file=str(self.project / entry['file']))

        # Not written, the table is missing on both sides (Mage's branch has not tested an
        # empty frame on BigQuery, and it reads what an earlier run left).
        mage, service = DuckBigQuery(KEY['project_id']), DuckBigQuery(KEY['project_id'])
        with unittest.mock.patch.object(BigQuery, 'with_config', return_value=mage):
            with self.assertRaises(Exception) as in_mage:
                pipeline.get_block(reader.uuid).execute_sync()
        with unittest.mock.patch.object(BigQuery, 'with_config', return_value=service):
            with self.assertRaises(Exception) as in_service:
                sql.run(entry, [frame], {}, self.repo_path)

        self.assertIn('does not exist', str(in_mage.exception))
        self.assertIn('does not exist', str(in_service.exception))
        self.assertEqual(service.calls, mage.calls)
        self.assertFalse(any(c[0] == 'export' and c[4] for c in service.calls))

    def test_another_profile_of_the_same_project_reads_the_table_and_another_copies_it(self):
        io_config = self.project / 'io_config.yaml'
        other = dict(KEY, project_id='other-project')
        io_config.write_text(
            io_config.read_text()
            + f'same_project:\n  GOOGLE_SERVICE_ACC_KEY: {json.dumps(KEY)}\n'
            '  GOOGLE_LOCATION: US\n'
            + f'other_project:\n  GOOGLE_SERVICE_ACC_KEY: {json.dumps(other)}\n'
            '  GOOGLE_LOCATION: US\n'
        )
        orders = self.add('orders', 'data_loader', ORDERS)
        totals = self.sql_block('totals', TOTALS, [orders])
        same = self.sql_block('same', TOP, [totals], data_provider_profile='same_project')
        repo_io = str(io_config)
        self.assertFalse(sql.should_upload(
            dict(data_provider='bigquery', data_provider_profile='same_project'),
            dict(language='sql', configuration=dict(
                data_provider='bigquery', data_provider_profile='warehouse')),
            repo_io, (),
        ))
        self.assertTrue(sql.should_upload(
            dict(data_provider='bigquery', data_provider_profile='other_project'),
            dict(language='sql', configuration=dict(
                data_provider='bigquery', data_provider_profile='warehouse')),
            repo_io, (),
        ))

        mage, _, service, _ = self.run_both([orders, totals, same], dict(min_amount=0))

        self.assertEqual(service.calls, mage.calls)

    def test_a_bigquery_sql_block_exports_with_its_client(self):
        orders = self.add('orders', 'data_loader', ORDERS)
        self.sql_block('totals', TOTALS, [orders])

        captured = export.capture(self.repo_path, [self.pipeline.uuid])

        packages = {p.lower().replace('_', '-') for p in captured.requirements}
        self.assertIn('google-cloud-bigquery', packages)
        self.assertTrue(Path(self.project / 'io_config.yaml').is_file())
