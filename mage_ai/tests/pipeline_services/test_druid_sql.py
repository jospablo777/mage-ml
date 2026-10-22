"""
Druid SQL blocks in pipeline services, against Mage's Druid branch. Druid takes queries,
not writes, so its blocks run raw SQL. No Druid runs here: both run on a stand-in for
Mage's Druid client over DuckDB, so the test compares the queries they send and the rows
that come back.
"""
import contextlib
import io
import unittest.mock

import duckdb
import pandas as pd

from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.pipeline_services import export
from mage_ai.pipeline_services.runtime import sql
from mage_ai.tests.pipeline_services.test_export import ExportTestCase

EVENTS = '''SELECT region, SUM(amount) AS total
FROM wikipedia_events
WHERE amount >= {{ min_amount }}
GROUP BY region
ORDER BY region'''
PROFILE = '''olap:
  DRUID_HOST: broker.example.com
  DRUID_PORT: 8082
  DRUID_PATH: /druid/v2/sql/
  DRUID_SCHEME: https
  DRUID_USER: reader
  DRUID_PASSWORD: secret
'''  # pragma: allowlist secret


class DuckDruid:
    """Mage's Druid client over DuckDB with one datasource, recording each request."""

    def __init__(self):
        self.connection = duckdb.connect()
        self.connection.execute(
            "CREATE TABLE wikipedia_events AS SELECT * FROM (VALUES "
            "('north', 10.0), ('south', 5.5), ('north', 2.5), ('east', 7.0)) t(region, amount)"
        )
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute_queries(self, queries, query_variables=None, commit=False,
                        fetch_query_at_indexes=None):
        self.calls.append(('queries', list(queries), list(fetch_query_at_indexes or [])))
        results = []
        for query, fetch in zip(queries, fetch_query_at_indexes or [False] * len(queries)):
            frame = self.connection.execute(query).df()
            results.append(frame if fetch else None)
        return results

    def export(self, *args, **kwargs):
        raise Exception('write operations are not supported.')


class DruidSqlTest(ExportTestCase):
    def setUp(self):
        super().setUp()
        io_config = self.project / 'io_config.yaml'
        previous = io_config.read_text() if io_config.exists() else None
        io_config.write_text(PROFILE)
        self.addCleanup(
            lambda: io_config.write_text(previous) if previous is not None
            else io_config.unlink(),
        )

    def druid_block(self, name, code, raw=True):
        return self.add(name, 'data_loader', code, [], language='sql', configuration=dict(
            data_provider='druid', data_provider_profile='olap', use_raw_sql=raw,
        ))

    def test_the_service_queries_druid_as_mage_does_and_gets_the_same_rows(self):
        from mage_ai.io.druid import Druid

        events = self.druid_block('events', EVENTS)
        variables = dict(min_amount=5)

        pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        mage = DuckDruid()
        with unittest.mock.patch.object(Druid, 'with_config', return_value=mage):
            pipeline.get_block(events.uuid).execute_sync(global_vars=dict(variables))
        mage_output = pipeline.variable_manager.get_variable(
            pipeline.uuid, events.uuid, 'output_0',
        )

        manifest = export.capture(self.repo_path, [self.pipeline.uuid]).manifest
        entry = manifest['pipelines'][0]['blocks'][0]
        entry = dict(entry, file=str(self.project / entry['file']))
        service = DuckDruid()
        with unittest.mock.patch.object(Druid, 'with_config', return_value=service):
            [output] = sql.run(entry, [], dict(variables), self.repo_path)

        self.assertEqual(service.calls, mage.calls)
        self.assertIn('WHERE amount >= 5', service.calls[0][1][0])
        pd.testing.assert_frame_equal(
            output.reset_index(drop=True), mage_output.reset_index(drop=True),
        )
        self.assertEqual(output.to_dict('list'), dict(
            region=['east', 'north', 'south'], total=[7.0, 10.0, 5.5],
        ))

    def test_a_druid_block_that_writes_its_table_stops_the_export(self):
        self.druid_block('events', EVENTS, raw=False)

        with self.assertRaises(export.ExportError) as raised:
            export.capture(self.repo_path, [self.pipeline.uuid])

        self.assertIn('Druid takes queries only', str(raised.exception))

    def test_a_druid_sql_block_exports_with_its_client(self):
        self.druid_block('events', EVENTS)

        captured = export.capture(self.repo_path, [self.pipeline.uuid])

        packages = {p.lower().replace('_', '-') for p in captured.requirements}
        self.assertIn('pydruid', packages)


class DruidConnectionTest(unittest.TestCase):
    def test_opening_a_connection_prints_no_password(self):
        from mage_ai.io import druid

        client = druid.Druid(host='broker', port=8082, user='reader', password='hunter2',
                             verbose=False)
        printed = io.StringIO()
        with unittest.mock.patch.object(druid, 'ConnectionWrapper'), \
                contextlib.redirect_stdout(printed):
            client.open()
        self.assertNotIn('hunter2', printed.getvalue())
