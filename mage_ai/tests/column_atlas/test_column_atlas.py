import asyncio
import os
import textwrap
import time
from unittest.mock import MagicMock, patch

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.column_atlas.errors import (
    AtlasEngineError,
    AtlasInvalidRequest,
    AtlasTimeout,
    AtlasUnavailable,
)
from mage_ai.column_atlas.pool import WorkerPool
from mage_ai.column_atlas.service import ColumnAtlasService
from mage_ai.column_atlas.sources import parse_source, resolve
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration.db.models.schedules import BlockRun
from mage_ai.tests.base_test import AsyncDBTestCase, DBTestCase, TestCase
from mage_ai.tests.factory import create_pipeline_run_with_schedule, create_user

LOADER = '''
import pandas as pd
@data_loader
def load(**kwargs):
    return pd.DataFrame({
        'id': pd.Series(range(1, 101), dtype='Int64'),
        'name': pd.Series([f'n{i % 7}' if i % 10 else None for i in range(100)], dtype='str'),
        'amount': [i * 1.5 for i in range(100)],
    })
'''


def slow_worker(connection, *args):
    connection.recv()
    time.sleep(30)


def dying_worker(connection, *args):
    connection.recv()
    os._exit(3)


def answering_worker(connection, *args):
    while True:
        request = connection.recv()
        connection.send(('ok', os.getpid() if request.get('action') == 'pid' else request))


class OutputMixin:
    def output_pipeline(self):
        pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        block = Block.create(f'{pipeline.uuid}_load', 'data_loader', self.repo_path,
                             pipeline=pipeline)
        with open(block.file_path, 'w') as file:
            file.write(textwrap.dedent(LOADER))
        block.execute_sync()
        return pipeline, block


class SourceTest(OutputMixin, DBTestCase):
    def test_a_stored_dataframe_resolves_to_its_parquet_file(self):
        pipeline, block = self.output_pipeline()

        resolved = resolve(parse_source(dict(
            pipeline_uuid=pipeline.uuid, block_uuid=block.uuid, variable_uuid='output_0',
        )), self.repo_path)

        self.assertTrue(resolved.path.endswith(os.path.join('output_0', 'data.parquet')))
        self.assertTrue(os.path.exists(resolved.path))
        self.assertEqual(resolved.label, f'{block.uuid} / output_0')

    def test_a_new_write_changes_the_generation(self):
        pipeline, block = self.output_pipeline()
        source = parse_source(dict(pipeline_uuid=pipeline.uuid, block_uuid=block.uuid))
        first = resolve(source, self.repo_path).generation
        time.sleep(0.01)
        block.execute_sync()

        self.assertNotEqual(resolve(source, self.repo_path).generation, first)

    def test_a_block_run_reads_its_runs_output(self):
        pipeline, block = self.output_pipeline()
        run = create_pipeline_run_with_schedule(pipeline_uuid=pipeline.uuid)
        block_run = BlockRun.get(pipeline_run_id=run.id, block_uuid=block.uuid)
        block.execute_sync(execution_partition=run.execution_partition)

        resolved = resolve(parse_source(dict(
            pipeline_uuid=pipeline.uuid, block_uuid=block.uuid, block_run_id=block_run.id,
        )), self.repo_path)

        self.assertIn(run.execution_partition, resolved.path)
        # A block run of another block, or of another pipeline, is refused.
        with self.assertRaises(AtlasUnavailable):
            resolve(parse_source(dict(
                pipeline_uuid=pipeline.uuid, block_uuid='other', block_run_id=block_run.id,
            )), self.repo_path)

    def test_sources_that_cannot_be_explored(self):
        pipeline, block = self.output_pipeline()
        for payload in [
            dict(pipeline_uuid='../etc', block_uuid=block.uuid),
            dict(pipeline_uuid=pipeline.uuid, block_uuid='a/../../b'),
            dict(pipeline_uuid=pipeline.uuid, block_uuid=block.uuid, variable_uuid='df'),
            dict(pipeline_uuid=pipeline.uuid, block_uuid=block.uuid, block_run_id='x'),
            dict(pipeline_uuid=pipeline.uuid, block_uuid=block.uuid, block_run_id=True),
            'not a source',
        ]:
            with self.subTest(payload):
                with self.assertRaises(AtlasInvalidRequest):
                    parse_source(payload)
        for payload in [
            dict(pipeline_uuid='missing', block_uuid=block.uuid),
            dict(pipeline_uuid=pipeline.uuid, block_uuid='missing'),
            dict(pipeline_uuid=pipeline.uuid, block_uuid=block.uuid, variable_uuid='output_9'),
        ]:
            with self.subTest(payload):
                with self.assertRaises(AtlasUnavailable):
                    resolve(parse_source(payload), self.repo_path)

    def test_outputs_that_are_not_dataframes_are_not_explored(self):
        pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        block = Block.create(f'{pipeline.uuid}_dict', 'data_loader', self.repo_path,
                             pipeline=pipeline)
        with open(block.file_path, 'w') as file:
            file.write('@data_loader\ndef load(**kwargs):\n    return {"a": 1}\n')
        block.execute_sync()

        with self.assertRaises(AtlasUnavailable):
            resolve(parse_source(dict(pipeline_uuid=pipeline.uuid, block_uuid=block.uuid)),
                    self.repo_path)


class PoolTest(TestCase):
    def test_queries_run_in_worker_processes(self):
        pool = WorkerPool(workers=2, timeout_seconds=20, target=answering_worker)
        self.addCleanup(pool.close)

        pid = pool.call('source-a', dict(action='pid'))

        self.assertNotEqual(pid, os.getpid())
        # Requests for one source go to the same worker, which keeps its caches.
        self.assertEqual(pool.call('source-a', dict(action='pid')), pid)

    def test_a_query_past_its_deadline_is_stopped(self):
        pool = WorkerPool(workers=1, timeout_seconds=2, target=slow_worker)
        self.addCleanup(pool.close)

        started = time.monotonic()
        with self.assertRaises(AtlasTimeout):
            pool.call('source', dict(action='count'))

        self.assertLess(time.monotonic() - started, 10)
        self.assertIsNone(pool._workers[0].process)

    def test_a_worker_that_dies_is_replaced(self):
        pool = WorkerPool(workers=1, timeout_seconds=20, target=dying_worker)
        self.addCleanup(pool.close)

        with self.assertRaises(AtlasEngineError):
            pool.call('source', dict(action='count'))
        pool._workers[0].target = answering_worker

        self.assertEqual(pool.call('source', dict(action='echo')), dict(action='echo'))


class ServiceTest(OutputMixin, DBTestCase):
    def setUp(self):
        super().setUp()
        self.pool = WorkerPool(workers=1, timeout_seconds=60)
        self.addCleanup(self.pool.close)
        self.service = ColumnAtlasService(self.pool)
        pipeline, block = self.output_pipeline()
        self.source = parse_source(dict(pipeline_uuid=pipeline.uuid, block_uuid=block.uuid))
        self.resolved = resolve(self.source, self.repo_path)

    def query(self, action, **payload):
        return asyncio.run(self.service.query(action, self.source, self.resolved, payload))

    def test_metadata_counts_rows_and_summaries(self):
        metadata = self.query('metadata')
        self.assertEqual(metadata['row_count'], 100)
        self.assertEqual([c['name'] for c in metadata['columns']], ['id', 'name', 'amount'])
        self.assertEqual(metadata['label'], self.resolved.label)

        view = dict(filters=[dict(column_id=0, op='gt', value='90')],
                    sort=[dict(column_id=2, descending=True)])
        self.assertEqual(self.query('count', view=view)['row_count'], 10)
        rows = self.query('rows', view=view, offset=0, limit=3, columns=[0, 2])
        self.assertEqual(rows['rows'], [['100', '148.5'], ['99', '147.0'], ['98', '145.5']])

        (name,) = self.query('summaries', view={}, columns=[1], bins=8)['summaries']
        self.assertEqual(name['missing'], 10)
        self.assertEqual(name['distinct'], 7)

    def test_every_result_names_the_output_version(self):
        version = self.resolved.generation
        self.assertEqual(self.query('metadata')['source_version'], version)
        self.assertEqual(self.query('count', view={})['source_version'], version)
        rows = self.query('rows', view={}, offset=0, limit=2, columns=[0])
        self.assertEqual(rows['source_version'], version)
        summaries = self.query('summaries', view={}, columns=[0])
        self.assertEqual(summaries['source_version'], version)

        # The block runs again: the next answer names the new version.
        time.sleep(0.01)
        pipeline = Pipeline.get(self.source.pipeline_uuid, repo_path=self.repo_path)
        pipeline.get_block(self.source.block_uuid).execute_sync()
        self.resolved = resolve(self.source, self.repo_path)
        self.assertNotEqual(self.resolved.generation, version)
        self.assertEqual(self.query('count', view={})['source_version'], self.resolved.generation)

    def test_results_are_cached_by_output_generation(self):
        calls = MagicMock(wraps=self.pool.call)
        with patch.object(self.pool, 'call', calls):
            self.query('count', view={})
            self.query('count', view={})
        self.assertEqual(calls.call_count, 1)

    def test_invalid_requests(self):
        for action, payload in [
            ('drop', {}),
            ('rows', dict(offset=-1, limit=10, columns=[0])),
            ('rows', dict(offset=0, limit=600, columns=[0])),
            ('rows', dict(offset=0, limit=10, columns=[])),
            ('rows', dict(offset=0, limit=10, columns=[0, 0])),
            ('summaries', dict(columns=[0], bins=2)),
            ('count', dict(view='filters')),
        ]:
            with self.subTest(action=action, payload=payload):
                with self.assertRaises(AtlasInvalidRequest):
                    self.query(action, **payload)
        # The engine rejects a filter that does not fit the column.
        with self.assertRaises(AtlasInvalidRequest):
            self.query('count', view=dict(filters=[dict(column_id=0, op='contains', value='1')]))


class EndpointTest(OutputMixin, AsyncDBTestCase):
    async def query(self, user, **payload):
        return await BaseOperation(
            action=OperationType.CREATE,
            payload=dict(column_atlas_query=payload),
            resource='column_atlas_queries',
            user=user,
        ).execute()

    async def test_the_endpoint_queries_an_output(self):
        pipeline, block = self.output_pipeline()
        owner = create_user(_owner=True)
        pool = WorkerPool(workers=1, timeout_seconds=60)
        self.addCleanup(pool.close)
        with patch('mage_ai.column_atlas.service._service', ColumnAtlasService(pool)):
            response = await self.query(
                owner,
                action='count',
                source=dict(pipeline_uuid=pipeline.uuid, block_uuid=block.uuid),
                view=dict(filters=[dict(column_id=1, op='is_null')]),
            )
            missing = await self.query(
                owner,
                action='metadata',
                source=dict(pipeline_uuid=pipeline.uuid, block_uuid='missing'),
            )
            anonymous = await self.query(
                None,
                action='count',
                source=dict(pipeline_uuid=pipeline.uuid, block_uuid=block.uuid),
            )
            without_role = await self.query(
                create_user(),
                action='count',
                source=dict(pipeline_uuid=pipeline.uuid, block_uuid=block.uuid),
            )

        self.assertNotIn('error', response, response.get('error'))
        result = response['column_atlas_query']['result']
        self.assertEqual(result['row_count'], 10)
        self.assertTrue(result['source_version'])
        self.assertEqual(missing['error']['code'], 404)
        self.assertEqual(missing['error']['message'], 'The block does not exist')
        self.assertEqual(anonymous['error']['code'], 403)
        self.assertEqual(without_role['error']['code'], 403)


GEO_LOADER = '''
import geopandas as gpd
@data_loader
def load(**kwargs):
    return gpd.GeoDataFrame(
        {'store': ['San José', 'Cartago', 'Limón']},
        geometry=gpd.points_from_xy([-84.0907, -83.9194, None], [9.9281, 9.8644, None]),
        crs='EPSG:4326',
    )
'''


class GeoAndUnreadableOutputTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pool = WorkerPool(workers=1, timeout_seconds=60)
        self.addCleanup(self.pool.close)
        self.service = ColumnAtlasService(self.pool)

    def query(self, source, resolved, action, **payload):
        return asyncio.run(self.service.query(action, source, resolved, payload))

    def test_geodataframe_geometry_shows_as_wkt(self):
        pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        block = Block.create(f'{pipeline.uuid}_geo', 'data_loader', self.repo_path,
                             pipeline=pipeline)
        with open(block.file_path, 'w') as file:
            file.write(textwrap.dedent(GEO_LOADER))
        block.execute_sync()
        source = parse_source(dict(pipeline_uuid=pipeline.uuid, block_uuid=block.uuid))
        resolved = resolve(source, self.repo_path)

        metadata = self.query(source, resolved, 'metadata')
        geometry = next(c for c in metadata['columns'] if c['name'] == 'geometry')
        self.assertEqual(geometry['dtype'], 'Geometry(Point)')
        self.assertEqual(geometry['kind'], 'other')
        self.assertFalse(geometry['exact_integer'])

        rows = self.query(source, resolved, 'rows', view={}, offset=0, limit=3,
                          columns=[geometry['id']])
        self.assertEqual(rows['rows'][0], ['POINT (-84.0907 9.9281)'])
        self.assertIn(rows['rows'][2], [[None], ['POINT EMPTY']])
        (summary,) = self.query(source, resolved, 'summaries', view={},
                                columns=[geometry['id']])['summaries']
        self.assertEqual(summary['count'], 3)

    def test_a_file_the_engine_cannot_read_fails_the_request_not_the_worker(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        from mage_ai.column_atlas.sources import ResolvedSource

        path = os.path.join(self.repo_path, 'broken.parquet')
        pq.write_table(pa.table({'id': list(range(1000))}), path)
        with open(path, 'r+b') as file:
            file.truncate(os.path.getsize(path) // 2)
        source = parse_source(dict(pipeline_uuid='p', block_uuid='b'))
        resolved = ResolvedSource(path=path, generation='1', label='b / output_0')

        worker = self.pool._worker_for(source.key)
        with self.assertRaises(AtlasEngineError) as context:
            self.query(source, resolved, 'metadata')
        self.assertNotIn(path, str(context.exception))
        pid = worker.process.pid
        with self.assertRaises(AtlasEngineError):
            self.query(source, ResolvedSource(path=path, generation='2', label='b'), 'metadata')
        self.assertTrue(worker.alive())
        self.assertEqual(worker.process.pid, pid)
