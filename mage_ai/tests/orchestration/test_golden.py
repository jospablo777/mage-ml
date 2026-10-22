"""
Golden runs (golden.py): a run's outputs fingerprinted as data, recorded as the golden file,
and later runs compared with it block by block, with effect counts from probes.
"""
import textwrap
from pathlib import Path
from unittest import TestCase

import pandas as pd
import polars as pl
import pyarrow as pa
import yaml
from typer.testing import CliRunner

from mage_ai.data_preparation.executors.pipeline_executor import PipelineExecutor
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import golden
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.tests.base_test import DBTestCase


class FingerprintTest(TestCase):
    def test_the_same_data_in_pandas_polars_and_arrow_is_equal(self):
        rows = dict(id=[1, 2, 3], name=['a', 'b', None], value=[1.5, 2.25, 3.0])
        prints = [
            golden.fingerprint(pd.DataFrame(rows)),
            golden.fingerprint(pl.DataFrame(rows)),
            golden.fingerprint(pa.table(rows)),
        ]
        self.assertEqual(prints[0], prints[1])
        self.assertEqual(prints[0], prints[2])
        self.assertEqual(prints[0]['rows'], 3)
        self.assertEqual(prints[0]['columns'],
                         [['id', 'integer'], ['name', 'string'], ['value', 'float']])

    def test_row_order_changes_the_order_hash_only(self):
        first = golden.fingerprint(pd.DataFrame(dict(id=[1, 2, 3])))
        second = golden.fingerprint(pd.DataFrame(dict(id=[3, 1, 2])))
        self.assertEqual(first['content'], second['content'])
        self.assertNotEqual(first['order'], second['order'])

    def test_floats_compare_to_twelve_digits_and_nan_equals_nan(self):
        sums = [0.1 + 0.2 + 0.3, 0.3 + 0.2 + 0.1]
        self.assertNotEqual(sums[0], sums[1])
        self.assertEqual(
            golden.fingerprint(pd.DataFrame(dict(v=[sums[0], float('nan')]))),
            golden.fingerprint(pd.DataFrame(dict(v=[sums[1], float('nan')]))),
        )
        self.assertNotEqual(
            golden.fingerprint(pd.DataFrame(dict(v=[0.6]))),
            golden.fingerprint(pd.DataFrame(dict(v=[0.6000001]))),
        )

    def test_integer_widths_are_one_kind_and_time_zones_another(self):
        self.assertEqual(
            golden.fingerprint(pa.table(dict(n=pa.array([1], pa.int32())))),
            golden.fingerprint(pa.table(dict(n=pa.array([1], pa.int64())))),
        )
        naive = golden.fingerprint(pd.DataFrame(dict(t=pd.to_datetime(['2026-01-01']))))
        aware = golden.fingerprint(pd.DataFrame(dict(
            t=pd.to_datetime(['2026-01-01']).tz_localize('UTC'),
        )))
        self.assertEqual(naive['columns'], [['t', 'datetime']])
        self.assertEqual(aware['columns'], [['t', 'datetime_tz']])

    def test_a_groupby_index_is_data(self):
        frame = pd.DataFrame(dict(k=['a', 'b'], v=[1, 2])).set_index('k')
        self.assertEqual(golden.fingerprint(frame)['columns'],
                         [['k', 'string'], ['v', 'integer']])

    def test_geo_frames_are_tables_with_their_geometries_as_text(self):
        import geopandas as gpd
        from shapely.geometry import Point

        stores = gpd.GeoDataFrame(
            {'id': [1, 2]}, geometry=[Point(-84.0912345678901, 9.93), Point(-84.1, 9.94)],
            crs=4326,
        )
        found = golden.fingerprint(stores)
        # A file's bytes differ by platform; the table does not.
        self.assertEqual(found['kind'], 'table')
        self.assertEqual(found['columns'], [['id', 'integer'], ['geometry', 'string']])
        moved = stores.set_geometry(stores.geometry.translate(1e-12, 0))
        self.assertEqual(golden.fingerprint(moved)['content'], found['content'])
        apart = stores.set_geometry(stores.geometry.translate(1e-6, 0))
        self.assertNotEqual(golden.fingerprint(apart)['content'], found['content'])
        self.assertEqual(golden.fingerprint(stores.geometry)['kind'], 'table')

    def test_json_outputs(self):
        self.assertEqual(golden.fingerprint(dict(b=1, a=[1, 2]))['rows'], 2)
        self.assertEqual(
            golden.fingerprint(dict(b=1, a=2))['content'],
            golden.fingerprint(dict(a=2, b=1))['content'],
        )
        self.assertEqual(golden.fingerprint([dict(x=1), dict(x=2)])['kind'], 'list')
        with self.assertRaises(golden.GoldenError):
            golden.fingerprint(object())


class GoldenRunTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)
        self.export = Path(self.repo_path) / 'exports' / f'{self.pipeline.uuid}.csv'

    def block(self, name, kind, code, upstream=()):
        block = Block.create(
            f'{self.pipeline.uuid}_{name}', kind, self.repo_path, language='python',
        )
        Path(block.file_path).write_text(textwrap.dedent(code))
        self.pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    def flow(self):
        load = self.block('load', 'data_loader', '''
            import pandas as pd

            @data_loader
            def load(**kwargs):
                return pd.DataFrame(dict(id=[1, 2, 3], amount=[10.0, 20.0, 30.0]))
        ''')
        double = self.block('double', 'transformer', '''
            @transformer
            def double(df, **kwargs):
                return df.assign(amount=df['amount'] * 2)
        ''', upstream=[load])
        self.block('write', 'data_exporter', f'''
            import os

            @data_exporter
            def write(df, **kwargs):
                path = {str(self.export)!r}
                os.makedirs(os.path.dirname(path), exist_ok=True)
                header = not os.path.exists(path)
                df.to_csv(path, mode='a', header=header, index=False)
        ''', upstream=[double])
        self.pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)

    def run_pipeline(self) -> PipelineRun:
        PipelineExecutor(self.pipeline).execute(record_run=True, update_status=False)
        return golden.latest_completed_run(self.pipeline)

    def declare_effects(self):
        path = Path(golden.golden_path(self.pipeline))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(dict(effects=dict(
            rows_exported=dict(file=f'exports/{self.pipeline.uuid}.csv'),
        ))))

    def test_a_recorded_run_compares_equal_and_a_changed_block_differs(self):
        self.flow()
        self.declare_effects()
        first = self.run_pipeline()
        document = golden.record(self.pipeline, first)
        self.assertEqual(document['effects']['rows_exported']['count'], 3)
        load = document['blocks'][f'{self.pipeline.uuid}_load']['output_0']
        self.assertEqual(load['rows'], 3)
        self.assertEqual(load['columns'], [['id', 'integer'], ['amount', 'float']])

        comparison = golden.compare(self.pipeline, first)
        self.assertTrue(comparison['passed'], golden.format_comparison(comparison))
        self.assertEqual({b['result'] for b in comparison['blocks']}, {golden.EQUAL})

        # The exporter appends: a second run doubles the file, which the probe sees.
        self.export.unlink()
        double = self.pipeline.get_block(f'{self.pipeline.uuid}_double')
        Path(double.file_path).write_text(textwrap.dedent('''
            @transformer
            def double(df, **kwargs):
                return df.assign(amount=df['amount'] * 3)
        '''))
        self.pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)
        second = self.run_pipeline()
        comparison = golden.compare(self.pipeline, second)
        results = {b['block_uuid'].split('_')[-1]: b for b in comparison['blocks']}
        self.assertEqual(results['load']['result'], golden.EQUAL)
        self.assertEqual(results['double']['result'], golden.DIFFERENT)
        self.assertEqual(results['double']['differences'], ['output_0: values differ'])
        self.assertEqual(comparison['effects'][0]['result'], golden.EQUAL)
        self.assertFalse(comparison['passed'])

        third = self.run_pipeline()
        comparison = golden.compare(self.pipeline, third)
        effect = comparison['effects'][0]
        self.assertEqual((effect['result'], effect['expected'], effect['actual']),
                         (golden.DIFFERENT, 3, 6))
        text = golden.format_comparison(comparison)
        self.assertIn('values differ', text)
        self.assertIn('expected 3, counted 6', text)

    def test_the_command_records_and_compares(self):
        from mage_ai.cli.main import app

        self.flow()
        run = self.run_pipeline()
        runner = CliRunner()
        recorded = runner.invoke(app, ['golden', 'record', self.repo_path, self.pipeline.uuid])
        self.assertEqual(recorded.exit_code, 0, recorded.output)
        self.assertIn(f'run {run.id}, 3 blocks', recorded.output)
        compared = runner.invoke(app, ['golden', 'compare', self.repo_path, self.pipeline.uuid,
                                       '--run', str(run.id)])
        self.assertEqual(compared.exit_code, 0, compared.output)
        self.assertIn('Equal.', compared.output)

    def test_compare_without_a_golden_file_says_how_to_make_one(self):
        self.flow()
        run = self.run_pipeline()
        with self.assertRaises(golden.GoldenError) as raised:
            golden.compare(self.pipeline, run)
        self.assertIn('mage golden record', str(raised.exception))

    def test_a_sql_probe_counts_rows_through_the_projects_io_config(self):
        import duckdb

        database = Path(self.repo_path) / 'golden_probe.duckdb'
        with duckdb.connect(str(database)) as connection:
            connection.execute('CREATE OR REPLACE TABLE written AS SELECT * FROM range(7)')
        config = Path(self.repo_path) / 'io_config.yaml'
        kept = config.read_text() if config.exists() else None
        config.write_text(f'default:\n  DUCKDB_DATABASE: {database}\n')
        try:
            self.assertEqual(golden.count_effect(dict(
                data_provider='duckdb', sql='SELECT count(*) FROM written',
            ), self.repo_path), 7)
            with self.assertRaises(golden.GoldenError):
                golden.count_effect(dict(data_provider='sqlite', sql='SELECT 1'),
                                    self.repo_path)
        finally:
            if kept is None:
                config.unlink()
            else:
                config.write_text(kept)
