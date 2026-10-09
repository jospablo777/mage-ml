"""
GeoDataFrame outputs, stored as GeoParquet. They were stored as plain pandas frames and
came back without their geometry type and coordinate reference system.
"""
import textwrap
import unittest
from unittest.mock import patch

import pandas as pd

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.data_preparation.models.utils import infer_variable_type
from mage_ai.data_preparation.models.variable import Variable
from mage_ai.data_preparation.models.variables.constants import VariableType
from mage_ai.tests.base_test import DBTestCase
from mage_ai.tests.factory import create_pipeline

try:
    import geopandas as gpd
    from shapely.geometry import LineString, Point, Polygon
except ImportError:  # The geo extra is not installed.
    gpd = None


def frame(rows=3):
    return gpd.GeoDataFrame(
        {
            'name': pd.Series([f'n{i}' for i in range(rows)], dtype='str'),
            'observed_at': pd.date_range('2024-01-01', periods=rows, freq='h', tz='UTC'),
            'a_long_column_name_over_ten': range(rows),
        },
        geometry=[
            [Point(-84.1, 9.9), LineString([(0, 0), (1, 1)]),
             Polygon([(0, 0), (1, 0), (1, 1)])][i % 3]
            for i in range(rows)
        ],
        crs='EPSG:4326',
    )


@unittest.skipIf(gpd is None, 'geopandas is not installed (mage-ml[geo])')
@patch('mage_ai.data.models.manager.DataManager.readable', return_value=False)
@patch('mage_ai.data.models.manager.DataManager.writeable', return_value=False)
class GeoVariableTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.root = create_pipeline(self._testMethodName, self.repo_path).dir_path

    def test_a_geodataframe_round_trips(self, *_):
        data = frame()
        self.assertEqual(infer_variable_type(data)[0], VariableType.GEO_DATAFRAME)

        Variable('geo', self.root, 'block').write_data(data)
        read = Variable('geo', self.root, 'block').read_data()

        self.assertIsInstance(read, gpd.GeoDataFrame)
        self.assertEqual(read.crs, data.crs)
        self.assertEqual(list(read.columns), list(data.columns))
        pd.testing.assert_frame_equal(
            pd.DataFrame(read.drop(columns='geometry')),
            pd.DataFrame(data.drop(columns='geometry')),
        )
        self.assertTrue(read.geometry.geom_equals(data.geometry).all())

    def test_a_sample_reads_the_first_rows(self, *_):
        Variable('geo', self.root, 'block').write_data(frame(rows=20))

        read = Variable('geo', self.root, 'block').read_data(sample=True, sample_count=5)

        self.assertIsInstance(read, gpd.GeoDataFrame)
        self.assertEqual(read['name'].tolist(), [f'n{i}' for i in range(5)])


@unittest.skipIf(gpd is None, 'geopandas is not installed (mage-ml[geo])')
class GeoBlockTest(DBTestCase):
    def test_the_next_block_gets_a_geodataframe(self):
        pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        load = Block.create('geo_load', 'data_loader', self.repo_path, pipeline=pipeline)
        with open(load.file_path, 'w') as file:
            file.write(textwrap.dedent('''
                import geopandas as gpd
                from shapely.geometry import Point
                @data_loader
                def load(**kwargs):
                    return gpd.GeoDataFrame(
                        {'name': ['a', 'b']},
                        geometry=[Point(-84.1, 9.9), Point(-83.0, 10.0)], crs='EPSG:4326',
                    )
            '''))
        check = Block.create('geo_check', 'transformer', self.repo_path, pipeline=pipeline,
                             upstream_block_uuids=[load.uuid])
        with open(check.file_path, 'w') as file:
            file.write(textwrap.dedent('''
                import geopandas as gpd
                @transformer
                def check(frame, **kwargs):
                    assert isinstance(frame, gpd.GeoDataFrame), type(frame)
                    assert frame.crs.to_epsg() == 4326, frame.crs
                    return frame.to_crs(3857)
            '''))

        load.execute_sync()
        output = check.execute_sync(run_all_blocks=True)

        self.assertEqual(output['output'][0].crs.to_epsg(), 3857)

    def test_the_output_shows_as_a_table_with_wkt_geometry(self):
        """GeoDataFrame.to_json writes GeoJSON and took no orient: the output failed."""
        pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        load = Block.create('geo_show', 'data_loader', self.repo_path, pipeline=pipeline)
        with open(load.file_path, 'w') as file:
            file.write(textwrap.dedent('''
                import geopandas as gpd
                from shapely.geometry import Point
                @data_loader
                def load(**kwargs):
                    return gpd.GeoDataFrame(
                        {'name': ['a']}, geometry=[Point(-84.1, 9.9)], crs='EPSG:4326',
                    )
            '''))
        load.execute_sync(from_notebook=True)

        (output,) = load.get_outputs()

        self.assertEqual(output['type'], 'table')
        self.assertEqual(output['sample_data']['columns'], ['name', 'geometry'])
        self.assertEqual(output['sample_data']['rows'], [['a', 'POINT (-84.1 9.9)']])
