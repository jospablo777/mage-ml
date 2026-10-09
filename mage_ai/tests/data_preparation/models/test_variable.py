import datetime
import decimal
import json
import math
import os
from unittest.mock import patch

import numpy as np
import pandas as pd
import polars as pl
from pandas.testing import assert_frame_equal
from polars.testing import assert_frame_equal as assert_polars_frame_equal

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.data_preparation.models.utils import infer_variable_type
from mage_ai.data_preparation.models.variable import Variable
from mage_ai.data_preparation.models.variables.constants import (
    VariableAggregateDataTypeFilename,
    VariableType,
)
from mage_ai.tests.base_test import DBTestCase
from mage_ai.tests.factory import create_pipeline
from mage_ai.tests.test_shared import (
    build_iterable,
    build_list_complex,
    build_matrix_sparse,
    build_pandas,
    build_pandas_series,
    build_polars,
    build_polars_series,
)


class VariableTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = create_pipeline(self.faker.unique.name(), self.repo_path)

    def test_write_and_read_data(self):
        pipeline = self.__create_pipeline('test pipeline 1')
        variable1 = Variable('var1', pipeline.dir_path, 'block1')
        variable2 = Variable('var2', pipeline.dir_path, 'block1')
        variable3 = Variable('var3', pipeline.dir_path, 'block2')
        variable4 = Variable('var4', pipeline.dir_path, 'block2')
        variable1.write_data('test')
        variable2.write_data(123)
        variable3.write_data([1, 2, 3, 4])
        variable4.write_data({'k1': 'v1', 'k2': 'v2'})
        self.assertEqual(variable1.read_data(), 'test')
        self.assertEqual(variable2.read_data(), 123)
        self.assertEqual(variable3.read_data(), [1, 2, 3, 4])
        self.assertEqual(variable4.read_data(), {'k1': 'v1', 'k2': 'v2'})

    def test_write_and_read_dataframe(self):
        with patch('mage_ai.data.models.manager.DataManager.writeable', return_value=False):
            with patch('mage_ai.data.models.manager.DataManager.readable', return_value=False):
                pipeline = self.__create_pipeline('test pipeline 2')
                variable1 = Variable(
                    'var1',
                    pipeline.dir_path,
                    'block1',
                    variable_type=VariableType.DATAFRAME,
                )
                variable2 = Variable('var2', pipeline.dir_path, 'block2')
                df1 = pd.DataFrame(
                    [
                        [1, 'test'],
                        [2, 'test2'],
                    ],
                    columns=['col1', 'col2'],
                )
                df2 = pd.DataFrame(
                    [
                        [1, 'test', 3.123, np.nan],
                        [2, 'test2', 4.321, np.nan],
                    ],
                    columns=['col1', 'col2', 'col3', 'col4'],
                )
                df2['col4'] = df2['col4'].astype('Int64')
                variable1.write_data(df1)
                variable2.write_data(df2)
                variable_dir_path = os.path.join(pipeline.dir_path, '.variables')
                self.assertTrue(
                    os.path.exists(
                        os.path.join(variable_dir_path, 'block1', 'var1', 'data.parquet'),
                    )
                )
                self.assertTrue(
                    os.path.exists(
                        os.path.join(variable_dir_path, 'block1', 'var1', 'sample_data.parquet'),
                    )
                )
                self.assertTrue(
                    os.path.exists(
                        os.path.join(variable_dir_path, 'block2', 'var2', 'data.parquet'),
                    )
                )
                self.assertTrue(
                    os.path.exists(
                        os.path.join(variable_dir_path, 'block2', 'var2', 'sample_data.parquet'),
                    )
                )
                assert_frame_equal(variable1.read_data(), df1)
                assert_frame_equal(variable1.read_data(sample=True, sample_count=1), df1.iloc[:1])
                assert_frame_equal(variable2.read_data(), df2)
                assert_frame_equal(variable2.read_data(sample=True, sample_count=1), df2.iloc[:1])

    def write_and_read(self, name: str, df: pd.DataFrame, **read_kwargs):
        with patch('mage_ai.data.models.manager.DataManager.writeable', return_value=False):
            with patch('mage_ai.data.models.manager.DataManager.readable', return_value=False):
                pipeline = self.__create_pipeline(f'test pipeline {name}')
                variable = Variable(name, pipeline.dir_path, 'block1')
                variable.write_data(df)
                return variable.read_data(**read_kwargs)

    def json_frame(self) -> pd.DataFrame:
        return pd.DataFrame({
            'ni': pd.array([1, None, 3], dtype='Int64'),
            'cat': pd.Categorical(['x', 'y', 'x']),
            'tz': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']).tz_localize('UTC'),
            'd': [{'a': 1}, {'a': 2}, {'a': 3}],
            'l': [[1, 2], [3], [4, 5]],
        })

    def test_dataframe_with_json_columns_keeps_other_dtypes(self):
        df = self.json_frame()

        back = self.write_and_read('json_dtypes', df)

        self.assertEqual(back['ni'].dtype, pd.Int64Dtype())
        self.assertIsInstance(back['cat'].dtype, pd.CategoricalDtype)
        self.assertEqual(str(back['tz'].dtype), str(df['tz'].dtype))
        self.assertEqual(back['d'].tolist(), df['d'].tolist())
        self.assertEqual(back['l'].tolist(), df['l'].tolist())

    def test_dataframe_with_json_columns_and_custom_index(self):
        # A non-range index skips the polars writer and serializes the JSON columns.
        df = pd.DataFrame(
            {'n': [1, 2], 'd': [{'a': 1}, None], 'l': [[1], [2, 3]]},
            index=[10, 20],
        )

        back = self.write_and_read('json_index', df)

        self.assertEqual(back.index.tolist(), [10, 20])
        self.assertEqual(back['d'].tolist()[0], {'a': 1})
        self.assertTrue(pd.isna(back['d'].tolist()[1]))
        self.assertEqual(back['l'].tolist(), [[1], [2, 3]])

    def test_write_data_leaves_the_input_frame_unchanged(self):
        df = pd.DataFrame({
            0: [1, 2],
            'mixed': pd.Series([1, 'a'], dtype=object),
            'd': [{'a': 1}, None],
        }, index=[5, 6])
        original = df.copy()

        self.write_and_read('unchanged', df)

        assert_frame_equal(df, original)
        self.assertEqual(df.columns.tolist(), [0, 'mixed', 'd'])

    def test_sample_read_skips_json_columns_cut_from_the_sample(self):
        df = pd.DataFrame({'a': [1, 2], 'b': [3, 4], 'd': [{'x': 1}, {'x': 2}]})

        with patch(
            'mage_ai.data_preparation.models.variable.DATAFRAME_SAMPLE_MAX_COLUMNS', 2,
        ):
            sample = self.write_and_read('sample_cut', df, sample=True)

        self.assertEqual(sample.columns.tolist(), ['a', 'b'])
        self.assertEqual(sample['a'].tolist(), [1, 2])

    def test_exact_values_survive_the_variable_round_trip(self):
        """
        Values an exact PostgreSQL load returns, which Parquet alone changes: decimals of
        mixed magnitude, bytes ending in zero bytes, times with an offset, NaN next to
        NULL in a float column, and decimals and dates inside lists.
        """
        offset = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        df = pd.DataFrame({
            'id': pd.array([1, 2, 3], dtype='Int64'),
            'dec': pd.Series([
                decimal.Decimal('123456789012345678901234567890123456789012345.6789'),
                decimal.Decimal('-1E-50'),
                None,
            ], dtype=object),
            'raw': pd.Series([b'\x00\xff\x10binary\x00', b'', None], dtype=object),
            'tz': pd.Series([datetime.time(12, 0, tzinfo=offset), None, None], dtype=object),
            'f': pd.Series([1.5, float('nan'), None], dtype=object),
            'decs': pd.Series([[decimal.Decimal('1.10'), None], [], None], dtype=object),
            'dates': pd.Series([[datetime.date(2000, 1, 1)], [], None], dtype=object),
            'doc': pd.Series([{'a': 1}, {'b': [1, None]}, None], dtype=object),
        })

        back = self.write_and_read('exact_values', df)

        self.assertEqual(back['dec'].tolist(), df['dec'].tolist())
        self.assertEqual(back['raw'].tolist(), df['raw'].tolist())
        self.assertEqual(back['tz'].tolist(), df['tz'].tolist())
        self.assertEqual(back['f'].tolist()[0], 1.5)
        self.assertTrue(math.isnan(back['f'].tolist()[1]))
        self.assertIsNone(back['f'].tolist()[2])
        self.assertEqual(back['decs'].tolist(), df['decs'].tolist())
        self.assertIsInstance(back['decs'].tolist()[0][0], decimal.Decimal)
        self.assertEqual(back['dates'].tolist(), df['dates'].tolist())
        self.assertEqual(back['doc'].tolist(), df['doc'].tolist())

    def test_write_and_read_dataframe_analysis(self):
        pipeline = self.__create_pipeline('test pipeline 3')
        variable = Variable(
            'var1',
            pipeline.dir_path,
            'block1',
            variable_type=VariableType.DATAFRAME_ANALYSIS,
        )
        data = dict(
            metadata=dict(
                column_types=dict(
                    col1='number',
                    col2='text',
                ),
            ),
            statistics=dict(
                count=100,
                count_distinct=50,
            ),
            insights=dict(),
            suggestions=[
                dict(
                    title='Remove outliers',
                )
            ],
        )
        variable.write_data(data)
        self.assertEqual(variable.read_data(), data)

    def test_write_and_read_json(self):
        pipeline = self.__create_pipeline('test pipeline 4')
        variable = Variable(
            'var1',
            pipeline.dir_path,
            'block1',
        )
        data = dict(
            results=[100] * 100,
        )
        variable.write_data(data)
        self.assertTrue(
            os.path.exists(
                os.path.join(
                    self.repo_path,
                    'pipelines/test_pipeline_4/.variables/block1/var1/data.json',
                )
            )
        )
        self.assertTrue(
            os.path.exists(
                os.path.join(
                    self.repo_path,
                    'pipelines/test_pipeline_4/.variables/block1/var1/sample_data.json',
                )
            )
        )
        self.assertEqual(variable.read_data(), data)
        self.assertEqual(
            variable.read_data(sample=True),
            dict(
                results=[100] * 20,
            ),
        )

    def test_write_and_read_polars_dataframe(self):
        pipeline = self.__create_pipeline('test pipeline polars')
        variable1 = Variable(
            'polars1',
            pipeline.dir_path,
            'block1',
        )
        variable2 = Variable('polars2', pipeline.dir_path, 'block2')
        df1 = pl.DataFrame(
            [
                [1, 'test'],
                [2, 'test2'],
            ],
            orient="row",
            schema=['col1', 'col2'],
        )
        df2 = pl.DataFrame(
            [
                [1, 'test', 3.123, 41414123123124],
                [2, 'test2', 4.321, 12111111],
            ],
            orient="row",
            schema=['col1', 'col2', 'col3', 'col4'],
        )
        df2 = df2.cast({'col4': pl.Int64})
        variable1.write_data(df1)
        variable2.write_data(df2)
        variable_dir_path = os.path.join(pipeline.dir_path, '.variables')
        self.assertTrue(
            os.path.exists(
                os.path.join(variable_dir_path, 'block1', 'polars1', 'data.parquet'),
            )
        )
        self.assertTrue(
            os.path.exists(
                os.path.join(variable_dir_path, 'block1', 'polars1', 'sample_data.parquet'),
            )
        )
        self.assertTrue(
            os.path.exists(
                os.path.join(variable_dir_path, 'block2', 'polars2', 'data.parquet'),
            )
        )
        self.assertTrue(
            os.path.exists(
                os.path.join(variable_dir_path, 'block2', 'polars2', 'sample_data.parquet'),
            )
        )
        assert_polars_frame_equal(variable1.read_data(), df1)
        assert_polars_frame_equal(variable1.read_data(sample=True, sample_count=1), df1.head(1))
        assert_polars_frame_equal(variable2.read_data(), df2)
        assert_polars_frame_equal(variable2.read_data(sample=True, sample_count=1), df2.head(1))

    def test_write_and_read_polars_dataframe_with_map_column(self):
        pipeline = self.__create_pipeline('test pipeline polars map')
        variable = Variable('polars_map', pipeline.dir_path, 'block1')
        df = pl.DataFrame(
            {'id': [1, 2], 'attrs': [{'a': 1}, {'b': 2, 'c': 3}]},
            schema={'id': pl.Int64, 'attrs': pl.Map(pl.String, pl.Int64)},
        )

        variable.write_data(df)

        back = variable.read_data()
        self.assertEqual(back.schema['attrs'], pl.Map(pl.String, pl.Int64))
        assert_polars_frame_equal(back, df)
        assert_polars_frame_equal(variable.read_data(sample=True, sample_count=1), df.head(1))

    def test_write_statistics_for_pandas_dataframe(self):
        with patch('mage_ai.data.models.manager.DataManager.writeable', return_value=False):
            data = build_pandas()
            variable = Variable(
                'var_pandas',
                self.pipeline.dir_path,
                'block_pandas',
                variable_type=infer_variable_type(data)[0],
            )
            variable.write_data(data)
            with open(
                os.path.join(variable.variable_path, VariableAggregateDataTypeFilename.STATISTICS),
                'r',
            ) as f:
                self.assertEqual(json.load(f)['original_row_count'], 1_000)

    def test_write_statistics_for_pandas_series(self):
        with patch('mage_ai.data.models.manager.DataManager.writeable', return_value=False):
            data = build_pandas_series()
            variable = Variable(
                'var_pandas_series',
                self.pipeline.dir_path,
                'block_pandas_series',
                variable_type=infer_variable_type(data)[0],
            )
            variable.write_data(data)
            with open(
                os.path.join(variable.variable_path, VariableAggregateDataTypeFilename.STATISTICS),
                'r',
            ) as f:
                self.assertEqual(json.load(f)['original_row_count'], 2_000)

    def test_write_statistics_for_polars_dataframe(self):
        with patch.multiple(
            'mage_ai.settings.server',
            MEMORY_MANAGER_PANDAS_V2=True,
            MEMORY_MANAGER_POLARS_V2=True,
            MEMORY_MANAGER_V2=True,
        ):
            with patch('mage_ai.data.models.manager.DataManager.writeable', return_value=True):
                data = build_polars()
                variable = Variable(
                    'var_polars',
                    self.pipeline.dir_path,
                    'block_polars',
                    variable_type=infer_variable_type(data)[0],
                )
                variable.write_data(data)
                with open(
                    os.path.join(
                        variable.variable_path, VariableAggregateDataTypeFilename.STATISTICS
                    ),
                    'r',
                ) as f:
                    self.assertEqual(json.load(f)['original_row_count'], 3_000)

    def test_write_statistics_for_polars_series(self):
        with patch('mage_ai.data.models.manager.DataManager.writeable', return_value=True):
            data = build_polars_series()
            variable = Variable(
                'var_polars_series',
                self.pipeline.dir_path,
                'block_polars_series',
                variable_type=infer_variable_type(data)[0],
            )
            variable.write_data(data)
            with open(
                os.path.join(variable.variable_path, VariableAggregateDataTypeFilename.STATISTICS),
                'r',
            ) as f:
                self.assertEqual(json.load(f)['original_row_count'], 4_000)

    def test_write_statistics_for_iterable(self):
        with patch('mage_ai.data.models.manager.DataManager.writeable', return_value=False):
            data = build_iterable()
            variable = Variable(
                'var_iterable',
                self.pipeline.dir_path,
                'block_iterable',
                variable_type=infer_variable_type(data)[0],
            )
            variable.write_data(data)
            with open(
                os.path.join(variable.variable_path, VariableAggregateDataTypeFilename.STATISTICS),
                'r',
            ) as f:
                self.assertEqual(json.load(f)['original_row_count'], 5_000)

    def test_write_statistics_for_sparse_matrix(self):
        with patch('mage_ai.data.models.manager.DataManager.writeable', return_value=False):
            data = build_matrix_sparse()
            variable = Variable(
                'var_sparse_matrix',
                self.pipeline.dir_path,
                'block_sparse_matrix',
                variable_type=infer_variable_type(data)[0],
            )
            variable.write_data(data)
            with open(
                os.path.join(variable.variable_path, VariableAggregateDataTypeFilename.STATISTICS),
                'r',
            ) as f:
                self.assertEqual(json.load(f)['original_row_count'], 6_000)

    def test_write_statistics_for_list_complex(self):
        with patch('mage_ai.data.models.manager.DataManager.writeable', return_value=False):
            data = build_list_complex()
            variable = Variable(
                'var_list_complex',
                self.pipeline.dir_path,
                'block_list_complex',
                variable_type=infer_variable_type(data)[0],
            )
            variable.write_data(data)
            # 100 for the data generator
            # 6 for the other objects
            row_count = 106
            with open(
                os.path.join(variable.variable_path, VariableAggregateDataTypeFilename.STATISTICS),
                'r',
            ) as f:
                self.assertEqual(json.load(f)['original_row_count'], row_count)

    def test_write_data_async_for_list_complex(self):
        """The async write of complex lists called a misspelled method and raised."""
        import asyncio

        with patch('mage_ai.data.models.manager.DataManager.writeable', return_value=False):
            data = build_list_complex()
            variable_type = infer_variable_type(data)[0]
            self.assertEqual(variable_type, VariableType.LIST_COMPLEX)
            written = {}
            for uuid, asynchronous in [('var_sync', False), ('var_async', True)]:
                variable = Variable(
                    uuid, self.pipeline.dir_path, 'block_async', variable_type=variable_type,
                )
                if asynchronous:
                    asyncio.run(variable.write_data_async(data))
                else:
                    variable.write_data(data)
                written[uuid] = sorted(os.listdir(variable.variable_path))

            # The same files as the sync write, which read back the same.
            self.assertEqual(written['var_async'], written['var_sync'])
            read = Variable('var_async', self.pipeline.dir_path, 'block_async').read_data()
            expected = Variable('var_sync', self.pipeline.dir_path, 'block_async').read_data()
            self.assertEqual(repr(read), repr(expected))
            # The sparse matrix in the list comes back sparse, with its values.
            self.assertEqual(type(read[-1]), type(data[-1]))
            self.assertEqual((read[-1] != data[-1]).nnz, 0)

    def test_sparse_matrix_in_a_list_written_as_a_dense_table(self):
        """Outputs written before matrices were stored as npz still read."""
        from mage_ai.data_preparation.models.utils import construct_value

        matrix = build_matrix_sparse()
        rows = pd.DataFrame(
            matrix.toarray(), columns=[str(i) for i in range(matrix.shape[1])],
        ).to_dict('records')

        value = construct_value(
            dict(name='csr_matrix', module='scipy.sparse._csr', variable_type='matrix_sparse'),
            rows,
        )

        self.assertEqual((value != matrix).nnz, 0)

    def __create_pipeline(self, name):
        pipeline = Pipeline.create(
            name,
            repo_path=self.repo_path,
        )
        block1 = Block.create('block1', 'data_loader', self.repo_path)
        block2 = Block.create('block2', 'transformer', self.repo_path)
        pipeline.add_block(block1)
        pipeline.add_block(block2)
        return pipeline
