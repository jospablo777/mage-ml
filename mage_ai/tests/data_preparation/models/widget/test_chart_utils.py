import pandas as pd
import polars as pl

from mage_ai.data_preparation.models.widget.constants import (
    VARIABLE_NAME_X,
    VARIABLE_NAME_Y,
)
from mage_ai.data_preparation.models.widget.utils import build_x_y
from mage_ai.tests.base_test import TestCase

METRICS = [
    dict(aggregation='count', column='category'),
    dict(aggregation='sum', column='amount'),
]


class BuildXYTest(TestCase):
    def frame(self):
        return pd.DataFrame({
            'category': ['b', 'a', 'b', 'a', 'b'],
            'amount': [1, 2, 3, 4, 5],
        })

    def test_metric_on_the_grouping_column(self):
        """GroupBy.apply drops the grouping columns in pandas 3, so count(category) raised."""
        data = build_x_y(self.frame(), ['category'], METRICS)

        self.assertEqual(data[VARIABLE_NAME_X], ['a', 'b'])
        self.assertEqual(data[VARIABLE_NAME_Y], [[2, 3], [6, 9]])

    def test_several_grouping_columns_give_tuples(self):
        df = self.frame().assign(region=['x', 'x', 'y', 'y', 'y'])

        data = build_x_y(df, ['category', 'region'], METRICS)

        self.assertEqual(data[VARIABLE_NAME_X], [('a', 'x'), ('a', 'y'), ('b', 'x'), ('b', 'y')])

    def test_polars_outputs(self):
        """The check for a groupby attribute skipped Polars frames, which have group_by."""
        expected = build_x_y(self.frame(), ['category'], METRICS)

        self.assertEqual(build_x_y(pl.from_pandas(self.frame()), ['category'], METRICS), expected)
        self.assertEqual(
            build_x_y(pl.from_pandas(self.frame()).lazy(), ['category'], METRICS),
            expected,
        )

    def test_values_that_are_not_frames(self):
        self.assertEqual(build_x_y({'a': 1}, ['a'], METRICS), {})
