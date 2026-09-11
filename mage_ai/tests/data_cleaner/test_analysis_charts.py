"""
Dataframe profiling for block outputs.

Every dataframe a block returns is profiled through data_cleaner.clean, and the
result feeds the statistics and charts shown next to the block. A failure here is
swallowed by the caller, so the profile silently disappears instead of raising.

pandas 3 broke three things in this path: corr rejects text columns, Series.view is
gone, and a set is no longer accepted as a column indexer.
"""
import pandas as pd

from mage_ai.data_cleaner.analysis import charts
from mage_ai.data_cleaner.column_types.constants import ColumnType
from mage_ai.data_cleaner.data_cleaner import clean as clean_data
from mage_ai.tests.base_test import TestCase


def feature_frame(rows: int = 6) -> pd.DataFrame:
    return pd.DataFrame({
        'case_id': pd.Series([345038 + i for i in range(rows)], dtype='int64'),
        'opened_at': pd.to_datetime(
            ['2026-0%d-01 00:00:00+00:00' % ((i % 9) + 1) for i in range(rows)],
            utc=True,
        ),
        'flag': pd.Series([True, False] * (rows // 2), dtype='bool'),
        'total_order': pd.Series([0, 1] * (rows // 2), dtype='int64'),
        'country': pd.Series(['CR', 'US', 'MX', 'BR', 'AR', 'CL'][:rows]),
    })


class CorrelationTests(TestCase):
    def test_text_columns_are_skipped_instead_of_raising(self):
        result = charts.build_correlation_data(feature_frame())

        self.assertNotIn('country', result)
        self.assertIn('case_id', result)

    def test_correlation_is_reported_between_numeric_columns(self):
        df = pd.DataFrame({
            'a': pd.Series([1.0, 2.0, 3.0, 4.0]),
            'b': pd.Series([2.0, 4.0, 6.0, 8.0]),
            'label': pd.Series(['w', 'x', 'y', 'z']),
        })

        result = charts.build_correlation_data(df)

        self.assertEqual(result['a'][0]['x'], [dict(label='b')])
        self.assertAlmostEqual(result['a'][0]['y'][0]['value'], 1.0)


class HistogramTests(TestCase):
    def test_empty_column_returns_nothing(self):
        self.assertIsNone(
            charts.build_histogram_data('case_id', pd.Series([], dtype='int64'),
                                        ColumnType.NUMBER)
        )

    def test_all_null_column_returns_nothing(self):
        series = pd.Series([None, None], dtype='float64')

        self.assertIsNone(charts.build_histogram_data('score', series, ColumnType.NUMBER))

    def test_values_are_bucketed(self):
        series = pd.Series(range(100), dtype='int64')

        result = charts.build_histogram_data('n', series, ColumnType.NUMBER)

        self.assertEqual(sum(bucket['value'] for bucket in result['y']), 100)


class TimeSeriesTests(TestCase):
    def test_buckets_use_epoch_seconds_at_any_resolution(self):
        features = [dict(uuid='case_id', column_type=ColumnType.NUMBER)]

        for unit in ('s', 'us', 'ns'):
            with self.subTest(unit=unit):
                df = feature_frame()
                df['opened_at'] = df['opened_at'].dt.as_unit(unit)

                result = charts.build_time_series_data(df, features, 'opened_at')

                # 2026-01-01T00:00:00Z. A nanosecond assumption gave 1.7 billion times
                # too small a number for microsecond columns.
                self.assertEqual(result['case_id']['x'][0]['min'], 1767225600.0)

    def test_overview_accepts_categorical_columns(self):
        """The eligible columns were collected into a set and used as an indexer."""
        overview = charts.build_overview_data(
            feature_frame(),
            [dict(uuid='opened_at', column_type=ColumnType.DATETIME)],
            ['case_id', 'total_order'],
        )

        self.assertEqual(len(overview['time_series']), 1)
        self.assertIn('case_id', overview['scatter_plot'])


class BlockOutputProfileTests(TestCase):
    def profile(self, df):
        return clean_data(df, df_original=df, transform=False, verbose=False)

    def test_feature_frame_is_profiled(self):
        analysis = self.profile(feature_frame())

        self.assertEqual(analysis['statistics']['original_row_count'], 6)
        self.assertEqual(analysis['column_types']['case_id'], ColumnType.NUMBER)
        self.assertEqual(analysis['column_types']['opened_at'], ColumnType.DATETIME)
        self.assertEqual(analysis['column_types']['country'], ColumnType.TEXT)

    def test_prediction_frame_is_profiled(self):
        df = pd.DataFrame({
            'case_id': pd.Series(range(1, 7), dtype='int64'),
            'model_id': pd.Series(['family-codename'] * 6),
            'prediction': pd.Series([0.1, 0.25, 0.4, 0.6, 0.75, 0.9], dtype='float64'),
            'predicted_at': pd.Series([pd.Timestamp('2026-01-01', tz='UTC')] * 6),
        })

        analysis = self.profile(df)

        self.assertEqual(analysis['statistics']['original_row_count'], 6)
        self.assertEqual(
            analysis['column_types']['prediction'], ColumnType.NUMBER_WITH_DECIMALS,
        )

    def test_empty_frame_is_profiled(self):
        analysis = self.profile(feature_frame().iloc[0:0])

        self.assertEqual(analysis['statistics']['original_row_count'], 0)

    def test_naive_datetime_frame_is_profiled(self):
        df = pd.DataFrame({
            'n': pd.Series(range(6), dtype='int64'),
            'when': pd.to_datetime(['2026-01-0%d' % (i + 1) for i in range(6)]),
        })

        self.assertEqual(self.profile(df)['statistics']['original_row_count'], 6)

    def test_every_column_gets_a_type(self):
        analysis = self.profile(feature_frame())

        self.assertNotIn(None, analysis['column_types'].values())
