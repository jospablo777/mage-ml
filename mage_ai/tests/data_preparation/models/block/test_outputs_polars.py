import datetime as dt
import decimal
from unittest.mock import patch

import polars as pl

from mage_ai.data_preparation.models.block.outputs import format_output_data
from mage_ai.data_preparation.models.constants import DATAFRAME_SAMPLE_COUNT_PREVIEW
from mage_ai.tests.base_test import DBTestCase
from mage_ai.tests.factory import create_pipeline_with_blocks


class OutputPreviewTestCase(DBTestCase):
    def setUp(self):
        super().setUp()
        _, blocks = create_pipeline_with_blocks(
            self.faker.unique.name(), self.repo_path, return_blocks=True,
        )
        self.block = blocks[0]

    def preview(self, frame, **kwargs):
        with patch.object(type(self.block), 'get_analysis', return_value=None):
            output, _ = format_output_data(self.block, frame, 'output_0', **kwargs)
        return output


class PolarsOutputPreviewTest(OutputPreviewTestCase):
    """The preview of a Polars output that the notebook shows under a block."""

    def test_types_preview_as_json(self):
        frame = pl.DataFrame({
            'span': pl.Series([dt.timedelta(seconds=1.5), None]),
            'amount': pl.Series([decimal.Decimal('1.10'), None], dtype=pl.Decimal(10, 2)),
            'big': pl.Series([2**100, None], dtype=pl.Int128),
            'at': pl.Series([dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc), None]),
            'tags': [['a'], None],
            'raw': [b'\xff', None],
        })

        output = self.preview(frame)

        self.assertEqual(output['shape'], [2, 6])
        self.assertEqual(output['sample_data']['columns'], frame.columns)
        self.assertEqual(output['sample_data']['rows'], [
            ['PT1.5S', 1.10, 2**100, '2024-01-01 00:00:00.000000+00:00', ['a'], '\\xff'],
            [None, None, None, None, None, None],
        ])

    def test_dates_after_the_year_9999(self):
        frame = pl.DataFrame({'at': [dt.datetime(2024, 1, 1)]}).with_columns(
            pl.datetime(10000, 1, 1).alias('far'),
        )

        output = self.preview(frame)

        self.assertEqual(
            output['sample_data']['rows'],
            [['2024-01-01 00:00:00.000000', '+10000-01-01 00:00:00.000000']],
        )

    def test_the_preview_holds_a_sample(self):
        """Every row was converted to Python and JSON for the preview."""
        frame = pl.DataFrame({'a': range(100_000)})

        output = self.preview(frame)

        self.assertEqual(len(output['sample_data']['rows']), DATAFRAME_SAMPLE_COUNT_PREVIEW)
        self.assertEqual(output['shape'], [100_000, 1])

    def test_a_failed_analysis_does_not_fail_the_preview(self):
        frame = pl.DataFrame({'a': [1, 2]})

        with patch.object(type(self.block), 'get_analysis', side_effect=OSError('gone')):
            output, _ = format_output_data(self.block, frame, 'output_0')

        self.assertEqual(output['shape'], [2, 1])


class PandasOutputPreviewTest(OutputPreviewTestCase):
    def test_bytes_preview_as_hex(self):
        """to_json decoded bytes as UTF-8 and failed on other bytes."""
        import pandas as pd

        frame = pd.DataFrame({'raw': [b'\xff', None], 'n': [1, 2]})

        output = self.preview(frame)

        self.assertEqual(output['sample_data']['rows'], [['\\xff', 1], [None, 2]])
        self.assertEqual(frame['raw'][0], b'\xff')
