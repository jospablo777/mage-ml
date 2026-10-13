import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq
from typer.testing import CliRunner

from mage_ai.cli.main import app


class ExamplesTest(unittest.TestCase):
    def test_retail_writes_the_same_shop_into_the_project(self):
        with tempfile.TemporaryDirectory() as project:
            result = CliRunner().invoke(app, ['examples', 'retail', project])

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertIn('Wrote 2,000 customers and 30,300 order rows', result.output)
            folder = Path(project) / 'data' / 'shop'
            orders = pq.read_table(folder / 'orders.parquet')
            self.assertEqual(orders.num_rows, 30_300)
            self.assertEqual(
                orders.schema.names,
                ['order_id', 'customer_id', 'ordered_at', 'status', 'currency', 'amount',
                 'units'],
            )
            again = Path(project) / 'again'
            again.mkdir()
            CliRunner().invoke(app, ['examples', 'retail', str(again)])
            self.assertTrue(
                pq.read_table(again / 'data' / 'shop' / 'orders.parquet').equals(orders),
                'the data differs between two runs',
            )

    def test_a_missing_project_says_so(self):
        result = CliRunner().invoke(app, ['examples', 'retail', '/no/such/project'])

        self.assertEqual(result.exit_code, 1)
        self.assertIn('is not a folder', result.output)
