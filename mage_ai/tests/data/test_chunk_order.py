import tempfile
import unittest

import polars as pl

from mage_ai.data.tabular.models import BatchSettings
from mage_ai.data.tabular.reader import scan_batch_datasets_generator
from mage_ai.data.tabular.writer import to_parquet_sync


class ChunkOrderTest(unittest.TestCase):
    def test_rows_of_more_than_ten_chunks_keep_their_order(self):
        """
        pyarrow lists mage_chunk=10 before mage_chunk=2, so the rows of the eleventh chunk
        and later came back in the wrong place.
        """
        directory = tempfile.mkdtemp()
        settings = BatchSettings()
        settings.items.maximum = 10
        to_parquet_sync(directory, df=pl.DataFrame({'x': list(range(125))}), settings=settings)

        values = []
        for batch in scan_batch_datasets_generator(directory):
            values.extend(getattr(batch, 'record_batch', batch).column('x').to_pylist())

        self.assertEqual(values, list(range(125)))
