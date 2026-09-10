import asyncio
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from mage_ai.data.tabular.models import BatchSettings
from mage_ai.data.tabular.reader import (
    read_metadata,
    sample_batch_datasets,
    scan_batch_datasets_generator,
    scan_batch_datasets_generator_async,
    scan_dataset_parts,
)
from mage_ai.data_preparation.storage.local_storage import LocalStorage


class BatchReaderTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'data.parquet'
        self.table = pa.table({
            'id': list(range(20)),
            'label': ['keep' if i % 2 == 0 else 'other' for i in range(20)],
        })
        pq.write_table(self.table, self.path, row_group_size=4)
        self.settings = BatchSettings.load(items={'minimum': 4, 'maximum': 4})

    def ids(self, batches):
        return [value for batch in batches for value in batch.record_batch.column('id').to_pylist()]

    def test_limit_and_offset_across_row_groups(self):
        for scan in (False, True):
            with self.subTest(scan=scan):
                batches = scan_batch_datasets_generator(
                    str(self.path), limit=7, offset=3, settings=self.settings, scan=scan,
                )
                self.assertEqual(self.ids(batches), list(range(3, 10)))

    def test_zero_limit_and_offset_past_end(self):
        for options in ({'limit': 0}, {'offset': 25}):
            with self.subTest(options=options):
                self.assertEqual(self.ids(scan_batch_datasets_generator(
                    str(self.path), **options,
                )), [])

    def test_negative_bounds_are_rejected(self):
        for options in ({'limit': -1}, {'offset': -1}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                list(scan_batch_datasets_generator(str(self.path), **options))

    def test_filters_combine_multiple_conditions_and_groups(self):
        batches = scan_batch_datasets_generator(str(self.path), filters=[
            [('id', '>=', 3), ('id', '<', 12), ('label', '==', 'keep')],
            [('id', '==', 17)],
        ])
        self.assertEqual(self.ids(batches), [4, 6, 8, 10, 17])

    def test_arrow_expression_filter(self):
        batches = scan_batch_datasets_generator(
            str(self.path), filter=ds.field('id') >= 17,
        )
        self.assertEqual(self.ids(batches), [17, 18, 19])

    def test_string_filter(self):
        batches = scan_batch_datasets_generator(str(self.path), filters=[['id >= 17']])
        self.assertEqual(self.ids(batches), [17, 18, 19])

    def test_file_metadata_and_unlimited_parts(self):
        self.assertEqual(read_metadata(str(self.path))['num_rows'], 20)
        self.assertEqual(self.ids(scan_dataset_parts(str(self.path))), list(range(20)))

    def test_parts_include_all_sources(self):
        second = Path(self.directory.name) / 'second.parquet'
        pq.write_table(pa.table({'id': [20, 21], 'label': ['keep', 'other']}), second)
        self.assertEqual(
            self.ids(scan_dataset_parts([str(self.path), str(second)])), list(range(22)),
        )

    def test_limits_apply_after_filtering(self):
        batches = scan_batch_datasets_generator(
            str(self.path), filter=ds.field('label') == 'keep', offset=3, limit=4,
        )
        self.assertEqual(self.ids(batches), [6, 8, 10, 12])

    def test_sample_honors_count_without_mutating_settings(self):
        sample = sample_batch_datasets(
            str(self.path), sample_count=2, settings=self.settings,
        )
        self.assertEqual(sample.record_batch.column('id').to_pylist(), [0, 1])
        self.assertEqual(self.settings.items.maximum, 4)

    def test_async_reader_honors_bounds(self):
        async def read():
            batches = await scan_batch_datasets_generator_async(
                str(self.path), offset=3, limit=7, settings=self.settings,
            )
            return self.ids([batch async for batch in batches])

        self.assertEqual(asyncio.run(read()), list(range(3, 10)))

    def test_async_reader_keeps_dataset_io_off_event_loop(self):
        event_loop_thread = threading.get_ident()

        def read_batches(*args, **kwargs):
            self.assertNotEqual(threading.get_ident(), event_loop_thread)
            batches = scan_batch_datasets_generator(*args, **kwargs)

            def generate():
                self.assertNotEqual(threading.get_ident(), event_loop_thread)
                yield from batches

            return generate()

        async def read():
            batches = await scan_batch_datasets_generator_async(str(self.path), limit=2)
            return self.ids([batch async for batch in batches])

        with patch('mage_ai.data.tabular.reader.scan_batch_datasets_generator', read_batches):
            self.assertEqual(asyncio.run(read()), [0, 1])

    def test_local_pandas_read_applies_projection_and_filter(self):
        frame = LocalStorage().read_parquet(
            str(self.path), columns=['id'], filters=[('id', '>=', 17)],
        )
        self.assertEqual(frame.columns.tolist(), ['id'])
        self.assertEqual(frame['id'].tolist(), [17, 18, 19])

    def test_local_polars_read_applies_projection_and_row_limit(self):
        frame = LocalStorage().read_polars_parquet(str(self.path), columns=['id'], n_rows=2)
        self.assertEqual(frame.columns, ['id'])
        self.assertEqual(frame['id'].to_list(), [0, 1])
