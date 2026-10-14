"""
The object storage sink as MageML runs it: one object per batch, Parquet that keeps integers
past 64 bits and Unicode, JSON lines as written, and a refusal where Parquet would change
a value.
"""
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from mage_ai.streaming.sinks.object_storage import ObjectStorageSink, parquet_table


class ObjectStorageSinkTest(unittest.TestCase):
    def sink(self, directory, **settings):
        return ObjectStorageSink(dict(url=f'file://{directory}/lake', **settings))

    def test_parquet_keeps_values_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            sink = self.sink(directory, date_partition_format='dt=%Y')
            sink.batch_write([
                dict(id=1, big=123456789012345678901234567890, city='Zürich 🚀'),
                dict(id=2, big=None, city=None),
            ])
            files = list(Path(directory, 'lake').rglob('*.parquet'))
            self.assertEqual(len(files), 1)
            self.assertTrue(files[0].parent.name.startswith('dt='))
            rows = pq.read_table(files[0]).to_pylist()
        self.assertEqual(rows[0]['big'], Decimal('123456789012345678901234567890'))
        self.assertEqual(rows[0]['city'], 'Zürich 🚀')
        self.assertIsNone(rows[1]['big'])

    def test_json_lines_are_written_as_given(self):
        with tempfile.TemporaryDirectory() as directory:
            sink = self.sink(directory, format='jsonl')
            sink.batch_write([dict(a=2**70), 'text'])
            text = next(Path(directory, 'lake').rglob('*.jsonl')).read_text()
        self.assertEqual(text.splitlines(), [json.dumps(dict(a=2**70)), 'text'])

    def test_what_parquet_would_change_is_refused(self):
        with self.assertRaisesRegex(ValueError, 'write jsonl'):
            parquet_table([dict(a=dict(b=2**70))])
        with self.assertRaisesRegex(ValueError, 'not a JSON object'):
            parquet_table(['text'])
        table = parquet_table([dict(n=int('9' * 45))])
        self.assertEqual(str(table.schema.field('n').type), 'string')
