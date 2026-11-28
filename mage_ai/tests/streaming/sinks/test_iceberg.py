import tempfile
from pathlib import Path

from mage_ai.streaming.sinks.iceberg import IcebergSink, partition_field, rejoined
from mage_ai.streaming.sinks.sink_factory import SinkFactory
from mage_ai.tests.base_test import TestCase


class IcebergSinkTest(TestCase):
    """Editor runs append each batch to the table with pyiceberg, on a SQL catalog here."""

    def setUp(self):
        super().setUp()
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        warehouse = Path(self.dir.name)
        self.catalog = dict(
            type='sql',
            uri=f'sqlite:///{warehouse / "catalog.db"}',
            warehouse=f'file://{warehouse}',
        )

    def sink(self, **settings):
        return SinkFactory.get_sink(dict(
            connector_type='iceberg', catalog=dict(self.catalog), **settings,
        ))

    def test_a_table_is_created_from_the_first_batch_and_appended_to(self):
        sink = self.sink(
            table='lake.events', create_table=True,
            columns=dict(ts='timestamptz'), partition_by=['day(ts)', 'region'],
        )
        self.assertIsInstance(sink, IcebergSink)
        sink.batch_write([
            dict(id=1, region='eu', ts='2026-11-27T10:00:00Z', tags=['a', 'b'], amount=2),
            dict(data=dict(id=2, region='us', ts='2026-11-28T09:00:00Z', amount=2.5),
                 metadata=dict(key='x')),
        ])
        sink.batch_write([dict(id=3, region='eu', unknown='left out')])

        table = sink.catalog.load_table('lake.events')
        self.assertEqual(
            [(f.name, str(f.field_type)) for f in table.schema().fields],
            [('id', 'long'), ('region', 'string'), ('ts', 'timestamptz'), ('tags', 'string'),
             ('amount', 'double')],
        )
        self.assertEqual(
            [f.name for f in table.spec().fields], ['ts_day', 'region'],
        )
        rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda r: r['id'])
        self.assertEqual(
            [(r['id'], r['region'], r['tags'], r['amount']) for r in rows],
            [(1, 'eu', '["a", "b"]', 2.0), (2, 'us', None, 2.5), (3, 'eu', None, None)],
        )
        self.assertEqual(rows[0]['ts'].isoformat(), '2026-11-27T10:00:00+00:00')
        self.assertEqual(len(table.snapshots()), 2)

    def test_a_missing_table_and_bad_settings_are_refused(self):
        sink = self.sink(table='lake.missing')
        with self.assertRaisesRegex(ValueError, 'does not exist'):
            sink.batch_write([dict(id=1)])
        with self.assertRaisesRegex(ValueError, 'with its namespace'):
            self.sink(table='events')
        with self.assertRaisesRegex(ValueError, 'is not a type'):
            self.sink(table='a.b', columns=dict(ts='datetime'))
        with self.assertRaisesRegex(ValueError, 'partition_by'):
            self.sink(table='a.b', partition_by=['week(ts)'])

    def test_text_columns_take_other_values_as_json_and_partitions_need_their_column(self):
        sink = self.sink(table='lake.notes', create_table=True, columns=dict(note='string'))
        sink.batch_write([dict(id=1, note=True), dict(id=2, note=2.5), dict(id=3, note='x')])
        rows = sorted(sink.catalog.load_table('lake.notes').scan().to_arrow().to_pylist(),
                      key=lambda r: r['id'])
        self.assertEqual([r['note'] for r in rows], ['true', '2.5', 'x'])

        sink = self.sink(table='lake.parts', create_table=True, partition_by=['day(ts)'])
        with self.assertRaisesRegex(ValueError, 'first messages have no ts; give its type'):
            sink.batch_write([dict(id=1)])

    def test_old_snapshots_leave_the_metadata_but_the_newest_stay(self):
        import time

        sink = self.sink(table='lake.expiring', create_table=True, expire_snapshots=dict(
            older_than='0s', keep=2, every='0s',
        ))
        for n in range(5):
            sink.batch_write([dict(id=n)])
            time.sleep(0.005)
        table = sink.catalog.load_table('lake.expiring')
        self.assertEqual(len(table.metadata.snapshots), 2)
        self.assertEqual(len(table.scan().to_arrow()), 5)

        for bad, message in [
            (dict(keep=2), 'older_than is required'),
            (dict(older_than='soon'), 'such as 1d'),
            (dict(older_than='1d', keep=0), '1 or more'),
            (dict(older_than='1d', keep_last=3), 'unknown setting keep_last'),
        ]:
            with self.assertRaisesRegex(ValueError, message):
                self.sink(table='lake.x', expire_snapshots=bad)

    def test_partition_fields_read_as_transforms_of_columns(self):
        self.assertEqual(partition_field('region'), ('identity', None, 'region'))
        self.assertEqual(partition_field('day(ts)'), ('day', None, 'ts'))
        self.assertEqual(partition_field('bucket(16, id)'), ('bucket', 16, 'id'))
        self.assertEqual(partition_field('truncate(4,name)'), ('truncate', 4, 'name'))
        # As YAML reads [day(ts), bucket(16, id)].
        self.assertEqual(
            rejoined(['day(ts)', 'bucket(16', 'id)', 'region']),
            ['day(ts)', 'bucket(16, id)', 'region'],
        )
