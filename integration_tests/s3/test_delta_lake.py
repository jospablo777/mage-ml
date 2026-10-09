"""
The Delta Lake S3 destination of mage_integrations, against MinIO.

It failed to import since deltalake 0.20 (its vendored writer imported names deltalake
removed), and building the table URI raised TypeError before that. It now writes with
deltalake's write_deltalake.
"""
import pytest

SCHEMA = {'properties': {
    'id': {'type': ['integer']},
    'n': {'type': ['null', 'integer']},
    'f': {'type': ['null', 'number']},
    'text': {'type': ['null', 'string']},
    'flag': {'type': ['null', 'boolean']},
    'day': {'type': ['string']},
    'tags': {'type': ['null', 'array']},
    'record': {'type': ['null', 'object']},
}}


def rows(ids, day='2024-01-01', **values):
    return [
        {'record': dict(
            {'id': i, 'n': 2**53 + i, 'f': i / 2, 'text': f't{i}', 'flag': i % 2 == 0,
             'day': day, 'tags': [i], 'record': {'a': i}},
            **values,
        )}
        for i in ids
    ]


@pytest.fixture
def delta(connector_config):
    from mage_integrations.destinations.delta_lake_s3 import DeltaLakeS3

    def build(mode='append', partition_keys=None, schema=SCHEMA):
        target = DeltaLakeS3(
            config=dict(connector_config, object_key_path='lake', table='orders', mode=mode),
            batch_processing=True,
        )
        target.schemas = {'orders': schema}
        target.partition_keys = {'orders': partition_keys or []}
        return target

    return build


def read(target):
    import deltalake

    table = deltalake.DeltaTable(
        target.build_table_uri('orders'), storage_options=target.build_storage_options(),
    )
    return table.schema().to_pyarrow(), sorted(
        table.to_pyarrow_table().to_pylist(), key=lambda r: r['id'],
    )


def test_types_and_nulls_stay_the_same_across_batches(delta):
    """
    Columns holding a null were turned into text with '' for null, and integers with a
    null into floats. The table schema was overwritten with each batch.
    """
    target = delta()
    target.export_batch_data(rows([1, 2]), 'orders')
    target.export_batch_data(rows([3], n=None, f=None, text=None, flag=None, tags=None,
                                  record=None), 'orders')

    schema, records = read(target)

    assert {f.name: str(f.type) for f in schema} == {
        'id': 'int64', 'n': 'int64', 'f': 'double', 'text': 'string', 'flag': 'bool',
        'day': 'string', 'tags': 'string', 'record': 'string',
    }
    assert records == [
        {'id': 1, 'n': 2**53 + 1, 'f': 0.5, 'text': 't1', 'flag': False,
         'day': '2024-01-01', 'tags': '[1]', 'record': '{"a": 1}'},
        {'id': 2, 'n': 2**53 + 2, 'f': 1.0, 'text': 't2', 'flag': True,
         'day': '2024-01-01', 'tags': '[2]', 'record': '{"a": 2}'},
        {'id': 3, 'n': None, 'f': None, 'text': None, 'flag': None,
         'day': '2024-01-01', 'tags': None, 'record': None},
    ]


def test_overwrite_keeps_every_batch_of_a_sync(delta):
    """Each batch replaced the table, so only the last batch of a sync was kept."""
    delta(mode='append').export_batch_data(rows([100]), 'orders')

    target = delta(mode='overwrite')
    target.export_batch_data(rows([1, 2]), 'orders')
    target.export_batch_data(rows([3]), 'orders')

    assert [r['id'] for r in read(target)[1]] == [1, 2, 3]

    # The next sync replaces the table again.
    delta(mode='overwrite').export_batch_data(rows([9]), 'orders')
    assert [r['id'] for r in read(target)[1]] == [9]


def test_partitioned_overwrite_replaces_only_the_partitions_written(delta):
    first = delta(mode='overwrite', partition_keys=['day'])
    first.export_batch_data(rows([1, 2], day='2024-01-01') + rows([3], day='2024-01-02'),
                            'orders')

    second = delta(mode='overwrite', partition_keys=['day'])
    second.export_batch_data(rows([10], day='2024-01-02'), 'orders')
    # A later batch of the same sync adds to the partition it already replaced.
    second.export_batch_data(rows([11], day='2024-01-02') + rows([12], day="it's"),
                             'orders')

    assert [(r['id'], r['day']) for r in read(second)[1]] == [
        (1, '2024-01-01'), (2, '2024-01-01'), (10, '2024-01-02'), (11, '2024-01-02'),
        (12, "it's"),
    ]


def test_new_columns_are_added(delta):
    delta().export_batch_data(rows([1]), 'orders')
    wider = {'properties': dict(SCHEMA['properties'], extra={'type': ['null', 'integer']})}

    target = delta(schema=wider)
    target.export_batch_data(rows([2], extra=5), 'orders')

    schema, records = read(target)
    assert 'extra' in schema.names
    assert [(r['id'], r['extra']) for r in records] == [(1, None), (2, 5)]


def test_cleanup_without_a_log_leaves_other_tables(delta, s3, bucket):
    """
    Objects under a table path without a Delta log are removed before the first write.
    The prefix had no trailing slash, so orders_archive was removed with orders.
    """
    s3.put_object(Bucket=bucket, Key='lake/orders/stale.parquet', Body=b'x')
    s3.put_object(Bucket=bucket, Key='lake/orders_archive/keep.parquet', Body=b'x')

    delta().export_batch_data(rows([1]), 'orders')

    keys = [o['Key'] for o in s3.list_objects_v2(Bucket=bucket)['Contents']]
    assert 'lake/orders_archive/keep.parquet' in keys
    assert 'lake/orders/stale.parquet' not in keys
