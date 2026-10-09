"""
The Amazon S3 source and destination of mage_integrations, the Singer connectors of
Mage's data integration pipelines, against MinIO.
"""
import datetime as dt
import decimal
import io

import polars as pl
import pytest

FRAME = pl.DataFrame({
    'n': [2**53 + 1, None, -(2**63)],
    'text': ['ñ "q"\n', '', None],
    'flag': [True, None, False],
    'day': [dt.date(2024, 2, 29), None, dt.date(1, 1, 1)],
    'at': pl.Series([dt.datetime(2024, 1, 1, 12, 0, 0, 123456, tzinfo=dt.timezone.utc), None,
                     dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)]),
    'dec': pl.Series([decimal.Decimal('1.10'), None, decimal.Decimal('-9.99')],
                     dtype=pl.Decimal(10, 2)),
    'ints': [[1, None], None, []],
    'record': [{'a': 1, 'b': 'x'}, None, {'a': None, 'b': None}],
    'vector': pl.Series([[0.5, 1.5], None, [2.5, None]], dtype=pl.Array(pl.Float64, 2)),
})
FLAT = ['n', 'text', 'flag']


def read_source(config):
    from mage_integrations.sources.amazon_s3 import AmazonS3

    source = AmazonS3(config=config)
    stream = source.discover().streams[0]
    types = {
        name: [t for t in prop.to_dict()['type'] if t != 'null'][0]
        for name, prop in stream.schema.properties.items()
    }
    records = [r for batch in source.load_data(stream) for r in batch]
    for record in records:
        record.pop('_s3_last_modified')
    return types, records


def test_source_reads_parquet_values_exactly(connector_config, s3, bucket):
    body = io.BytesIO()
    FRAME.write_parquet(body)
    s3.put_object(Bucket=bucket, Key='orders/part-0.parquet', Body=body.getvalue())

    types, records = read_source(dict(connector_config, prefix='orders/'))

    assert types == {
        'n': 'integer', 'text': 'string', 'flag': 'boolean', 'day': 'string',
        'at': 'string', 'dec': 'string', 'ints': 'array', 'record': 'object',
        'vector': 'array', '_s3_last_modified': 'string',
    }
    expected = FRAME.to_dicts()
    # The fixed-size list with a null used to fail the read; NaN is not JSON.
    assert records == expected


def test_source_reads_csv_values_exactly(connector_config, s3, bucket):
    """
    pandas read 2**53 + 1 as a float and -2**63 as missing; the source reads CSV with
    Polars.
    """
    s3.put_object(Bucket=bucket, Key='orders/part-0.csv',
                  Body=FRAME.select(FLAT).write_csv().encode())

    types, records = read_source(dict(connector_config, prefix='orders/'))

    assert {k: types[k] for k in FLAT} == {'n': 'integer', 'text': 'string', 'flag': 'boolean'}
    assert records == FRAME.select(FLAT).to_dicts()


def test_source_reads_every_matching_object(connector_config, s3, bucket):
    for part in range(3):
        s3.put_object(Bucket=bucket, Key=f'orders/part-{part}.csv',
                      Body=f'n\n{part}\n'.encode())
    s3.put_object(Bucket=bucket, Key='orders/notes.txt', Body=b'n\n99\n')
    s3.put_object(Bucket=bucket, Key='orders/empty.csv', Body=b'')

    _, records = read_source(dict(connector_config, prefix='orders/', search_pattern=r'\.csv$'))

    assert sorted(r['n'] for r in records) == [0, 1, 2]


def destination(connector_config, file_type, schema):
    from mage_integrations.destinations.amazon_s3 import AmazonS3

    target = AmazonS3(
        config=dict(connector_config, object_key_path='out', table='orders',
                    file_type=file_type),
        batch_processing=True,
    )
    target.schemas = {'orders': schema}
    return target


def written(s3, bucket, file_type):
    frames = []
    for item in s3.list_objects_v2(Bucket=bucket, Prefix='out/orders/').get('Contents', []):
        body = io.BytesIO(s3.get_object(Bucket=bucket, Key=item['Key'])['Body'].read())
        frame = pl.read_parquet(body) if file_type == 'parquet' else pl.read_csv(body)
        frames.append(frame.drop('_mage_created_at', '_mage_updated_at'))
    return frames


SCHEMA = {'properties': {
    'n': {'type': ['null', 'integer']},
    'text': {'type': ['null', 'string']},
    'record': {'type': ['null', 'object']},
}}
BATCHES = [
    [{'record': {'n': 2**53 + 1, 'text': 'a', 'record': {'a': 1}}},
     {'record': {'n': None, 'text': None, 'record': None}}],
    [{'record': {'n': -(2**63), 'text': 'ñ', 'record': {'a': None}}}],
]


@pytest.mark.parametrize('file_type', ['parquet', 'csv'])
def test_destination_keeps_every_batch(connector_config, s3, bucket, file_type):
    """
    Each batch is a file named after the time it was written. Names had one-second
    resolution, so batches written within the same second replaced each other.
    """
    target = destination(connector_config, file_type, SCHEMA)

    for batch in BATCHES:
        target.export_batch_data([dict(r, record=dict(r['record'])) for r in batch], 'orders')

    frames = written(s3, bucket, file_type)
    assert len(frames) == 2
    rows = sorted(pl.concat(frames, how='vertical').to_dicts(), key=lambda r: str(r['n']))
    if file_type == 'csv':
        # CSV files hold nested values as JSON text; they were Python reprs.
        records = ['{"a": 1}', None, '{"a": null}']
    else:
        records = [{'a': 1}, None, {'a': None}]
    expected = [
        {'n': -(2**63), 'text': 'ñ', 'record': records[2]},
        {'n': 2**53 + 1, 'text': 'a', 'record': records[0]},
        {'n': None, 'text': None, 'record': records[1]},
    ]
    assert rows == expected


def test_source_to_destination(connector_config, s3, bucket):
    """Records the source reads are written by the destination with the same values."""
    body = io.BytesIO()
    FRAME.select(FLAT + ['day', 'record']).write_parquet(body)
    s3.put_object(Bucket=bucket, Key='orders/part-0.parquet', Body=body.getvalue())
    types, records = read_source(dict(connector_config, prefix='orders/'))
    schema = {'properties': {name: {'type': ['null', t]} for name, t in types.items()}}

    target = destination(connector_config, 'parquet', schema)
    target.export_batch_data([{'record': dict(r)} for r in records], 'orders')

    [frame] = written(s3, bucket, 'parquet')
    assert frame.to_dicts() == records
