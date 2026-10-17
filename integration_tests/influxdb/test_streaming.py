"""
MageML's InfluxDB stream sink against InfluxDB 2.7: a batch is written when batch_write
returns, and messages without a timestamp do not overwrite each other.
"""
import uuid

import pytest


@pytest.fixture
def measurement():
    return f'it_{uuid.uuid4().hex[:10]}'


def points(influxdb, measurement):
    from influxdb_client import InfluxDBClient

    with InfluxDBClient(url=influxdb['url'], token=influxdb['token'], org=influxdb['org']) as c:
        tables = c.query_api().query(
            f'from(bucket: "{influxdb["bucket"]}") |> range(start: -1h, stop: 1h) '
            f'|> filter(fn: (r) => r._measurement == "{measurement}") '
            '|> filter(fn: (r) => r._field == "n")'
        )
        return sorted(record.get_value() for table in tables for record in table.records)


def sink(influxdb, measurement, **extra):
    from mage_ai.streaming.sinks.influxdb import InfluxDbSink

    client = InfluxDbSink(dict(
        url=influxdb['url'], token=influxdb['token'], org=influxdb['org'],
        bucket=influxdb['bucket'], measurement=measurement, **extra,
    ))
    return client


def test_a_batch_is_written_when_batch_write_returns_and_none_overwrite(influxdb, measurement):
    """
    The writer batched in the background, so batch_write returned before writing (the
    source acked unwritten messages), and messages stamped in the same millisecond with the
    same tags overwrote each other.
    """
    writer = sink(influxdb, measurement)
    writer.batch_write([dict(n=i, city='Zürich') for i in range(5)])

    assert points(influxdb, measurement) == [0, 1, 2, 3, 4]


def test_a_rejected_batch_raises(influxdb, measurement):
    writer = sink(influxdb, measurement)
    writer.config.bucket = 'no_such_bucket'

    from influxdb_client.rest import ApiException

    with pytest.raises(ApiException, match='no_such_bucket'):
        writer.batch_write([dict(n=1)])


def test_the_documented_time_is_used(influxdb, measurement):
    """The docs say metadata['time']; only metadata['timestamp'] was read."""
    from influxdb_client import InfluxDBClient

    writer = sink(influxdb, measurement)
    writer.batch_write([dict(data=dict(n=1), metadata=dict(time=1_700_000_000_000))])

    with InfluxDBClient(url=influxdb['url'], token=influxdb['token'], org=influxdb['org']) as c:
        tables = c.query_api().query(
            f'from(bucket: "{influxdb["bucket"]}") |> range(start: 0) '
            f'|> filter(fn: (r) => r._measurement == "{measurement}")'
        )
        times = [record.get_time().timestamp() for t in tables for record in t.records]
    assert times == [1_700_000_000.0]
