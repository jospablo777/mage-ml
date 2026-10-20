"""
Rust blocks on Redis (mage::redis), with the redis profile of the test project: a Rust
exporter writes a Python block's features as one hash per row with a TTL, and a Rust
loader reads them back by key with their types; writing again replaces each hash whole.
"""
import datetime as dt
import os
import uuid

import pytest
import redis as redis_client

from integration_tests import rust_features
from integration_tests.mage_runner import execute_pipeline
from integration_tests.postgres.test_sql_block_matrix import build, error_of, output

PROFILE = dict(data_provider_profile='redis')

FEATURES = '''
import pandas as pd


@data_loader
def load(*args, **kwargs):
    return pd.DataFrame({
        'customer_id': pd.array([1, 2, 3], dtype='int64'),
        'orders_7d': pd.array([3, None, 2**62], dtype='Int64'),
        'spend_7d': [120.5, 0.1, None],
        'segment': pd.array(['vip', None, 'é'], dtype='string'),
        'last_seen': pd.to_datetime(
            ['2024-06-01 10:00:00.123456', None, '2024-01-02 03:04:05'], format='ISO8601',
        ),
    })
'''

EXPORT = '''
use mage::prelude::*;
use mage::redis::Redis;
use std::time::Duration;

fn export_data(features: DataFrame, vars: Vars) -> Result<()> {
    let prefix: String = vars.get("prefix")?;
    let written = Redis::connect()?.write_hashes(
        &features, &prefix, &["customer_id"], Some(Duration::from_secs(3600)),
    )?;
    println!("wrote {written} hashes");
    Ok(())
}
'''

LOAD = '''
use mage::prelude::*;
use mage::redis::Redis;

fn load_data(vars: Vars) -> Result<DataFrame> {
    let prefix: String = vars.get("prefix")?;
    let customers = df!("customer_id" => [1i64, 2, 3, 99])?;
    Redis::connect()?.read_hashes(&customers, &prefix, &[
        ("orders_7d", DataType::Int64),
        ("spend_7d", DataType::Float64),
        ("segment", DataType::String),
        ("last_seen", DataType::Datetime(TimeUnit::Microseconds, None)),
    ])
}
'''

PASS_ON = '''
@transformer
def keep(rows, *args, **kwargs):
    return rows
'''


@pytest.fixture
def redis_url():
    url = os.getenv('MAGE_TEST_REDIS_URL')
    if not url:
        pytest.skip('Redis is not configured: MAGE_TEST_REDIS_URL unset')
    return url


@pytest.fixture
def redis_workspace(mage_project, redis_url):
    with rust_features.enabled(mage_project, 'redis') as rust:
        yield rust


@pytest.mark.timeout(1800)
def test_features_round_trip_through_redis_hashes(mage_project, redis_workspace, redis_url):
    prefix = f'it_{uuid.uuid4().hex[:8]}'
    client = redis_client.Redis.from_url(redis_url)
    writer = build(mage_project, [
        dict(uuid='features', type='data_loader', code=FEATURES),
        dict(uuid='write', type='data_exporter', language='rust', code=EXPORT,
             upstream=['features'], config=PROFILE),
    ])
    try:
        run = execute_pipeline(writer, prefix=prefix)
        assert run.status.value == 'completed', error_of(run)

        assert client.hgetall(f'{prefix}:1') == {
            b'orders_7d': b'3', b'spend_7d': b'120.5', b'segment': b'vip',
            b'last_seen': b'2024-06-01T10:00:00.123456',
        }
        # Nulls are not stored.
        assert client.hgetall(f'{prefix}:2') == {b'spend_7d': b'0.1'}
        assert client.hget(f'{prefix}:3', 'orders_7d') == str(2**62).encode()
        assert 0 < client.ttl(f'{prefix}:1') <= 3600

        reader = build(mage_project, [
            dict(uuid='read', type='data_loader', language='rust', code=LOAD, config=PROFILE),
            dict(uuid='keep', type='transformer', code=PASS_ON, upstream=['read']),
        ])
        run = execute_pipeline(reader, prefix=prefix)
        assert run.status.value == 'completed', error_of(run)
        back = output(reader, run, 'keep').to_dicts()
        assert [r['orders_7d'] for r in back] == [3, None, 2**62, None]
        assert [r['spend_7d'] for r in back] == [120.5, 0.1, None, None]
        assert [r['segment'] for r in back] == ['vip', None, 'é', None]
        assert back[0]['last_seen'] == dt.datetime(2024, 6, 1, 10, 0, 0, 123456)

        # A stale field from an earlier write does not survive the next one.
        client.hset(f'{prefix}:1', 'stale', 'x')
        run = execute_pipeline(writer, prefix=prefix)
        assert run.status.value == 'completed', error_of(run)
        assert client.hget(f'{prefix}:1', 'stale') is None
    finally:
        for key in client.scan_iter(f'{prefix}:*'):
            client.delete(key)
