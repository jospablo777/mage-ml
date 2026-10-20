"""
Rust blocks on ClickHouse (mage::clickhouse), with the clickhouse profile of the test
project: a Rust exporter writes a Python block's frame, a Rust loader reads the table back
and a Python block receives it with the same values, and the types ClickHouse cannot send
in Arrow read exactly.
"""
import datetime as dt
import decimal

import pytest

from integration_tests import rust_features
from integration_tests.mage_runner import execute_pipeline
from integration_tests.postgres.test_sql_block_matrix import build, error_of, output

PROFILE = dict(data_provider_profile='clickhouse')

FRAME = '''
import datetime as dt
import decimal

import pandas as pd


@data_loader
def load(*args, **kwargs):
    return pd.DataFrame({
        'id': pd.array([1, 2, 3], dtype='int64'),
        'count': pd.array([10, None, 2**62], dtype='Int64'),
        'amount': [0.5, None, 2.25],
        'label': pd.array(['a', None, 'é'], dtype='string'),
        'at': pd.to_datetime(
            ['2024-01-02 03:04:05.123456', '2024-06-01 00:00:00', None], utc=True,
            format='ISO8601',
        ),
        'day': [dt.date(2024, 1, 2), dt.date(1900, 1, 1), None],
    })
'''

EXPORT = '''
use mage::clickhouse::{ClickHouse, WritePolicy};
use mage::prelude::*;

fn export_data(rows: DataFrame) -> Result<()> {
    let written = ClickHouse::connect()?.write(&rows, "from_rust", WritePolicy::Replace)?;
    println!("wrote {written} rows");
    Ok(())
}
'''

LOAD = '''
use mage::clickhouse::ClickHouse;
use mage::prelude::*;

fn load_data(vars: Vars) -> Result<DataFrame> {
    let query: String = vars.get("query")?;
    ClickHouse::connect()?.query(&query)
}
'''

PASS_ON = '''
@transformer
def keep(rows, *args, **kwargs):
    return rows
'''


def rows_of(frame):
    """A Rust block's output reaches Python blocks as a Polars frame."""
    return frame.to_dicts()


@pytest.fixture
def clickhouse_workspace(mage_project):
    with rust_features.enabled(mage_project, 'clickhouse') as rust:
        yield rust


@pytest.mark.timeout(1800)
def test_a_frame_round_trips_through_rust_blocks(mage_project, clickhouse_workspace, ch):
    pipeline = build(mage_project, [
        dict(uuid='frame', type='data_loader', code=FRAME),
        dict(uuid='write', type='data_exporter', language='rust', code=EXPORT,
             upstream=['frame'], config=PROFILE),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)
    types = dict(ch.query(
        "SELECT name, type FROM system.columns WHERE database = currentDatabase() "
        "AND table = 'from_rust'",
    ).result_rows)
    assert types['count'] == 'Nullable(Int64)'
    assert types['at'].startswith("Nullable(DateTime64(") and "'UTC'" in types['at']
    assert types['day'] == 'Nullable(Date32)'
    rows = ch.query('SELECT id, count, label, day FROM from_rust ORDER BY id').result_rows
    assert rows == [
        (1, 10, 'a', dt.date(2024, 1, 2)),
        (2, None, None, dt.date(1900, 1, 1)),
        (3, 2**62, 'é', None),
    ]

    reader = build(mage_project, [
        dict(uuid='read', type='data_loader', language='rust', code=LOAD, config=PROFILE),
        dict(uuid='keep', type='transformer', code=PASS_ON, upstream=['read']),
    ])
    run = execute_pipeline(reader, query='SELECT * FROM from_rust ORDER BY id')
    assert run.status.value == 'completed', error_of(run)
    rows = rows_of(output(reader, run, 'keep'))
    assert [r['count'] for r in rows] == [10, None, 2**62]
    assert [r['label'] for r in rows] == ['a', None, 'é']
    assert rows[0]['at'] == dt.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=dt.timezone.utc)
    assert [r['day'] for r in rows] == [dt.date(2024, 1, 2), dt.date(1900, 1, 1), None]


def test_types_arrow_cannot_carry_read_exactly(mage_project, clickhouse_workspace, ch):
    ch.command(
        'CREATE TABLE odd (id UInt8, u UUID, ip IPv4, e Enum8(\'on\' = 1, \'off\' = 2), '
        'big Int128, wide Decimal(50, 3), d Date, t DateTime(\'Europe/Paris\')) '
        'ENGINE = MergeTree ORDER BY id',
    )
    ch.command(
        "INSERT INTO odd VALUES (1, '00000000-0000-0000-0000-00000000002a', '10.0.0.1', "
        "'off', -170141183460469231731687303715884105728, "
        "12345678901234567890123456789012345678901234567.125, '2024-02-29', "
        "'2024-01-02 03:04:05')",
    )
    reader = build(mage_project, [
        dict(uuid='read', type='data_loader', language='rust', code=LOAD, config=PROFILE),
        dict(uuid='keep', type='transformer', code=PASS_ON, upstream=['read']),
    ])
    run = execute_pipeline(reader, query='SELECT * FROM odd')
    assert run.status.value == 'completed', error_of(run)
    row = rows_of(output(reader, run, 'keep'))[0]
    assert row['u'] == '00000000-0000-0000-0000-00000000002a'
    assert row['ip'] == '10.0.0.1'
    assert row['e'] == 'off'
    assert row['big'] == '-170141183460469231731687303715884105728'
    assert decimal.Decimal(row['wide']) == decimal.Decimal(
        '12345678901234567890123456789012345678901234567.125',
    )
    assert row['d'] == dt.date(2024, 2, 29)
    # 03:04:05 in Paris is 02:04:05 UTC.
    assert row['t'].astimezone(dt.timezone.utc).strftime('%H:%M:%S') == '02:04:05'


def test_a_failing_query_says_why(mage_project, clickhouse_workspace, ch):
    reader = build(mage_project, [
        dict(uuid='read', type='data_loader', language='rust', code=LOAD, config=PROFILE),
    ])
    run = execute_pipeline(reader, query='SELECT * FROM no_such_table')
    assert run.status.value == 'failed'
    assert 'no_such_table' in error_of(run, 'read')
