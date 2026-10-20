"""
Rust blocks on DuckDB (mage::duckdb), with the duckdb profile of the test project: a Rust
exporter writes a Python block's frame to a database file, MageML's Python client and a
Rust loader read it back with the same values, and DuckDB types Parquet cannot carry come
to Rust as exact text.
"""
import datetime as dt
import decimal

import duckdb
import pytest

from integration_tests import rust_features
from integration_tests.mage_runner import execute_pipeline
from integration_tests.postgres.test_sql_block_matrix import build, error_of, output

PROFILE = dict(data_provider_profile='duckdb')

FRAME = '''
import pandas as pd


@data_loader
def load(*args, **kwargs):
    return pd.DataFrame({
        'id': pd.array([1, 2, 3], dtype='int64'),
        'count': pd.array([10, None, 2**62], dtype='Int64'),
        'amount': [0.5, None, 2.25],
        'label': pd.array(['a', None, 'é'], dtype='string'),
        'at': pd.to_datetime(
            ['2024-01-02 03:04:05.123456', '2024-06-01 00:00:00', None], format='ISO8601',
        ),
    })
'''

EXPORT = '''
use mage::duckdb::{DuckDb, WritePolicy};
use mage::prelude::*;

fn export_data(rows: DataFrame) -> Result<()> {
    let written = DuckDb::connect()?.write(&rows, "from_rust", WritePolicy::Replace)?;
    println!("wrote {written} rows");
    Ok(())
}
'''

LOAD = '''
use mage::duckdb::DuckDb;
use mage::prelude::*;

fn load_data(vars: Vars) -> Result<DataFrame> {
    let query: String = vars.get("query")?;
    DuckDb::connect()?.load(&query)
}
'''

PASS_ON = '''
@transformer
def keep(rows, *args, **kwargs):
    return rows
'''


@pytest.fixture
def duckdb_workspace(mage_project, duckdb_path, monkeypatch):
    """The Rust environment with mage::duckdb; the duckdb profile uses the test's file."""
    monkeypatch.setenv('MAGE_TEST_DUCKDB_DATABASE', duckdb_path)
    with rust_features.enabled(mage_project, 'duckdb') as rust:
        yield rust


@pytest.mark.timeout(1800)
def test_a_frame_round_trips_through_rust_blocks(mage_project, duckdb_workspace, duckdb_path):
    pipeline = build(mage_project, [
        dict(uuid='frame', type='data_loader', code=FRAME),
        dict(uuid='write', type='data_exporter', language='rust', code=EXPORT,
             upstream=['frame'], config=PROFILE),
    ])
    run = execute_pipeline(pipeline)
    assert run.status.value == 'completed', error_of(run)

    # The file Rust wrote, read by the DuckDB of MageML's Python environment.
    with duckdb.connect(duckdb_path, read_only=True) as connection:
        rows = connection.execute(
            'SELECT id, count, amount, label, "at" FROM main.from_rust ORDER BY id',
        ).fetchall()
    assert rows == [
        (1, 10, 0.5, 'a', dt.datetime(2024, 1, 2, 3, 4, 5, 123456)),
        (2, None, None, None, dt.datetime(2024, 6, 1)),
        (3, 2**62, 2.25, 'é', None),
    ]

    with duckdb.connect(duckdb_path) as connection:
        connection.execute(
            "CREATE TABLE exotic AS SELECT 1 AS id, "
            "170141183460469231731687303715884105727::HUGEINT AS huge, "
            "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID AS uid, "
            "123456789012345678.1234::DECIMAL(22, 4) AS amount",
        )
    reader = build(mage_project, [
        dict(uuid='read', type='data_loader', language='rust', code=LOAD, config=PROFILE),
        dict(uuid='keep', type='transformer', code=PASS_ON, upstream=['read']),
    ])
    run = execute_pipeline(reader, query='SELECT * FROM from_rust ORDER BY id')
    assert run.status.value == 'completed', error_of(run)
    back = output(reader, run, 'keep').to_dicts()
    assert [r['count'] for r in back] == [10, None, 2**62]
    assert [r['label'] for r in back] == ['a', None, 'é']

    run = execute_pipeline(reader, query='SELECT * FROM exotic')
    assert run.status.value == 'completed', error_of(run)
    exotic = output(reader, run, 'keep').to_dicts()[0]
    assert exotic['huge'] == '170141183460469231731687303715884105727'
    assert exotic['uid'] == 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
    assert exotic['amount'] == decimal.Decimal('123456789012345678.1234')
