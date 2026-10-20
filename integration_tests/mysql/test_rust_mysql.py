"""
Rust blocks on MySQL (mage::mysql), with the mysql profile of the test project: a Rust
exporter writes a Python block's frame, and a Rust loader reads it back into a Python
block with the same values.
"""
import datetime as dt
import decimal
import uuid

import pytest

from integration_tests import rust_features
from integration_tests.mage_runner import execute_pipeline
from integration_tests.postgres.test_sql_block_matrix import build, error_of, output

PROFILE = dict(data_provider_profile='mysql')

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
            ['2024-01-02 03:04:05.123456', '2024-06-01 00:00:00', None], format='ISO8601',
        ),
    })
'''

EXPORT = '''
use mage::mysql::{MySql, WritePolicy};
use mage::prelude::*;

fn export_data(rows: DataFrame, vars: Vars) -> Result<()> {
    let table: String = vars.get("table")?;
    let written = MySql::connect()?.write(&rows, &table, WritePolicy::Replace)?;
    println!("wrote {written} rows");
    Ok(())
}
'''

LOAD = '''
use mage::mysql::MySql;
use mage::prelude::*;

fn load_data(vars: Vars) -> Result<DataFrame> {
    let query: String = vars.get("query")?;
    MySql::connect()?.query(&query)
}
'''

PASS_ON = '''
@transformer
def keep(rows, *args, **kwargs):
    return rows
'''


@pytest.fixture
def mysql_workspace(mage_project, mysql_database, monkeypatch):
    """The Rust environment with mage::mysql; the mysql profile uses the test's database."""
    monkeypatch.setenv('MAGE_TEST_MYSQL_DATABASE', mysql_database)
    with rust_features.enabled(mage_project, 'mysql') as rust:
        yield rust


def mysql_query(settings, sql):
    import os

    import mysql.connector

    connection = mysql.connector.connect(
        database=os.environ['MAGE_TEST_MYSQL_DATABASE'], **settings,
    )
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall() if cursor.with_rows else []
        connection.commit()
        return rows
    finally:
        connection.close()


@pytest.mark.timeout(1800)
def test_a_frame_round_trips_through_rust_blocks(
    mage_project, mysql_workspace, mysql_settings,
):
    table = f'from_rust_{uuid.uuid4().hex[:8]}'
    pipeline = build(mage_project, [
        dict(uuid='frame', type='data_loader', code=FRAME),
        dict(uuid='write', type='data_exporter', language='rust', code=EXPORT,
             upstream=['frame'], config=PROFILE),
    ])
    run = execute_pipeline(pipeline, table=table)
    assert run.status.value == 'completed', error_of(run)
    rows = mysql_query(mysql_settings, f'SELECT id, count, label, at FROM {table} ORDER BY id')
    assert rows == [
        (1, 10, 'a', dt.datetime(2024, 1, 2, 3, 4, 5, 123456)),
        (2, None, None, dt.datetime(2024, 6, 1)),
        (3, 2**62, 'é', None),
    ]

    reader = build(mage_project, [
        dict(uuid='read', type='data_loader', language='rust', code=LOAD, config=PROFILE),
        dict(uuid='keep', type='transformer', code=PASS_ON, upstream=['read']),
    ])
    run = execute_pipeline(reader, query=f'SELECT * FROM {table} ORDER BY id')
    assert run.status.value == 'completed', error_of(run)
    back = output(reader, run, 'keep').to_dicts()
    assert [r['count'] for r in back] == [10, None, 2**62]
    assert [r['label'] for r in back] == ['a', None, 'é']
    assert back[0]['at'] == dt.datetime(2024, 1, 2, 3, 4, 5, 123456)
    mysql_query(mysql_settings, f'DROP TABLE {table}')


def test_exact_decimals_and_a_failure_that_says_why(
    mage_project, mysql_workspace, mysql_settings,
):
    table = f'exact_{uuid.uuid4().hex[:8]}'
    mysql_query(mysql_settings, f'CREATE TABLE {table} (n DECIMAL(30, 10))')
    mysql_query(mysql_settings, f"INSERT INTO {table} VALUES ('12345678901234567890.0123456789')")
    reader = build(mage_project, [
        dict(uuid='read', type='data_loader', language='rust', code=LOAD, config=PROFILE),
        dict(uuid='keep', type='transformer', code=PASS_ON, upstream=['read']),
    ])
    run = execute_pipeline(reader, query=f'SELECT n FROM {table}')
    assert run.status.value == 'completed', error_of(run)
    value = output(reader, run, 'keep').to_dicts()[0]['n']
    assert value == decimal.Decimal('12345678901234567890.0123456789')

    run = execute_pipeline(reader, query='SELECT * FROM no_such_table')
    assert run.status.value == 'failed'
    assert 'no_such_table' in error_of(run, 'read')
    mysql_query(mysql_settings, f'DROP TABLE {table}')
