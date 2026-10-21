"""
Rust blocks on MongoDB (mage::mongodb), with the mongodb profile of the test project: a Rust
exporter writes a Python block's frame as documents, upserting by key on a rerun, and a Rust
loader reads them back into a Python block with their types.
"""
import datetime as dt
import decimal
import uuid

import pytest

from integration_tests import rust_features
from integration_tests.mage_runner import execute_pipeline
from integration_tests.postgres.test_sql_block_matrix import build, error_of, output

PROFILE = dict(data_provider_profile='mongodb')

FRAME = '''
import datetime as dt

import pandas as pd


@data_loader
def load(*args, **kwargs):
    return pd.DataFrame({
        'id': pd.array([1, 2, 3], dtype='int64'),
        'plays': pd.array([10, None, 2**40], dtype='Int64'),
        'name': pd.array(['a', None, 'é'], dtype='string'),
        'at': pd.to_datetime(['2024-01-02 03:04:05.123', '2024-06-01', None], format='ISO8601',
                             utc=True),
    })
'''

EXPORT = '''
use mage::mongodb::{MongoDb, OnConflict, WritePolicy};
use mage::prelude::*;

fn export_data(rows: DataFrame) -> Result<()> {
    let written = MongoDb::connect()?.write_unique(
        &rows, "listeners", WritePolicy::Append, &["id"], OnConflict::Update,
    )?;
    println!("wrote {written} documents");
    Ok(())
}
'''

LOAD = '''
use mage::mongodb::{doc, MongoDb};
use mage::prelude::*;

fn load_data() -> Result<DataFrame> {
    MongoDb::connect()?.load_fields("listeners", doc! {}, Some(doc! { "_id": 0 }))
}
'''

PASS_ON = '''
@transformer
def keep(rows, *args, **kwargs):
    return rows
'''


@pytest.fixture
def mongodb_workspace(mage_project, mongo, monkeypatch):
    """The Rust environment with mage::mongodb; the profile uses the test's database."""
    monkeypatch.setenv('MAGE_TEST_MONGODB_DATABASE', mongo.name)
    with rust_features.enabled(mage_project, 'mongodb') as rust:
        yield rust


@pytest.mark.timeout(1800)
def test_documents_round_trip_through_rust_blocks(mage_project, mongodb_workspace, mongo):
    writer = build(mage_project, [
        dict(uuid='frame', type='data_loader', code=FRAME),
        dict(uuid='write', type='data_exporter', language='rust', code=EXPORT,
             upstream=['frame'], config=PROFILE),
    ])
    for _ in range(2):
        run = execute_pipeline(writer)
        assert run.status.value == 'completed', error_of(run)
    # Upserted by id: the second run stored nothing twice.
    assert mongo.listeners.count_documents({}) == 3
    stored = mongo.listeners.find_one({'id': 3})
    assert stored['plays'] == 2**40 and stored['name'] == 'é'
    assert stored['at'] is None

    reader = build(mage_project, [
        dict(uuid='read', type='data_loader', language='rust', code=LOAD, config=PROFILE),
        dict(uuid='keep', type='transformer', code=PASS_ON, upstream=['read']),
    ])
    run = execute_pipeline(reader)
    assert run.status.value == 'completed', error_of(run)
    rows = sorted(output(reader, run, 'keep').to_dicts(), key=lambda r: r['id'])
    assert [r['plays'] for r in rows] == [10, None, 2**40]
    assert rows[0]['at'] == dt.datetime(2024, 1, 2, 3, 4, 5, 123000, tzinfo=dt.timezone.utc)
    assert [r['name'] for r in rows] == ['a', None, 'é']


def test_exact_decimals_read_from_documents(mage_project, mongodb_workspace, mongo):
    from bson.decimal128 import Decimal128

    mongo.listeners.insert_one({'id': 1, 'balance': Decimal128('12345678901234567890.0123')})
    reader = build(mage_project, [
        dict(uuid='read', type='data_loader', language='rust', code=LOAD, config=PROFILE),
        dict(uuid='keep', type='transformer', code=PASS_ON, upstream=['read']),
    ])
    run = execute_pipeline(reader)
    assert run.status.value == 'completed', error_of(run)
    value = output(reader, run, 'keep').to_dicts()[0]['balance']
    assert value == decimal.Decimal('12345678901234567890.0123')


@pytest.mark.timeout(1800)
def test_the_mongodb_templates_write_and_read_back_a_table(mage_project, mongodb_workspace):
    """The loader and exporter templates of the block menus, against MongoDB."""
    from integration_tests.rust_templates import template

    table = f'from_template_{uuid.uuid4().hex[:8]}'
    writer = build(mage_project, [
        dict(uuid='frame', type='data_loader', code=FRAME),
        dict(uuid='write', type='data_exporter', language='rust',
             code=template('data_exporters/rust/mongodb.rs'), upstream=['frame'],
             config=PROFILE),
    ])
    run = execute_pipeline(writer, collection=table)
    assert run.status.value == 'completed', error_of(run)
    reader = build(mage_project, [
        dict(uuid='read', type='data_loader', language='rust',
             code=template('data_loaders/rust/mongodb.rs'), config=PROFILE),
        dict(uuid='keep', type='transformer', code=PASS_ON, upstream=['read']),
    ])
    run = execute_pipeline(reader, collection=table)
    assert run.status.value == 'completed', error_of(run)
    rows = sorted(output(reader, run, 'keep').to_dicts(), key=lambda r: r['id'])
    assert [r['id'] for r in rows] == [1, 2, 3]
    assert [r['name'] for r in rows] == ['a', None, 'é']
