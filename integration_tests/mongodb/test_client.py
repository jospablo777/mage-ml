"""
Mage's MongoDB client (mage_ai.io.mongodb) against MongoDB 8.
"""
import datetime as dt
import decimal
import uuid

import numpy as np
import pandas as pd
import polars as pl
import pytest
from bson import Decimal128, Int64, ObjectId
from pymongo.errors import PyMongoError

UTC = dt.timezone.utc


def stored(mongo, collection='c'):
    return list(mongo[collection].find({}, {'_id': 0}).sort('id', 1))


def test_export_stores_every_value_type(mage_mongodb, mongo):
    """
    pymongo raised InvalidDocument for Decimal, date, timedelta, NumPy arrays and sets, and
    ValueError for UUID, so frames holding them failed to export.
    """
    key = uuid.UUID('12345678-1234-5678-1234-567812345678')
    frame = pd.DataFrame({
        'id': [1, 2],
        'big': pd.array([2**53 + 1, None], dtype='Int64'),
        'f': [0.5, np.nan],
        'text': pd.Series(['ñ "q"', None], dtype='str'),
        'flag': pd.array([True, None], dtype='boolean'),
        'amount': [decimal.Decimal('12345678901234567890.123456789'), None],
        'day': [dt.date(2024, 2, 29), None],
        'at': pd.to_datetime(['2024-01-01 12:00:00.123', None], format='ISO8601'),
        'zoned': pd.to_datetime(['2024-07-01 12:00', None], format='ISO8601')
        .tz_localize('America/New_York'),
        'span': pd.to_timedelta(['1.5 s', None]),
        'doc': [{'a': [1, None], 'b': {'c': 'd'}}, None],
        'array': [np.array([1, 2]), None],
        'tags': [{'b', 'a'}, None],
        'key': [key, None],
        'raw': [b'\x00\xff', None],
    })

    mage_mongodb.export(frame, 'c')

    first, second = stored(mongo)
    assert first == {
        'id': 1, 'big': 2**53 + 1, 'f': 0.5, 'text': 'ñ "q"', 'flag': True,
        'amount': Decimal128('12345678901234567890.123456789'),
        'day': dt.datetime(2024, 2, 29),
        'at': dt.datetime(2024, 1, 1, 12, 0, 0, 123000),
        'zoned': dt.datetime(2024, 7, 1, 16, 0),
        'span': 1.5,
        'doc': {'a': [1, None], 'b': {'c': 'd'}},
        'array': [1, 2],
        'tags': ['a', 'b'],
        'key': key,
        'raw': b'\x00\xff',
    }
    assert second == {name: None for name in first} | {'id': 2}


def test_bson_dates_hold_milliseconds(mage_mongodb, mongo):
    """BSON dates store milliseconds; MongoDB drops the microseconds of every client."""
    frame = pd.DataFrame({'id': [1], 'at': pd.to_datetime(['2024-01-01 00:00:00.123456'])})

    mage_mongodb.export(frame, 'c')

    assert stored(mongo)[0]['at'] == dt.datetime(2024, 1, 1, 0, 0, 0, 123000)


def test_polars_frames(mage_mongodb, mongo):
    frame = pl.DataFrame({
        'id': [1, 2],
        'big': [2**53 + 1, None],
        'day': [dt.date(2024, 1, 1), None],
        'ints': [[1, None], None],
        'amount': pl.Series([decimal.Decimal('1.10'), None], dtype=pl.Decimal(10, 2)),
        'record': [{'a': 1, 'b': 'x'}, None],
    })

    mage_mongodb.export(frame.lazy(), 'c')

    assert stored(mongo) == [
        {'id': 1, 'big': 2**53 + 1, 'day': dt.datetime(2024, 1, 1), 'ints': [1, None],
         'amount': Decimal128('1.10'), 'record': {'a': 1, 'b': 'x'}},
        {'id': 2, 'big': None, 'day': None, 'ints': None, 'amount': None, 'record': None},
    ]


def test_an_empty_frame_writes_nothing(mage_mongodb, mongo):
    """insert_many raised TypeError for an empty frame."""
    mage_mongodb.export(pd.DataFrame({'a': pd.Series([], dtype='int64')}), 'c')
    mage_mongodb.export([], 'c')

    assert stored(mongo) == []


def test_lists_of_dicts(mage_mongodb, mongo):
    mage_mongodb.export([{'id': 1, 'amount': decimal.Decimal('2.5')}, {'id': 2}], 'c')

    assert stored(mongo) == [{'id': 1, 'amount': Decimal128('2.5')}, {'id': 2, 'amount': None}]


def test_upsert_on_unique_fields(mage_mongodb, mongo):
    """Rerunning an export inserted every document again."""
    first = pd.DataFrame({'id': [1, 2], 'region': ['eu', 'eu'], 'v': ['a', 'b']})
    second = pd.DataFrame({'id': [2, 2, 3], 'region': ['eu', 'us', 'eu'], 'v': ['B', 'x', 'c']})
    options = dict(unique_constraints=['id', 'region'], unique_conflict_method='UPDATE')

    mage_mongodb.export(first, 'c', **options)
    mage_mongodb.export(second, 'c', **options)
    mage_mongodb.export(second, 'c', **options)

    rows = sorted((d['id'], d['region'], d['v']) for d in stored(mongo))
    assert rows == [(1, 'eu', 'a'), (2, 'eu', 'B'), (2, 'us', 'x'), (3, 'eu', 'c')]


def test_ignore_keeps_stored_documents(mage_mongodb, mongo):
    options = dict(unique_constraints=['id'], unique_conflict_method='IGNORE')

    mage_mongodb.export(pd.DataFrame({'id': [1], 'v': ['first']}), 'c', **options)
    mage_mongodb.export(pd.DataFrame({'id': [1, 2], 'v': ['second', 'new']}), 'c', **options)

    assert [(d['id'], d['v']) for d in stored(mongo)] == [(1, 'first'), (2, 'new')]


def test_unique_fields_must_have_values(mage_mongodb, mongo):
    frame = pd.DataFrame({'id': pd.array([1, None], dtype='Int64'), 'v': ['a', 'b']})

    with pytest.raises(ValueError, match='no value for a unique constraint'):
        mage_mongodb.export(frame, 'c', unique_constraints=['id'],
                            unique_conflict_method='UPDATE')
    with pytest.raises(ValueError, match='unique_conflict_method'):
        mage_mongodb.export(frame, 'c', unique_constraints=['id'])

    assert stored(mongo) == []


def test_replace_swaps_the_documents_and_keeps_indexes(mage_mongodb, mongo):
    mongo['c'].insert_many([{'id': i} for i in range(5)])
    mongo['c'].create_index('id', name='by_id', unique=True)

    mage_mongodb.export(pd.DataFrame({'id': [10, 11]}), 'c', if_exists='replace')

    assert [d['id'] for d in stored(mongo)] == [10, 11]
    assert mongo['c'].index_information()['by_id']['unique'] is True
    assert [n for n in mongo.list_collection_names() if 'staging' in n] == []


def test_a_failed_replace_leaves_the_collection(mage_mongodb, mongo):
    mongo['c'].insert_many([{'id': 1}, {'id': 2}])
    mongo['c'].create_index('id', name='by_id', unique=True)

    # Duplicate ids fail on the unique index of the staging collection.
    with pytest.raises(PyMongoError, match='duplicate key'):
        mage_mongodb.export(pd.DataFrame({'id': [3, 3]}), 'c', if_exists='replace')

    assert [d['id'] for d in stored(mongo)] == [1, 2]
    assert [n for n in mongo.list_collection_names() if 'staging' in n] == []


def test_exact_loads(mage_mongodb, mongo):
    """
    The default load builds a pandas frame from the documents: a field missing from a
    document, or null, turned integers into float, and 2**53 + 1 became 2**53.
    """
    object_id = ObjectId()
    mongo['c'].insert_many([
        {'_id': object_id, 'id': 1, 'n': Int64(2**53 + 1), 'amount': Decimal128('1.10'),
         'at': dt.datetime(2024, 1, 1, 12, 0, 0, 123000), 'doc': {'a': 1, 'b': 'x'},
         'ints': [1, 2], 'mixed': 1},
        {'id': 2, 'n': None, 'mixed': 'one'},
        {'id': 3},
    ])

    default = mage_mongodb.load('c')
    exact = mage_mongodb.load('c', exact_types=True)
    polars = mage_mongodb.load('c', polars=True)

    assert default['n'].dtype == 'float64'
    assert exact['n'].tolist() == [2**53 + 1, pd.NA, pd.NA]
    assert polars['n'].to_list() == [2**53 + 1, None, None]
    assert polars['_id'][0] == str(object_id)
    assert polars['amount'][0] == decimal.Decimal('1.10')
    assert polars['at'][0] == dt.datetime(2024, 1, 1, 12, 0, 0, 123000, tzinfo=UTC)
    assert polars['doc'][0] == {'a': 1, 'b': 'x'}
    assert polars['ints'].to_list() == [[1, 2], None, None]
    # A field with values of several types is JSON text.
    assert polars['mixed'].to_list() == ['1', '"one"', None]


def test_load_query_and_projection(mage_mongodb, mongo):
    mongo['c'].insert_many([{'id': i, 'v': i * 10} for i in range(5)])

    frame = mage_mongodb.load('c', query={'id': {'$gte': 3}}, projection={'_id': 0, 'v': 1},
                              polars=True)

    assert frame.to_dicts() == [{'v': 30}, {'v': 40}]


def test_round_trip(mage_mongodb, mongo):
    frame = pl.DataFrame({
        'id': [1, 2, 3],
        'big': [2**63 - 1, None, -(2**63)],
        'text': ['ñ', '', None],
        'at': pl.Series([dt.datetime(2024, 1, 1, 12, 0, 0, 123000, tzinfo=UTC), None,
                         dt.datetime(1969, 12, 31, 23, 59, 59, 999000, tzinfo=UTC)]),
        'amount': pl.Series([decimal.Decimal('1.10'), None, decimal.Decimal('-9.99')],
                            dtype=pl.Decimal(10, 2)),
    })

    mage_mongodb.export(frame, 'c')
    back = mage_mongodb.load('c', projection={'_id': 0}, polars=True).sort('id')

    assert back.select(frame.columns).cast(frame.schema).equals(frame)


def test_credentials_with_reserved_characters(mongodb_url, mongo):
    """The user and password went into the URI unescaped, so '@', ':' or '/' failed."""
    from pymongo import MongoClient

    from mage_ai.io.mongodb import MongoDB

    user, password = f'user@{mongo.name}', 'p:a/s@s%w?rd'
    admin = MongoClient(mongodb_url)
    admin[mongo.name].command('createUser', user, pwd=password, roles=['readWrite'])
    try:
        host_port = mongodb_url.split('//', 1)[1].split('/', 1)[0]
        host, port = host_port.split(':')
        client = MongoDB(
            host=host, port=int(port), user=user, password=password, database=mongo.name,
            verbose=False, client_options=dict(directConnection=True, authSource=mongo.name),
        )
        client.export(pd.DataFrame({'id': [1]}), 'c')
        client.client.close()
    finally:
        admin[mongo.name].command('dropUser', user)
        admin.close()

    assert stored(mongo) == [{'id': 1}]
