"""
The MongoDB source and destination of mage_integrations against MongoDB 8, run as
programs the way Mage runs them: the source writes Singer messages, the destination reads
them.
"""
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

from bson import Decimal128, Int64

ROOT = Path(__file__).resolve().parents[2] / 'mage_integrations' / 'mage_integrations'
SOURCE = ROOT / 'sources' / 'mongodb' / '__init__.py'
DESTINATION = ROOT / 'destinations' / 'mongodb' / '__init__.py'


def host_and_port(mongodb_url):
    host_port = mongodb_url.split('//', 1)[1].split('/', 1)[0]
    host, port = host_port.split(':')
    return host, int(port)


def run(args, **kwargs):
    result = subprocess.run([sys.executable, *map(str, args)], capture_output=True, text=True,
                            timeout=120, **kwargs)
    assert result.returncode == 0, result.stderr[-3000:]
    return result.stdout


def discover(mongodb_url, database, tmp_path, **extra):
    host, port = host_and_port(mongodb_url)
    config = tmp_path / 'source.json'
    config.write_text(json.dumps(dict(
        host=host, port=port, database=database, direct_connection='true', **extra,
    )))
    catalog = json.loads(run([SOURCE, '--config', config, '--discover']))
    return config, catalog


def select(catalog, tmp_path, replication_method='FULL_TABLE', replication_key=None):
    for stream in catalog['streams']:
        stream['replication_method'] = replication_method
        for entry in stream['metadata']:
            entry['metadata']['selected'] = True
        if replication_key:
            stream['bookmark_properties'] = [replication_key]
    path = tmp_path / 'catalog.json'
    path.write_text(json.dumps(catalog))
    return path


def records(output):
    return [
        json.loads(line)['record'] for line in output.splitlines()
        if line.startswith('{') and json.loads(line).get('type') == 'RECORD'
    ]


def seed(mongo):
    mongo['orders'].insert_many([
        {'id': 1, 'big': Int64(2**53 + 1), 'f': 0.5, 'flag': True, 'text': 'ñ "q"',
         'amount': Decimal128('12.34'), 'at': dt.datetime(2024, 1, 1, 12, 0, 0, 123000),
         'doc': {'a': 1, 'b': [1, 2]}, 'tags': ['x', 'y'], 'mixed': 1},
        {'id': 2, 'big': None, 'text': None, 'mixed': 'one'},
    ])


def test_discovery_types_match_the_records(mongodb_url, mongo, tmp_path):
    """
    Embedded documents, arrays and Decimal128 were discovered as text while the sync
    emitted objects, lists and numbers, so destinations failed the records in schema
    validation. A field with values of several types got one of them.
    """
    seed(mongo)

    _, catalog = discover(mongodb_url, mongo.name, tmp_path)

    properties = catalog['streams'][0]['schema']['properties']
    assert {name: prop['type'] for name, prop in properties.items()} == {
        '_id': ['null', 'string'], 'id': ['null', 'integer'], 'big': ['null', 'integer'],
        'f': ['null', 'number'], 'flag': ['null', 'boolean'], 'text': ['null', 'string'],
        'amount': ['null', 'number'], 'at': ['null', 'string'], 'doc': ['null', 'object'],
        'tags': ['null', 'array'], 'mixed': ['null', 'integer', 'string'],
    }
    assert properties['at']['format'] == 'date-time'


def test_sync_emits_dates_in_utc(mongodb_url, mongo, tmp_path):
    """
    BSON dates were read as local time, so on a host outside UTC every date was shifted by
    the host's offset. The source runs here with its zone at UTC-6.
    """
    seed(mongo)
    config, catalog = discover(mongodb_url, mongo.name, tmp_path)

    output = run([SOURCE, '--config', config, '--catalog', select(catalog, tmp_path)],
                 env={**__import__('os').environ, 'TZ': 'America/Costa_Rica'})

    first = next(r for r in records(output) if r['id'] == 1)
    assert first['at'] == '2024-01-01T12:00:00.123000Z'
    assert first['big'] == 2**53 + 1
    assert first['doc'] == {'a': 1, 'b': [1, 2]}


def test_incremental_sync_keeps_documents_after_the_bookmark(mongodb_url, mongo, tmp_path):
    """
    Datetime bookmarks were shifted by the host's offset as well, so the next incremental
    sync skipped the documents written in that window.
    """
    mongo['events'].insert_many([
        {'id': i, 'updated_at': dt.datetime(2024, 1, 1, 12, 0) + dt.timedelta(hours=i)}
        for i in range(3)
    ])
    config, catalog = discover(mongodb_url, mongo.name, tmp_path)
    catalog_path = select(catalog, tmp_path, 'INCREMENTAL', 'updated_at')
    env = {**__import__('os').environ, 'TZ': 'America/Costa_Rica'}

    first = run([SOURCE, '--config', config, '--catalog', catalog_path], env=env)
    state = [json.loads(line)['value'] for line in first.splitlines()
             if line.startswith('{') and json.loads(line).get('type') == 'STATE'][-1]
    mongo['events'].insert_one({'id': 3, 'updated_at': dt.datetime(2024, 1, 1, 16, 0)})
    state_path = tmp_path / 'state.json'
    state_path.write_text(json.dumps(state))
    second = run([SOURCE, '--config', config, '--catalog', catalog_path, '--state', state_path],
                 env=env)

    assert sorted(r['id'] for r in records(first)) == [0, 1, 2]
    assert 3 in [r['id'] for r in records(second)]


def destination(mongodb_url, database, tmp_path, messages, table_name='target'):
    input_path = tmp_path / 'input.jsonl'
    input_path.write_text('\n'.join(json.dumps(m) for m in messages) + '\n')
    config = json.dumps(dict(connection_string=mongodb_url, db_name=database,
                             table_name=table_name))
    run([DESTINATION, '--config_json', config, '--input_file_path', input_path,
         '--state', tmp_path / 'state_out.json'])


def test_source_to_destination(mongodb_url, mongo, tmp_path):
    """The destination failed on its first SCHEMA message after singer-sdk renamed a method."""
    seed(mongo)
    config, catalog = discover(mongodb_url, mongo.name, tmp_path)
    output = run([SOURCE, '--config', config, '--catalog', select(catalog, tmp_path)])
    messages = [json.loads(line) for line in output.splitlines() if line.startswith('{')]

    destination(mongodb_url, mongo.name, tmp_path, messages)

    copied = {d['id']: d for d in mongo['target'].find({}, {'_id': 0})}
    assert copied[1]['big'] == 2**53 + 1
    assert copied[1]['doc'] == {'a': 1, 'b': [1, 2]}
    assert copied[1]['tags'] == ['x', 'y']
    assert copied[1]['text'] == 'ñ "q"'
    assert copied[2]['big'] is None


def schema_message(keys, properties):
    return dict(type='SCHEMA', stream='orders', key_properties=keys,
                schema=dict(type='object', properties=properties))


def test_destination_upserts_on_every_key(mongodb_url, mongo, tmp_path):
    """
    Only the first key property identified a document, so records that differed in a later
    key replaced each other.
    """
    properties = {'id': {'type': ['integer']}, 'region': {'type': ['string']},
                  'n': {'type': ['null', 'number']}}
    rows = [{'id': 1, 'region': 'eu', 'n': 9007199254740993}, {'id': 1, 'region': 'us', 'n': 2}]
    messages = [schema_message(['id', 'region'], properties)]
    messages += [dict(type='RECORD', stream='orders', record=r) for r in rows]
    messages.append(dict(type='STATE', value={}))

    destination(mongodb_url, mongo.name, tmp_path, messages)
    destination(mongodb_url, mongo.name, tmp_path, messages)

    stored = sorted((d['id'], d['region'], d['n']) for d in mongo['target'].find())
    assert stored == [(1, 'eu', 2**53 + 1), (1, 'us', 2)]


def test_destination_keeps_ids_that_are_not_object_ids(mongodb_url, mongo, tmp_path):
    """Records whose _id was not an ObjectId string were skipped without an error."""
    properties = {'_id': {'type': ['string']}, 'v': {'type': ['null', 'string']}}
    messages = [
        schema_message(['_id'], properties),
        dict(type='RECORD', stream='orders', record={'_id': 'order-1', 'v': 'a'}),
        dict(type='RECORD', stream='orders', record={'_id': '0123456789abcdef01234567',
                                                      'v': 'b'}),
        dict(type='STATE', value={}),
    ]

    destination(mongodb_url, mongo.name, tmp_path, messages)

    ids = sorted(str(d['_id']) for d in mongo['target'].find())
    assert ids == ['0123456789abcdef01234567', 'order-1']
