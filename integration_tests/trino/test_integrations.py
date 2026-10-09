"""
The Trino destination of mage_integrations against Trino 483, run as a program the way
Mage runs it, in each catalog: memory, Iceberg and Delta Lake.
"""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / 'mage_integrations' / 'mage_integrations'
DESTINATION = ROOT / 'destinations' / 'trino' / '__init__.py'
# The destination's connector setting for each catalog.
CONNECTORS = {'memory': 'memory', 'iceberg': 'iceberg', 'delta': 'delta-lake'}

SCHEMA = {
    'properties': {
        'id': {'type': ['integer']},
        'big': {'type': ['null', 'integer']},
        'price': {'type': ['null', 'number']},
        'flag': {'type': ['null', 'boolean']},
        'name': {'type': ['null', 'string']},
        'created': {'type': ['null', 'string'], 'format': 'date-time'},
        'payload': {'type': ['null', 'object']},
        'tags': {'type': ['null', 'array'], 'items': {'type': ['null', 'string']}},
        'counts': {'type': ['null', 'array'], 'items': {'type': ['null', 'integer']}},
    },
    'type': 'object',
}
LONG_TEXT = 'ñ' * 1000
RECORDS = [
    {
        'id': 1, 'big': 2**63 - 1, 'price': 1.5, 'flag': True,
        'name': 'it\'s "quoted" \\ back \n newline 😀',
        'created': '2024-01-01T12:00:00.123456+00:00',
        'payload': {'a': "it's", 'b': [1, None], 'c': '\\'},
        'tags': ['a', "it's", 'x,y', '"q"'], 'counts': [1, 2],
    },
    {
        'id': 2, 'big': -(2**63), 'price': 1e300, 'flag': False, 'name': LONG_TEXT,
        'created': '1900-01-01T00:00:00+00:00', 'payload': {}, 'tags': [], 'counts': [],
    },
    {
        'id': 3, 'big': None, 'price': None, 'flag': None, 'name': None, 'created': None,
        'payload': None, 'tags': None, 'counts': None,
    },
]


def messages(records, **schema_message):
    return [
        dict(type='SCHEMA', stream='items', schema=json.loads(json.dumps(SCHEMA)),
             key_properties=['id'], replication_method='FULL_TABLE', **schema_message),
        *[dict(type='RECORD', stream='items', record=record) for record in records],
    ]


def write(trino_settings, catalog, schema, messages_, tmp_path):
    input_path = tmp_path / 'input.jsonl'
    input_path.write_text('\n'.join(json.dumps(m) for m in messages_) + '\n')
    config = json.dumps(dict(
        catalog=catalog, connector=CONNECTORS[catalog], host=trino_settings['host'],
        port=trino_settings['port'], username=trino_settings['user'], schema=schema,
        table='items',
    ))
    result = subprocess.run(
        [sys.executable, str(DESTINATION), '--config_json', config,
         '--input_file_path', str(input_path), '--state', str(tmp_path / 'state.json')],
        capture_output=True, text=True, timeout=300,
    )
    assert result.returncode == 0, result.stderr[-4000:]


def rows(tr):
    return tr(
        'SELECT id, big, price, flag, name, created, payload, tags, counts FROM items '
        'ORDER BY id'
    )


def test_values_arrive_unchanged(trino_settings, trino_catalog, trino_schema, tr, tmp_path):
    """
    Apostrophes became double quotes, so it's was written as it""s and JSON with an
    apostrophe was not JSON. Text was cut at 255 characters. In the memory connector, an
    apostrophe in an array failed the sync, arrays did not match their JSON columns, and
    objects were stored as JSON strings. Empty arrays became NULL.
    """
    write(trino_settings, trino_catalog, trino_schema, messages(RECORDS), tmp_path)

    first, second, third = rows(tr)
    assert first[:5] == [1, 2**63 - 1, 1.5, True, 'it\'s "quoted" \\ back \n newline 😀']
    assert json.loads(first[6]) == {'a': "it's", 'b': [1, None], 'c': '\\'}
    tags, counts = first[7:]
    if trino_catalog == 'memory':
        # The memory connector stores arrays as JSON.
        tags, counts = json.loads(tags), json.loads(counts)
    assert [tags, counts] == [['a', "it's", 'x,y', '"q"'], [1, 2]]
    assert second[:5] == [2, -(2**63), 1e300, False, LONG_TEXT]
    assert json.loads(second[6]) == {}
    # Empty arrays were written as NULL.
    assert second[7:] == [[], []] or [json.loads(v) for v in second[7:]] == [[], []]
    assert third == [3] + [None] * 8


def test_date_times(trino_settings, trino_catalog, trino_schema, tr, tmp_path):
    write(trino_settings, trino_catalog, trino_schema, messages(RECORDS), tmp_path)

    created = [row[5] for row in rows(tr)]
    assert created[0].startswith('2024-01-01') and '12:00:00.123456' in created[0]
    assert created[2] is None


def test_appends_and_new_columns(trino_settings, trino_catalog, trino_schema, tr, tmp_path):
    write(trino_settings, trino_catalog, trino_schema, messages(RECORDS[:1]), tmp_path)
    SCHEMA['properties']['extra'] = {'type': ['null', 'string']}
    try:
        write(trino_settings, trino_catalog, trino_schema,
              messages([dict(RECORDS[1], extra="it's new")]), tmp_path)
    finally:
        del SCHEMA['properties']['extra']

    # Trino's memory connector fails to read a column added after the table had rows
    # unless the query reads every column.
    columns = [row[0] for row in tr('DESCRIBE items')]
    extra = [(row[0], row[columns.index('extra')]) for row in tr('SELECT * FROM items ORDER BY id')]
    assert extra == [(1, None), (2, "it's new")]


def test_upserts(trino_settings, trino_catalog, trino_schema, tr, tmp_path):
    upsert = dict(unique_constraints=['id'], unique_conflict_method='UPDATE')
    write(trino_settings, trino_catalog, trino_schema, messages(RECORDS, **upsert), tmp_path)
    changed = dict(RECORDS[0], name="changed 'here'")

    write(trino_settings, trino_catalog, trino_schema, messages([changed], **upsert), tmp_path)

    if trino_catalog == 'memory':
        # The memory connector cannot MERGE, so the row is appended.
        assert tr('SELECT count(*) FROM items WHERE id = 1') == [[2]]
    else:
        assert tr('SELECT count(*), count(DISTINCT id) FROM items') == [[3, 3]]
        assert tr('SELECT name FROM items WHERE id = 1') == [["changed 'here'"]]
