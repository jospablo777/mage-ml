"""
The PostgreSQL source and destination of mage_integrations against PostgreSQL 16, run as
programs the way Mage runs them: the source writes Singer messages, the destination reads
them.
"""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / 'mage_integrations' / 'mage_integrations'
SOURCE = ROOT / 'sources' / 'postgresql' / '__init__.py'
DESTINATION = ROOT / 'destinations' / 'postgresql' / '__init__.py'

# How each source column compares with what the destination wrote. Singer has no date,
# decimal, interval or binary type: the destination writes dates as timestamptz at
# midnight UTC and naive timestamps as UTC, numeric as double precision, interval, time,
# timetz, bytea and uuid as text, and char(n) as text with its padding.
COMPARISONS = {
    'c_numeric': 'd.c_numeric = s.c_numeric::double precision',
    'c_numeric_free': 'd.c_numeric_free = s.c_numeric_free::double precision',
    'c_real': 'd.c_real = s.c_real::text::double precision',
    'c_char': 'd.c_char::char(5) = s.c_char',
    'c_date': "d.c_date = s.c_date::timestamp AT TIME ZONE 'UTC'",
    'c_time': 'd.c_time::time = s.c_time',
    'c_timetz': 'd.c_timetz::timetz = s.c_timetz',
    'c_timestamp': "d.c_timestamp = s.c_timestamp AT TIME ZONE 'UTC'",
    'c_interval': 'd.c_interval::interval = s.c_interval',
    'c_bytea': 'd.c_bytea::bytea = s.c_bytea',
    'c_uuid': 'd.c_uuid::uuid = s.c_uuid',
    'c_json': 'd.c_json::jsonb = s.c_json::jsonb',
    'c_jsonb': 'd.c_jsonb::jsonb = s.c_jsonb',
    'c_enum': 'd.c_enum = s.c_enum::text',
    'c_numeric_array': 'd.c_numeric_array = s.c_numeric_array::double precision[]',
    'c_date_array': 'd.c_date_array::date[] = s.c_date_array',
    'c_uuid_array': 'd.c_uuid_array::uuid[] = s.c_uuid_array',
    'c_int_array': 'd.c_int_array = s.c_int_array::bigint[]',
    'c_int_matrix': 'd.c_int_matrix = s.c_int_matrix::bigint[]',
}
# JSON cannot hold NaN or Infinity, which arrive as NULL.
NOT_IN_JSON = {
    'c_real': "s.c_real IN ('NaN', 'Infinity', '-Infinity')",
    'c_double': "s.c_double IN ('NaN', 'Infinity', '-Infinity')",
    'c_numeric_free': "s.c_numeric_free = 'NaN'",
    'c_numeric': "s.c_numeric = 'NaN'",
}


def run(args, **kwargs):
    result = subprocess.run(
        [sys.executable, *map(str, args)], capture_output=True, text=True, timeout=300,
        **kwargs,
    )
    assert result.returncode == 0, result.stderr[-4000:]
    return result.stdout


def connection_config(postgres_settings, schema, **extra):
    return dict(
        database=postgres_settings['dbname'], host=postgres_settings['host'],
        password=postgres_settings['password'], port=int(postgres_settings['port']),
        username=postgres_settings['user'], schema=schema, **extra,
    )


def discover(postgres_settings, schema, tmp_path):
    config = tmp_path / 'source.json'
    config.write_text(json.dumps(connection_config(postgres_settings, schema)))
    output = run([SOURCE, '--config', config, '--discover'])
    # Log messages come first, one JSON object per line; the catalog follows, indented.
    lines = output.splitlines()
    start = next(i for i, line in enumerate(lines) if line == '{')
    return config, json.loads('\n'.join(lines[start:]))


def select(catalog, tmp_path, stream='src', replication_method='FULL_TABLE', **options):
    catalog['streams'] = [s for s in catalog['streams'] if s['tap_stream_id'] == stream]
    for entry in catalog['streams']:
        entry['replication_method'] = replication_method
        entry.update(options)
        for metadata in entry['metadata']:
            metadata['metadata']['selected'] = True
    path = tmp_path / 'catalog.json'
    path.write_text(json.dumps(catalog))
    return path


def sync(config, catalog_path, state_path=None):
    args = [SOURCE, '--config', config, '--catalog', catalog_path]
    if state_path:
        args += ['--state', state_path]
    output = run(args)
    messages = [json.loads(line) for line in output.splitlines() if line.startswith('{')]
    return [m for m in messages if m.get('type') in ('SCHEMA', 'RECORD', 'STATE')]


def write(postgres_settings, schema, table, messages, tmp_path):
    input_path = tmp_path / 'input.jsonl'
    input_path.write_text('\n'.join(json.dumps(m) for m in messages) + '\n')
    config = json.dumps(connection_config(postgres_settings, schema, table=table))
    run([DESTINATION, '--config_json', config, '--input_file_path', input_path,
         '--state', tmp_path / 'state_out.json'])


def mismatches(pg, schema, table='dst'):
    from integration_tests.data import postgres_dataset

    found = {}
    with pg.cursor() as cursor:
        for name in postgres_dataset.COLUMN_NAMES:
            comparison = COMPARISONS.get(name, f'd.{name} = s.{name}')
            skip = ''
            if name in NOT_IN_JSON:
                skip = f'AND NOT coalesce({NOT_IN_JSON[name]}, false)'
            cursor.execute(f"""
                SELECT s.id, s.{name}::text, d.{name}::text
                FROM {schema}.src s JOIN {schema}.{table} d USING (id)
                WHERE ((s.{name} IS NULL) <> (d.{name} IS NULL)
                   OR NOT coalesce({comparison}, s.{name} IS NULL)) {skip}
                ORDER BY s.id LIMIT 3
            """)
            rows = cursor.fetchall()
            if rows:
                found[name] = rows
    pg.rollback()
    return found


def fetch_one(pg, statement):
    with pg.cursor() as cursor:
        cursor.execute(statement)
        result = cursor.fetchone()
    pg.rollback()
    return result


def test_full_table_sync(postgres_settings, pg, schema, source_table, source_rows, tmp_path):
    """
    The source failed on bytea, timetz and interval values, which its JSON writer could
    not encode. It discovered arrays as their element type, so the destination wrote
    their JSON into integer and text columns and failed; it wrote JSON values unencoded
    and None in arrays as None, and an apostrophe in JSON ended its SQL string.
    """
    config, catalog = discover(postgres_settings, schema, tmp_path)

    write(postgres_settings, schema, 'dst', sync(config, select(catalog, tmp_path)), tmp_path)

    assert fetch_one(pg, f'SELECT count(*) FROM {schema}.dst')[0] == len(source_rows)
    assert mismatches(pg, schema) == {}


def test_discovered_types(postgres_settings, schema, source_table, tmp_path):
    _, catalog = discover(postgres_settings, schema, tmp_path)
    properties = catalog['streams'][0]['schema']['properties']

    assert properties['c_int_array'] == {
        'type': ['null', 'array'], 'items': {'type': ['null', 'integer']},
    }
    assert properties['c_text_array']['items'] == {'type': ['null', 'string']}
    assert properties['c_uuid_array']['items'] == {'type': ['null', 'string'], 'format': 'uuid'}
    # interval contains "int" and was discovered as an integer.
    assert properties['c_interval']['type'] == ['null', 'string']
    assert properties['c_jsonb']['type'] == ['null', 'object']


def test_incremental_sync(postgres_settings, pg, schema, source_table, source_rows, tmp_path):
    config, catalog = discover(postgres_settings, schema, tmp_path)
    catalog_path = select(
        catalog, tmp_path, replication_method='INCREMENTAL', replication_key='id',
        bookmark_properties=['id'],
    )
    first = sync(config, catalog_path)
    state = [m for m in first if m['type'] == 'STATE'][-1]['value']
    state_path = tmp_path / 'state.json'
    state_path.write_text(json.dumps(state))
    with pg.cursor() as cursor:
        cursor.execute(
            f'INSERT INTO {schema}.src (id, c_text) VALUES (%s, %s), (%s, %s)',
            (10**6, 'new one', 10**6 + 1, "it's new"),
        )
    pg.commit()

    second = sync(config, catalog_path, state_path=state_path)

    assert len([m for m in first if m['type'] == 'RECORD']) == len(source_rows)
    new_ids = sorted(m['record']['id'] for m in second if m['type'] == 'RECORD')
    # The bookmarked row may come again, as the source compares with >=.
    assert new_ids[-2:] == [10**6, 10**6 + 1]
    assert len(new_ids) <= 3


def test_upserts_on_unique_constraints(postgres_settings, pg, schema, source_table, tmp_path):
    config, catalog = discover(postgres_settings, schema, tmp_path)
    catalog_path = select(
        catalog, tmp_path, unique_constraints=['id'], unique_conflict_method='UPDATE',
    )
    write(postgres_settings, schema, 'dst', sync(config, catalog_path), tmp_path)
    with pg.cursor() as cursor:
        cursor.execute(f'UPDATE {schema}.src SET c_text = %s WHERE id = 1', ("changed 'here'",))
    pg.commit()

    write(postgres_settings, schema, 'dst', sync(config, catalog_path), tmp_path)

    total, distinct = fetch_one(pg, f'SELECT count(*), count(DISTINCT id) FROM {schema}.dst')
    assert total == distinct
    assert fetch_one(pg, f'SELECT c_text FROM {schema}.dst WHERE id = 1')[0] == "changed 'here'"
