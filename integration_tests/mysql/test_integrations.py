"""
The MySQL source and destination of mage_integrations against MySQL 8.4, run as programs
the way Mage runs them: the source writes Singer messages, the destination reads them.
"""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / 'mage_integrations' / 'mage_integrations'
SOURCE = ROOT / 'sources' / 'mysql' / '__init__.py'
DESTINATION = ROOT / 'destinations' / 'mysql' / '__init__.py'


def run(args, **kwargs):
    result = subprocess.run(
        [sys.executable, *map(str, args)], capture_output=True, text=True, timeout=300,
        **kwargs,
    )
    assert result.returncode == 0, result.stderr[-4000:]
    return result.stdout


def connection_config(mysql_settings, database, **extra):
    return dict(
        database=database, host=mysql_settings['host'], port=int(mysql_settings['port']),
        username=mysql_settings['user'], password=mysql_settings['password'], **extra,
    )


def discover(mysql_settings, database, tmp_path):
    config = tmp_path / 'source.json'
    config.write_text(json.dumps(connection_config(mysql_settings, database)))
    output = run([SOURCE, '--config', config, '--discover'])
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


def write(mysql_settings, database, table, messages, tmp_path):
    input_path = tmp_path / 'input.jsonl'
    input_path.write_text('\n'.join(json.dumps(m) for m in messages) + '\n')
    config = json.dumps(connection_config(mysql_settings, database, table=table))
    run([DESTINATION, '--config_json', config, '--input_file_path', input_path,
         '--state', tmp_path / 'state_out.json'])


# How each source column compares with what the destination wrote. Singer has no
# decimal, date, time or binary type: DECIMAL is written as DOUBLE, DATE as DATETIME(6) at
# midnight, TIME and YEAR as text, BIT as its number, and binary values as text, in
# bytea's hex format when they are not UTF-8. A JSON null arrives as NULL.
COMPARISONS = {
    # FLOAT is single precision; the source writes its decimal text, read as a double.
    'c_float': 'ABS(d.c_float - s.c_float) <= ABS(s.c_float) * 1e-6',
    'c_decimal': 'd.c_decimal = CAST(s.c_decimal AS DOUBLE)',
    'c_decimal_wide': 'd.c_decimal_wide = CAST(s.c_decimal_wide AS DOUBLE)',
    'c_binary': (
        "d.c_binary = COALESCE(CONVERT(s.c_binary USING utf8mb4), "
        "CONCAT('\\\\x', LOWER(HEX(s.c_binary))))"
    ),
    'c_blob': (
        "d.c_blob = COALESCE(CONVERT(s.c_blob USING utf8mb4), "
        "CONCAT('\\\\x', LOWER(HEX(s.c_blob))))"
    ),
    'c_date': 'd.c_date = CAST(s.c_date AS DATETIME(6))',
    'c_time': 'CAST(d.c_time AS TIME(6)) = s.c_time',
    'c_year': 'CAST(d.c_year AS UNSIGNED) = s.c_year',
    'c_bit': 'CAST(d.c_bit AS UNSIGNED) = CAST(s.c_bit AS UNSIGNED)',
    'c_set': 'd.c_set = CAST(s.c_set AS CHAR)',
    'c_enum': 'd.c_enum = CAST(s.c_enum AS CHAR)',
}
SKIPPED = {'c_json': "JSON_TYPE(s.c_json) = 'NULL'"}


def mismatches(my, table='dst'):
    from integration_tests.data import mysql_dataset

    found = {}
    with my.cursor() as cursor:
        for name in mysql_dataset.COLUMN_NAMES:
            comparison = COMPARISONS.get(name, f'd.`{name}` = s.`{name}`')
            skip = f'AND NOT COALESCE({SKIPPED[name]}, false)' if name in SKIPPED else ''
            cursor.execute(f"""
                SELECT s.id FROM src s JOIN `{table}` d USING (id)
                WHERE ((s.`{name}` IS NULL) <> (d.`{name}` IS NULL)
                   OR NOT COALESCE({comparison}, s.`{name}` IS NULL)) {skip}
                ORDER BY s.id LIMIT 3
            """)
            ids = [row[0] for row in cursor.fetchall()]
            if ids:
                found[name] = ids
    return found


def fetch_one(my, statement):
    with my.cursor() as cursor:
        cursor.execute(statement)
        return cursor.fetchone()


def test_full_table_sync(mysql_settings, my, mysql_source, mysql_database, tmp_path):
    """
    The source failed on SET values and on bytes that are not UTF-8, and its log
    messages failed on them too. The destination cast integers to UNSIGNED, so every
    negative value was out of range, and created INT, CHAR(255) and CHAR(52) columns,
    which could not hold a BIGINT and cut text at 255 characters; a backslash in text
    ended its statement.
    """
    config, catalog = discover(mysql_settings, mysql_database, tmp_path)
    messages = sync(config, select(catalog, tmp_path))

    write(mysql_settings, mysql_database, 'dst', messages, tmp_path)

    rows = fetch_one(my, 'SELECT COUNT(*) FROM src')[0]
    assert fetch_one(my, 'SELECT COUNT(*) FROM dst')[0] == rows
    assert mismatches(my) == {}


def test_timestamps_are_in_utc(mysql_settings, my, mysql_source, mysql_database, tmp_path):
    """
    MySQL returns TIMESTAMP values in the session time zone, -06:00 on the test server,
    and the source wrote them without an offset, so the destination stored them 6 hours
    off.
    """
    config, catalog = discover(mysql_settings, mysql_database, tmp_path)

    write(mysql_settings, mysql_database, 'dst', sync(config, select(catalog, tmp_path)),
          tmp_path)

    with my.cursor() as cursor:
        cursor.execute("SET time_zone = '+00:00'")
        cursor.execute(
            'SELECT COUNT(*) FROM src s JOIN dst d USING (id) '
            'WHERE NOT (CAST(s.c_timestamp AS DATETIME(6)) <=> d.c_timestamp)',
        )
        assert cursor.fetchone()[0] == 0


def test_unsigned_columns(mysql_settings, my, mysql_source, mysql_database, tmp_path):
    """CAST to SIGNED wrapped 18446744073709551615 around to -1."""
    _, catalog = discover(mysql_settings, mysql_database, tmp_path)

    properties = catalog['streams'][0]['schema']['properties']
    assert properties['c_ubigint']['minimum'] == 0
    assert 'minimum' not in properties['c_bigint']


def test_incremental_sync(mysql_settings, my, mysql_source, mysql_database, tmp_path):
    config, catalog = discover(mysql_settings, mysql_database, tmp_path)
    catalog_path = select(
        catalog, tmp_path, replication_method='INCREMENTAL', replication_key='id',
        bookmark_properties=['id'],
    )
    first = sync(config, catalog_path)
    state = [m for m in first if m['type'] == 'STATE'][-1]['value']
    state_path = tmp_path / 'state.json'
    state_path.write_text(json.dumps(state))
    with my.cursor() as cursor:
        cursor.execute('SELECT MAX(id) FROM src')
        top = cursor.fetchone()[0]
        cursor.execute(
            'INSERT INTO src (id, c_text) VALUES (%s, %s), (%s, %s)',
            (top + 1, 'new one', top + 2, "it's new \\ here"),
        )
    my.commit()

    second = sync(config, catalog_path, state_path=state_path)

    new_ids = sorted(m['record']['id'] for m in second if m['type'] == 'RECORD')
    assert new_ids[-2:] == [top + 1, top + 2]
    assert len(new_ids) <= 3


def test_upserts_on_unique_constraints(mysql_settings, my, mysql_source, mysql_database,
                                       tmp_path):
    config, catalog = discover(mysql_settings, mysql_database, tmp_path)
    catalog_path = select(
        catalog, tmp_path, unique_constraints=['id'], unique_conflict_method='UPDATE',
    )
    write(mysql_settings, mysql_database, 'dst', sync(config, catalog_path), tmp_path)
    with my.cursor() as cursor:
        cursor.execute('UPDATE src SET c_text = %s WHERE id = 1', ("changed 'here' \\",))
    my.commit()

    write(mysql_settings, mysql_database, 'dst', sync(config, catalog_path), tmp_path)

    total, distinct = fetch_one(my, 'SELECT COUNT(*), COUNT(DISTINCT id) FROM dst')
    assert total == distinct
    assert fetch_one(my, 'SELECT c_text FROM dst WHERE id = 1')[0] == "changed 'here' \\"
