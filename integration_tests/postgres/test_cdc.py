"""
Change data capture with the PostgreSQL source of mage_integrations: LOG_BASED
replication, which reads the write-ahead log through a logical replication slot with
pgoutput, written to the PostgreSQL destination.
"""
import json
import subprocess
import sys
import time
import uuid

import pytest

from integration_tests.postgres.test_integrations import (
    DESTINATION,
    SOURCE,
    connection_config,
    discover,
    run,
)

LARGE = 'x' * 20_000


@pytest.fixture
def cdc(postgres_settings, pg, schema):
    """A table with a publication and a pgoutput replication slot of its own."""
    slot = f'it_{uuid.uuid4().hex[:12]}'
    publication = f'it_{uuid.uuid4().hex[:12]}'
    with pg.cursor() as cursor:
        cursor.execute(f'''
            CREATE TABLE {schema}.events (
                id integer PRIMARY KEY,
                name text,
                amount numeric(12, 2),
                flag boolean,
                created timestamptz,
                tags text[],
                doc jsonb,
                body text
            )
        ''')
        cursor.execute(f'''
            INSERT INTO {schema}.events VALUES
            (1, 'one', 1.50, true, '2024-01-01 12:00:00+00', '{{a,b}}', '{{"k": 1}}', %s),
            (2, 'two', 2.25, false, '2024-01-02 12:00:00+00', NULL, NULL, NULL)
        ''', (LARGE,))
        cursor.execute(f'CREATE PUBLICATION {publication} FOR TABLE {schema}.events')
    pg.commit()
    # A slot is created in a transaction without writes.
    with pg.cursor() as cursor:
        cursor.execute(
            "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,),
        )
    pg.commit()
    yield dict(slot=slot, publication=publication)
    with pg.cursor() as cursor:
        cursor.execute('SELECT pg_drop_replication_slot(%s)', (slot,))
        cursor.execute(f'DROP PUBLICATION {publication}')
    pg.commit()


def execute(pg, *statements):
    with pg.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)
    pg.commit()


class Syncs:
    """Runs the source with LOG_BASED replication, keeping its state between runs."""

    def __init__(self, postgres_settings, schema, cdc, tmp_path, poll_seconds=30):
        self.tmp_path = tmp_path
        self.state = None
        self.history = []
        config = connection_config(
            postgres_settings, schema, replication_slot=cdc['slot'],
            publication_name=cdc['publication'], logical_poll_total_seconds=poll_seconds,
        )
        self.config_path = tmp_path / 'cdc_source.json'
        self.config_path.write_text(json.dumps(config))
        _, catalog = discover(postgres_settings, schema, tmp_path)
        catalog['streams'] = [s for s in catalog['streams'] if s['tap_stream_id'] == 'events']
        for entry in catalog['streams']:
            entry['replication_method'] = 'LOG_BASED'
            entry['unique_constraints'] = ['id']
            entry['unique_conflict_method'] = 'UPDATE'
            for metadata in entry['metadata']:
                metadata['metadata']['selected'] = True
        self.catalog_path = tmp_path / 'catalog.json'
        self.catalog_path.write_text(json.dumps(catalog))

    def args(self):
        args = [sys.executable, str(SOURCE), '--config', str(self.config_path),
                '--catalog', str(self.catalog_path)]
        if self.state is not None:
            state_path = self.tmp_path / 'state.json'
            state_path.write_text(json.dumps(self.state))
            args += ['--state', str(state_path)]
        return args

    def finish(self, output):
        messages = [json.loads(line) for line in output.splitlines() if line.startswith('{')]
        self.log = [m.get('message') for m in messages if m.get('type') not in
                    ('SCHEMA', 'RECORD', 'STATE')]
        states = [m['value'] for m in messages if m.get('type') == 'STATE']
        if states:
            self.state = states[-1]
        return [m for m in messages if m.get('type') in ('SCHEMA', 'RECORD')]

    def sync(self):
        result = subprocess.run(self.args(), capture_output=True, text=True, timeout=300)
        assert result.returncode == 0, result.stderr[-4000:]
        self.stderr = result.stderr
        self.history.append(self.log_lines(result.stderr))
        return self.finish(result.stdout)

    @staticmethod
    def log_lines(stderr):
        return [
            json.loads(line[line.index('{'):])['message'] for line in stderr.splitlines()
            if '{"caller"' in line and ('Logical' in line or 'log' in line or 'end_lsn' in line)
        ]

    def start(self):
        return subprocess.Popen(self.args(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True)


def records(messages):
    return [m['record'] for m in messages if m['type'] == 'RECORD']


def write(postgres_settings, schema, messages, tmp_path):
    input_path = tmp_path / 'input.jsonl'
    input_path.write_text('\n'.join(json.dumps(m) for m in messages) + '\n')
    config = json.dumps(connection_config(postgres_settings, schema, table='dst'))
    run([DESTINATION, '--config_json', config, '--input_file_path', input_path,
         '--state', tmp_path / 'state_out.json'])


def destination_rows(pg, schema):
    with pg.cursor() as cursor:
        cursor.execute(f'''
            SELECT id, name, amount, flag, created, tags, doc, body,
                   _mage_deleted_at IS NOT NULL
            FROM {schema}.dst ORDER BY id
        ''')
        rows = cursor.fetchall()
    pg.rollback()
    return rows


def test_changes_after_the_initial_sync_reach_the_destination(
    postgres_settings, pg, schema, cdc, tmp_path,
):
    syncs = Syncs(postgres_settings, schema, cdc, tmp_path)
    initial = syncs.sync()
    assert sorted(r['id'] for r in records(initial)) == [1, 2]
    write(postgres_settings, schema, initial, tmp_path)

    execute(
        pg,
        f"INSERT INTO {schema}.events VALUES (3, 'it''s three', 3.75, NULL, "
        f"'2024-01-03 12:00:00+00', '{{\"x,y\",NULL}}', '{{\"a\": [1, null]}}', 'small')",
        f"UPDATE {schema}.events SET name = 'two again', amount = 20.5 WHERE id = 2",
        f'DELETE FROM {schema}.events WHERE id = 3',
        f"INSERT INTO {schema}.events (id, name) VALUES (4, 'four')",
    )
    changes = syncs.sync()
    write(postgres_settings, schema, changes, tmp_path)

    rows = destination_rows(pg, schema)
    assert [(r[0], r[1], float(r[2]) if r[2] is not None else None, r[8]) for r in rows] == [
        (1, 'one', 1.5, False),
        (2, 'two again', 20.5, False),
        (3, "it's three", 3.75, True),
        (4, 'four', None, False),
    ]


def test_changes_made_during_a_sync_are_not_lost(postgres_settings, pg, schema, cdc, tmp_path):
    """
    A log sync stopped at the LSN it read when it started and then bookmarked the
    server's current LSN, so a change committed while it ran was never read.
    """
    syncs = Syncs(postgres_settings, schema, cdc, tmp_path, poll_seconds=6)
    syncs.sync()
    execute(pg, f"INSERT INTO {schema}.events (id, name) VALUES (10, 'before')")

    process = syncs.start()
    time.sleep(3)
    execute(pg, f"INSERT INTO {schema}.events (id, name) VALUES (11, 'during')")
    output, errors = process.communicate(timeout=120)
    assert process.returncode == 0, errors[-4000:]
    syncs.history.append(syncs.log_lines(errors))
    during = records(syncs.finish(output))
    after = records(syncs.sync())

    assert sorted(r['id'] for r in during + after) == [10, 11], syncs.history


def test_tables_of_other_schemas_are_ignored(postgres_settings, pg, schema, cdc, tmp_path):
    """Changes were matched by table name, so a table of another schema leaked in."""
    other = f'it_{uuid.uuid4().hex[:12]}'
    execute(pg, f'CREATE SCHEMA {other}',
            f'CREATE TABLE {other}.events (id integer PRIMARY KEY, name text)',
            f"ALTER PUBLICATION {cdc['publication']} ADD TABLE {other}.events")
    try:
        syncs = Syncs(postgres_settings, schema, cdc, tmp_path)
        syncs.sync()
        execute(pg, f"INSERT INTO {other}.events VALUES (99, 'other schema')",
                f"INSERT INTO {schema}.events (id, name) VALUES (5, 'this schema')")

        changes = records(syncs.sync())
    finally:
        execute(pg, f'DROP SCHEMA {other} CASCADE')

    assert [(r['id'], r['name']) for r in changes] == [(5, 'this schema')]


def test_unchanged_large_values_are_kept(postgres_settings, pg, schema, cdc, tmp_path):
    """
    PostgreSQL leaves a large value an update did not change out of the log, and the
    change carried NULL for it, which the destination wrote over the value.
    """
    syncs = Syncs(postgres_settings, schema, cdc, tmp_path)
    write(postgres_settings, schema, syncs.sync(), tmp_path)
    execute(pg, f"UPDATE {schema}.events SET name = 'one again' WHERE id = 1")

    state = syncs.state
    changes = syncs.sync()
    write(postgres_settings, schema, changes, tmp_path)

    row = destination_rows(pg, schema)[0]
    assert (row[1], row[7]) == ('one again', LARGE), ' | '.join([str(state)] + [
        json.loads(line[line.index('{'):])['message'] for line in syncs.stderr.splitlines()
        if '{"caller"' in line
    ])


def test_value_types(postgres_settings, pg, schema, cdc, tmp_path):
    """The log has values as text; records carry the types of the columns."""
    syncs = Syncs(postgres_settings, schema, cdc, tmp_path)
    syncs.sync()
    # The next change can start at the slot's confirmed LSN, as it does on an idle
    # server; the bookmark must be before it, or that change is skipped.
    with pg.cursor() as cursor:
        cursor.execute(
            'SELECT confirmed_flush_lsn - %s::pg_lsn FROM pg_replication_slots '
            'WHERE slot_name = %s',
            ('0/0', cdc['slot']),
        )
        confirmed = int(cursor.fetchone()[0])
    pg.rollback()
    assert syncs.state['bookmarks']['events']['lsn'] == confirmed - 1
    execute(
        pg,
        f"INSERT INTO {schema}.events VALUES (7, 'seven', 7.25, true, "
        f"'2024-01-07 12:00:00.123456+00', '{{a,\"b c\"}}', '{{\"k\": [1, 2]}}', NULL)",
    )

    (record,) = records(syncs.sync())

    assert record['id'] == 7
    assert record['amount'] == 7.25
    assert record['flag'] is True
    assert record['created'].startswith('2024-01-07T12:00:00.123456')
    assert record['tags'] == ['a', 'b c']
    assert record['doc'] == {'k': [1, 2]}
    assert record['body'] is None


def test_a_delete_in_a_later_sync_keeps_the_row_values(
    postgres_settings, pg, schema, cdc, tmp_path,
):
    """
    A delete carries the key alone, and its NULLs were written over the row; the row is
    marked as deleted and keeps its values.
    """
    syncs = Syncs(postgres_settings, schema, cdc, tmp_path)
    write(postgres_settings, schema, syncs.sync(), tmp_path)
    execute(pg, f'DELETE FROM {schema}.events WHERE id = 2')

    write(postgres_settings, schema, syncs.sync(), tmp_path)

    row = destination_rows(pg, schema)[1]
    assert (row[0], row[1], float(row[2]), row[8]) == (2, 'two', 2.25, True)


def test_a_changed_primary_key_deletes_the_old_row(postgres_settings, pg, schema, cdc, tmp_path):
    syncs = Syncs(postgres_settings, schema, cdc, tmp_path)
    write(postgres_settings, schema, syncs.sync(), tmp_path)
    execute(pg, f'UPDATE {schema}.events SET id = 20 WHERE id = 2')

    write(postgres_settings, schema, syncs.sync(), tmp_path)

    rows = {row[0]: row for row in destination_rows(pg, schema)}
    assert rows[2][8] is True
    assert (rows[20][1], rows[20][8]) == ('two', False)


def test_a_failed_destination_reads_the_changes_again(
    postgres_settings, pg, schema, cdc, tmp_path,
):
    """
    Each change was confirmed to the server when it was read, so when the destination
    failed and the state was not saved, the next run found nothing.
    """
    syncs = Syncs(postgres_settings, schema, cdc, tmp_path)
    syncs.sync()
    saved = syncs.state
    execute(pg, f"INSERT INTO {schema}.events (id, name) VALUES (30, 'lost?')")
    assert [r['id'] for r in records(syncs.sync())] == [30]

    # The destination failed, so the state of that run was not kept.
    syncs.state = saved
    assert [r['id'] for r in records(syncs.sync())] == [30], syncs.history


def test_a_sync_without_changes_stops_when_it_has_read_the_log(
    postgres_settings, pg, schema, cdc, tmp_path,
):
    """A run waited logical_poll_total_seconds, 60 by default, when there were no changes."""
    syncs = Syncs(postgres_settings, schema, cdc, tmp_path, poll_seconds=60)
    syncs.sync()

    started = time.monotonic()
    assert records(syncs.sync()) == []

    assert time.monotonic() - started < 30
