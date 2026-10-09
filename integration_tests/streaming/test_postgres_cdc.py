"""
The PostgreSQL streaming source, which reads changes with logical replication.
"""
import datetime as dt
import decimal
import threading
import time
import uuid

import pytest

from integration_tests.streaming_runner import MODES, streaming_pipeline, wait_until

KEY = uuid.UUID(int=9)
LARGE = 'y' * 20_000


class Stop(Exception):
    pass


@pytest.fixture
def cdc_table(pg, schema):
    """A table, and the names of a slot and a publication dropped after the test."""
    with pg.cursor() as cursor:
        cursor.execute(f'''
            CREATE TABLE {schema}.events (
                id integer PRIMARY KEY, name text, amount numeric(12, 2), flag boolean,
                created timestamptz, tags text[], doc jsonb, key uuid, body text
            )
        ''')
        cursor.execute(f'CREATE TABLE {schema}.other (id integer PRIMARY KEY)')
    pg.commit()
    names = dict(
        slot=f'it_{uuid.uuid4().hex[:12]}', publication=f'it_{uuid.uuid4().hex[:12]}',
    )
    yield names
    with pg.cursor() as cursor:
        cursor.execute(
            'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots '
            'WHERE slot_name = %s', (names['slot'],),
        )
        cursor.execute(f"DROP PUBLICATION IF EXISTS {names['publication']}")
    pg.commit()


def execute(pg, *statements):
    with pg.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)
    pg.commit()


def source(postgres_settings, schema, cdc_table, checkpoint_path=None, **config):
    from mage_ai.streaming.sources.postgres import PostgresSource

    return PostgresSource(dict(
        host=postgres_settings['host'], port=int(postgres_settings['port']),
        database=postgres_settings['dbname'], user=postgres_settings['user'],
        password=postgres_settings['password'], replication_slot=cdc_table['slot'],
        publication_name=cdc_table['publication'], tables=[f'{schema}.events'],
        batch_timeout=0.3, **config,
    ), checkpoint_path=checkpoint_path)


def run_source(reader, handler, actions, timeout=30):
    errors = []

    def target():
        try:
            reader.batch_read(handler)
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    time.sleep(1)
    actions()
    thread.join(timeout)
    reader.destroy()
    assert not thread.is_alive(), 'batch_read did not return'
    return errors[0] if errors else None


def test_changes_with_their_types(postgres_settings, pg, schema, cdc_table):
    received = []

    def handle(changes):
        received.extend(changes)
        if len(received) >= 4:
            raise Stop

    def changes():
        execute(
            pg,
            f"INSERT INTO {schema}.events VALUES (1, 'it''s one', 1.50, true, "
            f"'2024-01-01 12:00:00.123456+00', '{{a,\"b c\"}}', '{{\"k\": [1, null]}}', "
            f"'{KEY}', NULL)",
            f"INSERT INTO {schema}.events (id, name) VALUES (2, 'two')",
            f"UPDATE {schema}.events SET name = 'two again' WHERE id = 2",
            f'DELETE FROM {schema}.events WHERE id = 1',
        )

    error = run_source(source(postgres_settings, schema, cdc_table), handle, changes)

    assert isinstance(error, Stop)
    first, second, third, fourth = received
    assert {k: v for k, v in first.items() if not k.startswith('_mage')} == dict(
        id=1, name="it's one", amount=decimal.Decimal('1.50'), flag=True,
        created=dt.datetime(2024, 1, 1, 12, 0, 0, 123456, tzinfo=dt.timezone.utc),
        tags=['a', 'b c'], doc={'k': [1, None]}, key=KEY, body=None,
    )
    assert (first['_mage_operation'], first['_mage_schema'], first['_mage_table']) == (
        'insert', schema, 'events',
    )
    assert first['_mage_lsn'] > 0 and first['_mage_commit_time']
    assert (second['_mage_operation'], third['_mage_operation']) == ('insert', 'update')
    assert third['name'] == 'two again'
    # A delete carries the key alone with the default replica identity.
    assert fourth['_mage_operation'] == 'delete'
    assert {k: v for k, v in fourth.items() if not k.startswith('_mage')} == {'id': 1}
    assert fourth['_mage_deleted_at']


def test_a_restarted_source_continues_after_handled_changes(
    postgres_settings, pg, schema, cdc_table, tmp_path,
):
    checkpoint = str(tmp_path / 'checkpoint.json')
    first = []

    def handle_first(changes):
        if any(c['id'] == 2 for c in changes):
            raise Stop
        first.extend(c['id'] for c in changes)

    def two_transactions():
        execute(pg, f'INSERT INTO {schema}.events (id) VALUES (1)')
        time.sleep(1)
        execute(pg, f'INSERT INTO {schema}.events (id) VALUES (2)')

    run_source(
        source(postgres_settings, schema, cdc_table, checkpoint, batch_size=1),
        handle_first, two_transactions,
    )
    execute(pg, f'INSERT INTO {schema}.events (id) VALUES (3)')
    second = []

    def handle_second(changes):
        second.extend(c['id'] for c in changes)
        if 3 in second:
            raise Stop

    run_source(
        source(postgres_settings, schema, cdc_table, checkpoint), handle_second, lambda: None,
    )

    assert first == [1]
    # The change whose handler failed comes again; the handled one does not.
    assert second == [2, 3]


def test_unchanged_large_values_are_read_from_the_table(
    postgres_settings, pg, schema, cdc_table,
):
    received = []

    def handle(changes):
        received.extend(changes)
        if any(c['_mage_operation'] == 'update' for c in received):
            raise Stop

    def changes():
        execute(pg, f"INSERT INTO {schema}.events (id, name, body) VALUES (1, 'a', '{LARGE}')")
        execute(pg, f"UPDATE {schema}.events SET name = 'b' WHERE id = 1")

    run_source(source(postgres_settings, schema, cdc_table), handle, changes)

    update = [c for c in received if c['_mage_operation'] == 'update'][0]
    assert (update['name'], update['body']) == ('b', LARGE)


def test_only_the_selected_tables_are_read(postgres_settings, pg, schema, cdc_table):
    execute(pg, f"CREATE PUBLICATION {cdc_table['publication']} "
                f'FOR TABLE {schema}.events, {schema}.other')
    received = []

    def handle(changes):
        received.extend((c['_mage_table'], c['id']) for c in changes)
        raise Stop

    def changes():
        execute(pg, f'INSERT INTO {schema}.other VALUES (99)',
                f'INSERT INTO {schema}.events (id) VALUES (5)')

    run_source(source(postgres_settings, schema, cdc_table), handle, changes)

    assert received == [('events', 5)]


@pytest.mark.parametrize('mode', MODES)
def test_postgres_changes_to_kafka(
    mode, mage_project, postgres_settings, pg, schema, cdc_table, kafka_bootstrap,
    kafka_topic,
):
    from integration_tests.streaming.test_pipelines import read_topic

    with streaming_pipeline(
        'stream_postgres', mode, tables=f'{schema}.events', slot=cdc_table['slot'],
        publication=cdc_table['publication'], out_topic=kafka_topic,
    ) as run:
        # The source creates the slot when it starts; changes before that are not read.
        def slot_exists():
            with pg.cursor() as cursor:
                cursor.execute('SELECT 1 FROM pg_replication_slots WHERE slot_name = %s',
                               (cdc_table['slot'],))
                found = cursor.fetchone()
            pg.rollback()
            return found

        wait_until(slot_exists, run, message='the replication slot')
        for i in range(20):
            execute(pg, f"INSERT INTO {schema}.events (id, name, amount) "
                        f"VALUES ({i}, 'n{i}', {i}.25)")
        values = wait_until(
            lambda: read_topic(kafka_bootstrap, kafka_topic, 20), run, message='Kafka sink',
        )

    assert sorted((v['id'], v['name'], v['amount']) for v in values) == [
        (i, f'n{i}', i + 0.25) for i in range(20)
    ]
    assert {v['_mage_operation'] for v in values} == {'insert'}


def test_the_notebook_check_creates_no_slot(
    mage_project, postgres_settings, pg, schema, cdc_table, capsys,
):
    """A slot keeps the server's log until it is read, so checking a block makes none."""
    from integration_tests.streaming.test_notebook import run_block

    run_block(
        'stream_postgres', 'stream_postgres_source', tables=f'{schema}.events',
        slot=cdc_table['slot'], publication=cdc_table['publication'],
    )

    output = capsys.readouterr().out
    assert 'wal_level: logical' in output
    assert f"Replication slot {cdc_table['slot']}: missing" in output
    with pg.cursor() as cursor:
        cursor.execute('SELECT 1 FROM pg_replication_slots WHERE slot_name = %s',
                       (cdc_table['slot'],))
        assert cursor.fetchone() is None
    pg.rollback()
