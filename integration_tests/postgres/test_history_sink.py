"""
The PostgreSQL sink's history mode in editor runs (ADR-0043), with the same versions as the
engine's sink (connector-postgres/src/history.rs): each change a version of its key that
ends where the next starts, a late change in its place, a delete a closing version, and a
replay nothing twice.
"""
from mage_ai.streaming.sinks.postgres import PostgresSink


def change(id_, price, op, at):
    return dict(id=id_, price=price, _mage_operation=op, _mage_commit_time=at)


def test_versions_end_where_the_next_starts_and_a_replay_adds_nothing(
    postgres_settings, pg, schema,
):
    sink = PostgresSink(dict(
        connector_type='postgres',
        host=postgres_settings['host'],
        port=postgres_settings['port'],
        database=postgres_settings['dbname'],
        username=postgres_settings['user'],
        password=postgres_settings['password'],
        schema=schema,
        table='products_history',
        history=dict(key='id'),
    ))
    try:
        first = [
            change(1, 10.5, 'insert', '2026-11-01T00:00:00Z'),
            change(2, 3, 'insert', '2026-11-01T00:00:00Z'),
            change(1, 12, 'update', '2026-11-03T00:00:00Z'),
        ]
        sink.batch_write(first)
        sink.batch_write(first)
        sink.batch_write([
            change(1, 11, 'update', '2026-11-02T00:00:00Z'),
            change(2, 3, 'delete', '2026-11-04T00:00:00Z'),
        ])
    finally:
        sink.destroy()

    with pg.cursor() as cursor:
        cursor.execute(
            f"SELECT id, price, to_char(valid_from, 'MM-DD'), "
            f"coalesce(to_char(valid_to, 'MM-DD'), '-'), _mage_deleted "
            f'FROM {schema}.products_history ORDER BY id, valid_from'
        )
        rows = [(i, float(p), f, t, d) for i, p, f, t, d in cursor.fetchall()]
        cursor.execute(
            f"SELECT to_char(through, 'MM-DD') FROM {schema}.mageml_history_progress "
            'WHERE history_table = %s', (f'{schema}.products_history',),
        )
        through = cursor.fetchone()[0]
    pg.rollback()
    assert rows == [
        (1, 10.5, '11-01', '11-02', False),
        (1, 11.0, '11-02', '11-03', False),
        (1, 12.0, '11-03', '-', False),
        (2, 3.0, '11-01', '11-04', False),
        (2, 3.0, '11-04', '-', True),
    ]
    assert through == '11-04'
