"""
The ClickHouse destination of mage_integrations against ClickHouse 25.8, run as a program
the way Mage runs it, on Singer messages.
"""
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

DESTINATION = (
    Path(__file__).resolve().parents[2]
    / 'mage_integrations' / 'mage_integrations' / 'destinations' / 'clickhouse' / '__init__.py'
)

SCHEMA = {
    'type': 'SCHEMA',
    'stream': 'orders',
    'key_properties': ['id'],
    'schema': {'properties': {
        'id': {'type': ['integer']},
        'big': {'type': ['null', 'integer']},
        'price': {'type': ['null', 'number']},
        'text': {'type': ['null', 'string']},
        'flag': {'type': ['null', 'boolean']},
        'at': {'type': ['null', 'string'], 'format': 'date-time'},
        'day': {'type': ['null', 'string'], 'format': 'date'},
    }},
}
RECORDS = [
    {'id': 1, 'big': 2**53 + 1, 'price': 1.5, 'text': 'ñ "q"', 'flag': True,
     'at': '2024-01-01T12:00:00.123456+00:00', 'day': '2024-02-29'},
    {'id': 2, 'big': None, 'price': None, 'text': None, 'flag': None, 'at': None, 'day': None},
]


def run_destination(clickhouse_settings, database, tmp_path, messages):
    input_path = tmp_path / 'input.jsonl'
    input_path.write_text('\n'.join(json.dumps(m) for m in messages) + '\n')
    settings = clickhouse_settings
    config = json.dumps(dict(
        sqlalchemy_url=(
            f"clickhouse+http://{settings['username']}:{settings['password']}"
            f"@{settings['host']}:{settings['port']}/{database}"
        ),
        table_name='orders',
    ))
    result = subprocess.run(
        [sys.executable, str(DESTINATION), '--config_json', config,
         '--input_file_path', str(input_path), '--state', str(tmp_path / 'state.json')],
        capture_output=True, text=True, timeout=180,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    return result


def column_types(ch, table):
    return dict(ch.query(
        'SELECT name, type FROM system.columns WHERE database = currentDatabase() '
        'AND table = {table:String}',
        parameters=dict(table=table),
    ).result_rows)


def test_records_reach_the_table(clickhouse_settings, clickhouse_database, ch, tmp_path):
    """
    The destination failed on its first SCHEMA message, since SQLAlchemy 2 removed
    MetaData's bind. Once it ran, integers were Int32, which wrapped 2**53 + 1 to 1, NULL
    became 0, '' or 1970-01-01 in columns that were not Nullable, and date-times lost
    their microseconds.
    """
    messages = [SCHEMA] + [{'type': 'RECORD', 'stream': 'orders', 'record': r} for r in RECORDS]

    run_destination(clickhouse_settings, clickhouse_database, tmp_path, messages)

    rows = ch.query(
        'SELECT id, big, price, text, flag, at, day FROM orders ORDER BY id',
    ).result_rows
    assert rows == [
        (1, 2**53 + 1, 1.5, 'ñ "q"', True, dt.datetime(2024, 1, 1, 12, 0, 0, 123456),
         dt.date(2024, 2, 29)),
        (2, None, None, None, None, None, None),
    ]
    assert column_types(ch, 'orders') == {
        'id': 'Int64', 'big': 'Nullable(Int64)', 'price': 'Nullable(Float64)',
        'text': 'Nullable(String)', 'flag': 'Nullable(Bool)',
        'at': "Nullable(DateTime64(6, 'UTC'))", 'day': 'Nullable(Date32)',
    }


def test_appends_to_the_table(clickhouse_settings, clickhouse_database, ch, tmp_path):
    messages = [SCHEMA] + [{'type': 'RECORD', 'stream': 'orders', 'record': r} for r in RECORDS]

    run_destination(clickhouse_settings, clickhouse_database, tmp_path, messages)
    run_destination(clickhouse_settings, clickhouse_database, tmp_path, messages)

    assert ch.query('SELECT count() FROM orders').result_rows == [(4,)]
