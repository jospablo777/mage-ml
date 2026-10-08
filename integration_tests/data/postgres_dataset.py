"""
Source data for the PostgreSQL integration tests.

The source table covers the column types Mage reads and writes, including arrays,
JSON, an enum and a two-dimensional array. Rows are hand-written edge cases followed
by rows from Faker with a fixed seed. Rows are inserted through psycopg2 adapters, so
the source data does not depend on the Mage code under test.
"""

import datetime
import decimal
import random
import uuid
from typing import Dict, List, Optional, Tuple

import numpy as np
import psycopg2
from faker import Faker
from psycopg2.extensions import UNICODE, new_array_type, register_type
from psycopg2.extras import Json, execute_values

SEED = 20261008
# psycopg2 returns uuid[] as the array literal unless a reader is registered.
UUID_ARRAY = new_array_type((2951,), 'UUID[]', UNICODE)
FAKER_LOCALES = ['en_US', 'ja_JP', 'ar_EG', 'de_DE', 'pt_BR', 'zh_CN', 'ru_RU', 'he_IL']
ENUM_LABELS = ['sad', 'ok', 'happy']

# (column, PostgreSQL type). {schema} is replaced with the test schema for the enum.
COLUMNS: List[Tuple[str, str]] = [
    ('id', 'integer'),
    ('c_smallint', 'smallint'),
    ('c_integer', 'integer'),
    ('c_bigint', 'bigint'),
    ('c_numeric', 'numeric(38, 10)'),
    ('c_numeric_free', 'numeric'),
    ('c_real', 'real'),
    ('c_double', 'double precision'),
    ('c_bool', 'boolean'),
    ('c_text', 'text'),
    ('c_varchar', 'varchar(64)'),
    ('c_char', 'char(5)'),
    ('c_bytea', 'bytea'),
    ('c_date', 'date'),
    ('c_time', 'time'),
    ('c_timetz', 'timetz'),
    ('c_timestamp', 'timestamp'),
    ('c_timestamptz', 'timestamptz'),
    ('c_interval', 'interval'),
    ('c_uuid', 'uuid'),
    ('c_json', 'json'),
    ('c_jsonb', 'jsonb'),
    ('c_int_array', 'integer[]'),
    ('c_bigint_array', 'bigint[]'),
    ('c_float_array', 'double precision[]'),
    ('c_bool_array', 'boolean[]'),
    ('c_text_array', 'text[]'),
    ('c_numeric_array', 'numeric[]'),
    ('c_date_array', 'date[]'),
    ('c_uuid_array', 'uuid[]'),
    ('c_int_matrix', 'integer[][]'),
    ('c_enum', '{schema}.mood'),
]
COLUMN_NAMES = [name for name, _ in COLUMNS]
JSON_COLUMNS = {'c_json', 'c_jsonb'}

UNICODE_TEXT = (
    'ñandú 🦊 中文 日本語 한국어 العربية עברית Ελληνικά кириллица '
    'é (combining) ​(zero width) 𝔘𝔫𝔦𝔠𝔬𝔡𝔢'
)
ESCAPES_TEXT = 'tab\there, newline\nhere, cr\rhere, backslash \\ and "quotes" and \'single\''


def _row(**values) -> Dict:
    row = {name: None for name in COLUMN_NAMES}
    row.update(values)
    return row


def edge_rows() -> List[Dict]:
    """
    Boundaries and encodings Faker does not produce.
    """
    return [
        _row(
            id=1,
            c_smallint=32767,
            c_integer=2147483647,
            c_bigint=2**53 + 1,
            c_numeric=decimal.Decimal('1234567890123456789012345678.0123456789'),
            c_numeric_free=decimal.Decimal('123456789012345678901234567890123456789012345.6789'),
            c_real=1.5,
            c_double=0.1,
            c_bool=True,
            c_text=UNICODE_TEXT,
            c_varchar=ESCAPES_TEXT[:64],
            c_char='ab',
            c_bytea=b'\x00\xff\x10binary\x00',
            c_date=datetime.date(2024, 2, 29),
            c_time=datetime.time(23, 59, 59, 999999),
            c_timetz=datetime.time(
                12, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30))
            ),
            c_timestamp=datetime.datetime(2024, 1, 1, 12, 34, 56, 789012),
            c_timestamptz=datetime.datetime(
                2024,
                6,
                1,
                8,
                0,
                0,
                123456,
                tzinfo=datetime.timezone(datetime.timedelta(hours=-6)),
            ),
            c_interval=datetime.timedelta(days=1, hours=2, minutes=3, seconds=4.5),
            c_uuid='0b9e1f3c-5a2d-4c7e-9f10-2b3c4d5e6f70',
            c_json={'a': [1, 2.5, None], 'b': 'x', 'unicode': UNICODE_TEXT, 'esc': ESCAPES_TEXT},
            c_jsonb={'nested': {'list': [{'k': 1}, {'k': 2}]}, 't': True, 'n': None, 'f': -0.5},
            c_int_array=[1, None, 3],
            c_bigint_array=[2**62, -(2**62)],
            c_float_array=[1.5, None, -0.0, 1e-300],
            c_bool_array=[True, False, None],
            c_text_array=[
                'plain',
                'with "quotes"',
                'with,comma',
                'with {braces}',
                'back\\slash',
                None,
                'NULL',
                '',
            ],
            c_numeric_array=[decimal.Decimal('1.10'), None, decimal.Decimal('-99999999.000001')],
            c_date_array=[datetime.date(2000, 1, 1), None],
            c_uuid_array=['11111111-2222-3333-4444-555555555555', None],
            c_int_matrix=[[1, 2], [3, 4]],
            c_enum='happy',
        ),
        _row(
            id=2,
            c_smallint=-32768,
            c_integer=-2147483648,
            c_bigint=-(2**63),
            c_numeric=decimal.Decimal('-0.0000000001'),
            c_numeric_free=decimal.Decimal('0'),
            c_real=float('nan'),
            c_double=float('inf'),
            c_bool=False,
            c_text='',
            c_varchar='\\N',
            c_char='',
            c_bytea=b'',
            c_date=datetime.date(1, 1, 1),
            c_time=datetime.time(0, 0),
            c_timetz=datetime.time(0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=-12))),
            c_timestamp=datetime.datetime(1970, 1, 1),
            c_timestamptz=datetime.datetime(
                1999,
                12,
                31,
                23,
                59,
                59,
                tzinfo=datetime.timezone(datetime.timedelta(hours=14)),
            ),
            c_interval=datetime.timedelta(days=-90),
            c_uuid='ffffffff-ffff-ffff-ffff-ffffffffffff',
            c_json=[],
            c_jsonb={},
            c_int_array=[],
            c_bigint_array=[],
            c_float_array=[],
            c_bool_array=[],
            c_text_array=[],
            c_numeric_array=[],
            c_date_array=[],
            c_uuid_array=[],
            c_int_matrix=[],
            c_enum='sad',
        ),
        # Every column NULL except the key.
        _row(id=3),
        _row(
            id=4,
            c_smallint=0,
            c_integer=0,
            c_bigint=-1,
            c_numeric=decimal.Decimal('0.0000000000'),
            c_numeric_free=decimal.Decimal('NaN'),
            c_real=float('-inf'),
            c_double=-0.0,
            c_text='NULL',
            c_varchar='None',
            c_char='nan',
            c_bytea='\\x00'.encode(),
            c_date=datetime.date(9999, 12, 31),
            c_time=datetime.time(12, 0, 0, 1),
            c_timestamp=datetime.datetime(2262, 4, 12, 0, 0, 0, 1),
            c_timestamptz=datetime.datetime(
                9999, 12, 31, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc
            ),
            c_interval=datetime.timedelta(microseconds=1),
            c_json='just a string',
            c_jsonb=[1, 'two', None, {'three': 3}],
            c_text_array=['', ' ', '\t', '\n'],
            c_enum='ok',
        ),
        _row(
            id=5,
            c_bigint=9223372036854775807,
            c_numeric=decimal.Decimal('9999999999999999999999999999.9999999999'),
            c_numeric_free=decimal.Decimal('-1E-50'),
            c_real=3.4028234663852886e38,
            c_double=1.7976931348623157e308,
            c_text='{"looks": "like json"}',
            c_varchar='SGVsbG8gd29ybGQ=',
            c_char='12345',
            c_timestamp=datetime.datetime(1, 1, 1, 0, 0, 0),
            c_interval=datetime.timedelta(days=36500, seconds=1),
            c_float_array=[5e-324, -1.7976931348623157e308],
            c_text_array=[UNICODE_TEXT, ESCAPES_TEXT],
            c_json={'deep': [[[[[1]]]]], 'big': 12345678901234567890},
        ),
        _row(
            id=6,
            c_double=5e-324,
            c_real=-1.401298464324817e-45,
            c_text=ESCAPES_TEXT,
            c_varchar=UNICODE_TEXT[:64],
        ),
    ]


def _maybe(fake: Faker, value, null_rate: float = 0.1):
    return None if fake.random.random() < null_rate else value


def _real(fake: Faker) -> float:
    # A float32 value, so it round-trips through a real column exactly.
    return float(np.float32(fake.pyfloat(min_value=-1e6, max_value=1e6)))


def faker_rows(count: int, start_id: int = 1000, seed: int = SEED) -> List[Dict]:
    fake = Faker(FAKER_LOCALES)
    Faker.seed(seed)
    fake.seed_instance(seed)
    rng = random.Random(seed)
    rows = []
    for offset in range(count):
        local = fake[rng.choice(FAKER_LOCALES)]
        timezone = datetime.timezone(
            datetime.timedelta(minutes=rng.randrange(-12 * 60, 14 * 60, 15))
        )
        timestamp = local.date_time_between('-100y', '+30y').replace(
            microsecond=rng.randrange(10**6)
        )
        rows.append(
            _row(
                id=start_id + offset,
                c_smallint=_maybe(local, rng.randint(-32768, 32767)),
                c_integer=_maybe(local, rng.randint(-(2**31), 2**31 - 1)),
                c_bigint=_maybe(local, rng.randint(-(2**63), 2**63 - 1)),
                c_numeric=_maybe(local, local.pydecimal(left_digits=27, right_digits=10)),
                c_numeric_free=_maybe(local, local.pydecimal(left_digits=30, right_digits=15)),
                c_real=_maybe(local, _real(local)),
                c_double=_maybe(local, local.pyfloat()),
                c_bool=_maybe(local, local.pybool()),
                c_text=_maybe(local, local.text(max_nb_chars=200)),
                c_varchar=_maybe(local, local.name()[:64]),
                c_char=_maybe(local, local.lexify('?????')),
                c_bytea=_maybe(local, local.binary(length=rng.randint(0, 64))),
                c_date=_maybe(local, local.date_between('-120y', '+30y')),
                c_time=_maybe(local, local.time_object().replace(microsecond=rng.randrange(10**6))),
                c_timetz=_maybe(local, local.time_object().replace(tzinfo=timezone)),
                c_timestamp=_maybe(local, timestamp),
                c_timestamptz=_maybe(local, timestamp.replace(tzinfo=timezone)),
                c_interval=_maybe(
                    local,
                    datetime.timedelta(
                        days=rng.randint(-10000, 10000),
                        seconds=rng.randint(0, 86399),
                        microseconds=rng.randrange(10**6),
                    ),
                ),
                c_uuid=_maybe(local, str(uuid.UUID(int=rng.getrandbits(128)))),
                c_json=_maybe(
                    local, local.pydict(nb_elements=4, value_types=[str, int, float, bool])
                ),
                c_jsonb=_maybe(
                    local,
                    {
                        'name': local.name(),
                        'address': local.address(),
                        'tags': local.words(nb=3),
                        'score': local.pyfloat(),
                        'nested': {'when': timestamp.isoformat(), 'ok': local.pybool()},
                    },
                ),
                c_int_array=_maybe(
                    local,
                    [
                        _maybe(local, rng.randint(-(2**31), 2**31 - 1), 0.2)
                        for _ in range(rng.randint(0, 5))
                    ],
                ),
                c_bigint_array=_maybe(
                    local, [rng.randint(-(2**63), 2**63 - 1) for _ in range(rng.randint(0, 4))]
                ),
                c_float_array=_maybe(local, [local.pyfloat() for _ in range(rng.randint(0, 4))]),
                c_bool_array=_maybe(local, [local.pybool() for _ in range(rng.randint(0, 4))]),
                c_text_array=_maybe(
                    local, [_maybe(local, local.word(), 0.2) for _ in range(rng.randint(0, 4))]
                ),
                c_numeric_array=_maybe(
                    local,
                    [
                        local.pydecimal(left_digits=8, right_digits=6)
                        for _ in range(rng.randint(0, 3))
                    ],
                ),
                c_date_array=_maybe(
                    local, [local.date_between('-50y', '+5y') for _ in range(rng.randint(0, 3))]
                ),
                c_uuid_array=_maybe(
                    local,
                    [str(uuid.UUID(int=rng.getrandbits(128))) for _ in range(rng.randint(0, 3))],
                ),
                c_int_matrix=_maybe(
                    local, [[rng.randint(-100, 100) for _ in range(2)] for _ in range(2)]
                ),
                c_enum=_maybe(local, rng.choice(ENUM_LABELS)),
            )
        )
    return rows


def column_types(schema: str) -> List[Tuple[str, str]]:
    return [(name, pg_type.format(schema=schema)) for name, pg_type in COLUMNS]


def create_source_table(connection, schema: str, table: str, rows: List[Dict]) -> None:
    """
    Create the enum and the source table in schema and insert rows with psycopg2 adapters.
    """
    types = column_types(schema)
    labels = ', '.join(f"'{label}'" for label in ENUM_LABELS)
    definitions = ',\n'.join(
        f'"{name}" {pg_type}' + (' PRIMARY KEY' if name == 'id' else '') for name, pg_type in types
    )
    with connection.cursor() as cursor:
        cursor.execute(f'CREATE TYPE {schema}.mood AS ENUM ({labels})')
        cursor.execute(f'CREATE TABLE {schema}.{table} ({definitions})')
        # (%s)::type, because -32768::smallint parses as -(32768::smallint).
        template = '(' + ', '.join(f'(%s)::{pg_type}' for _, pg_type in types) + ')'
        values = [tuple(_adapt(row[name], name) for name in COLUMN_NAMES) for row in rows]
        execute_values(
            cursor,
            f'INSERT INTO {schema}.{table} VALUES %s',
            values,
            template=template,
            page_size=500,
        )
    connection.commit()


def _adapt(value, column: str):
    if value is None:
        return None
    if column in JSON_COLUMNS:
        return Json(value)
    if isinstance(value, bytes):
        return psycopg2.Binary(value)
    return value


def create_like_table(connection, schema: str, source: str, target: str) -> None:
    """
    Create an empty table with the column types and primary key of source.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            f'CREATE TABLE {schema}.{target} (LIKE {schema}.{source} INCLUDING ALL)',
        )
    connection.commit()


def _comparable(column: str, source_type: str, target_type: Optional[str], alias: str) -> str:
    """
    SQL for a column value made comparable with the source column.
    """
    expression = f'{alias}."{column}"'
    if alias == 's':
        if column in JSON_COLUMNS:
            return f'{expression}::jsonb'
        if column == 'c_int_matrix' and target_type == 'jsonb':
            return f'to_jsonb({expression})'
        if _integer_into_float(source_type, target_type):
            return f'{expression}::text'
        return expression
    if target_type is None:
        return expression
    if column in JSON_COLUMNS:
        return f'{expression}::jsonb'
    if column == 'c_int_matrix' and target_type == 'jsonb':
        return expression
    if _integer_into_float(source_type, target_type):
        return f'{expression}::text'
    if target_type != source_type:
        return f'{expression}::{source_type}'
    return expression


def _integer_into_float(source_type: Optional[str], target_type: Optional[str]) -> bool:
    """
    An integer column written into a float column. Casting the float back can overflow
    the integer type, so both sides compare as text, which still shows lost precision.
    """
    return source_type in ('smallint', 'integer', 'bigint') and target_type in (
        'double precision',
        'real',
    )


def mismatches(
    connection,
    schema: str,
    source: str,
    target: str,
    columns: Optional[List[str]] = None,
    limit: int = 5,
    where: str = 'true',
) -> Dict[str, List]:
    """
    Compare target with source, column by column, matching rows on id.

    Returns {column: [(id, source value, target value), ...]} for columns that differ,
    plus '<rows>' for ids present in only one table. IS DISTINCT FROM treats two NULLs
    and two NaNs as equal. JSON is compared as jsonb. A target column of another type is
    cast to the source type, so text written for a uuid or enum column compares as such.
    """
    columns = columns or COLUMN_NAMES
    source_types = dict(_format_types(connection, schema, source))
    target_types = dict(_format_types(connection, schema, target))
    problems = {}
    with connection.cursor() as cursor:
        cursor.execute(
            f'SELECT array_agg(id ORDER BY id) FROM ('
            f'(SELECT id FROM {schema}.{source} WHERE {where} '
            f'EXCEPT SELECT id FROM {schema}.{target}) UNION ALL '
            f'(SELECT id FROM {schema}.{target} EXCEPT SELECT id FROM {schema}.{source} '
            f'WHERE {where})) ids',
        )
        missing = cursor.fetchone()[0]
        if missing:
            problems['<rows>'] = missing[:limit]
        for column in columns:
            if column == 'id':
                continue
            source_type = source_types[column]
            target_type = target_types.get(column)
            if target_type is None:
                problems[column] = ['<missing column>']
                continue
            left = _comparable(column, source_type, target_type, 's')
            right = _comparable(column, source_type, target_type, 't')
            cursor.execute(
                f'SELECT s.id, {left}::text, {right}::text '
                f'FROM {schema}.{source} s JOIN {schema}.{target} t USING (id) '
                f'WHERE {left} IS DISTINCT FROM {right} ORDER BY s.id LIMIT {limit}',
            )
            rows = cursor.fetchall()
            if rows:
                problems[column] = rows
    connection.rollback()
    return problems


def _format_types(connection, schema: str, table: str) -> List[Tuple[str, str]]:
    with connection.cursor() as cursor:
        cursor.execute(
            'SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute '
            'WHERE attrelid = to_regclass(%s) AND attnum > 0 AND NOT attisdropped '
            'ORDER BY attnum',
            (f'{schema}.{table}',),
        )
        return cursor.fetchall()


def fetch_rows(connection, schema: str, table: str) -> List[Dict]:
    with connection.cursor() as cursor:
        register_type(UUID_ARRAY, cursor)
        cursor.execute(f'SELECT * FROM {schema}.{table} ORDER BY id')
        names = [d.name for d in cursor.description]
        rows = [dict(zip(names, row)) for row in cursor.fetchall()]
    connection.rollback()
    return rows
