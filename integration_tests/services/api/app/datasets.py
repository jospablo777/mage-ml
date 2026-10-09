"""
Deterministic datasets served by the test API.

The tests import this module to build the values they expect, so it depends only on
Polars and the standard library.
"""
import datetime as dt
import decimal

import polars as pl

UTC = dt.timezone.utc

# Values that break naive serializers: limits, empty strings next to NULL, CSV and JSON
# special characters, and scripts in several languages.
EDGE_ROWS = [
    dict(
        id=1,
        big_id=2**62 + 1,
        small=-32768,
        amount=0.1,
        price=decimal.Decimal('12345678901234.5678'),
        flag=True,
        name='plain',
        day=dt.date(2024, 1, 1),
        created_at=dt.datetime(2024, 1, 1, 0, 0, 0, 123456, tzinfo=UTC),
        local_time=dt.datetime(2024, 1, 1, 8, 30),
        tags=['a', 'b'],
        payload=b'\x00\xff\x00',
    ),
    dict(
        id=2,
        big_id=-(2**63),
        small=32767,
        amount=-1.5e300,
        price=decimal.Decimal('-0.0001'),
        flag=False,
        name='',
        day=dt.date(1970, 1, 1),
        created_at=dt.datetime(1970, 1, 1, tzinfo=UTC),
        local_time=dt.datetime(1999, 12, 31, 23, 59, 59, 999999),
        tags=[],
        payload=b'',
    ),
    dict(
        id=3,
        big_id=None,
        small=None,
        amount=None,
        price=None,
        flag=None,
        name=None,
        day=None,
        created_at=None,
        local_time=None,
        tags=None,
        payload=None,
    ),
    dict(
        id=4,
        big_id=9007199254740993,
        small=0,
        amount=1e-300,
        price=decimal.Decimal('0'),
        flag=True,
        name='comma, "quote", new\nline\ttab \\ backslash',
        day=dt.date(2099, 12, 31),
        created_at=dt.datetime(2038, 1, 19, 3, 14, 8, tzinfo=UTC),
        local_time=dt.datetime(2024, 2, 29, 12, 0),
        tags=['with, comma', '"quoted"', ''],
        payload=b'\n\r\t',
    ),
    dict(
        id=5,
        big_id=2**63 - 1,
        small=1,
        amount=123456789.123456,
        price=decimal.Decimal('99999999999999.9999'),
        flag=False,
        name='ñandú 中文 العربية Ελληνικά 🐍',
        day=dt.date(2000, 2, 29),
        created_at=dt.datetime(2024, 6, 30, 23, 59, 59, 999999, tzinfo=UTC),
        local_time=dt.datetime(2024, 6, 30, 23, 59, 59),
        tags=['ñ', '🐍'],
        payload='ñ'.encode(),
    ),
]

SCHEMA = {
    'id': pl.Int64,
    'big_id': pl.Int64,
    'small': pl.Int16,
    'amount': pl.Float64,
    'price': pl.Decimal(18, 4),
    'flag': pl.Boolean,
    'name': pl.String,
    'day': pl.Date,
    'created_at': pl.Datetime('us', 'UTC'),
    'local_time': pl.Datetime('us'),
    'tags': pl.List(pl.String),
    'payload': pl.Binary,
}

# Columns each format can carry without a convention of its own. CSV and Excel have no
# lists or bytes.
FLAT_COLUMNS = [c for c in SCHEMA if c not in ('tags', 'payload')]
DATASETS = ('orders',)


def _generated_row(index: int) -> dict:
    # A linear congruential sequence keeps the values the same on every platform.
    value = (index * 2654435761) % 2**32
    return dict(
        id=index,
        big_id=value * 2**30 + index,
        small=(value % 65536) - 32768,
        amount=round((value % 1_000_000) / 100, 2),
        price=decimal.Decimal(value % 10**12) / decimal.Decimal(10_000),
        flag=value % 3 == 0 if value % 7 else None,
        name=f'customer {value % 997}' if value % 11 else None,
        day=dt.date(2020, 1, 1) + dt.timedelta(days=value % 2000),
        created_at=dt.datetime(2020, 1, 1, tzinfo=UTC)
        + dt.timedelta(microseconds=value * 997),
        local_time=dt.datetime(2020, 1, 1) + dt.timedelta(seconds=value % 10**8),
        tags=[f't{value % 5}'] * (value % 4),
        payload=value.to_bytes(4, 'big'),
    )


def orders(rows: int = 50) -> pl.DataFrame:
    """
    The edge rows followed by generated rows, rows in total (at least the edge rows).
    """
    records = EDGE_ROWS + [_generated_row(i) for i in range(len(EDGE_ROWS) + 1, rows + 1)]
    return pl.DataFrame(records[: max(rows, len(EDGE_ROWS))], schema=SCHEMA)


def dataset(name: str, rows: int = 50) -> pl.DataFrame:
    if name == 'orders':
        return orders(rows)
    raise KeyError(name)
