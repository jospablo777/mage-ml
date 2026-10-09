"""
A Polars frame with the Polars types for the SQL block tests, with edge values and
nulls, and the values PostgreSQL stores for each column.
"""
import datetime as dt
import decimal
import math
import random
from typing import Dict, List

import polars as pl

SEED = 20261010
WORDS = ['alpha', 'ñandú', 'Straße', '東京', '', 'NULL', 'tab\there', 'quote "q" \'s\'']


def _rows(count: int) -> List[Dict]:
    rng = random.Random(SEED)
    edge = [
        dict(
            i8=-128, i16=-32768, i32=-(2**31), i64=-(2**63), u8=255, u16=65535,
            u32=2**32 - 1, u64=2**64 - 1, i128=-(2**127), f32=1.5, f64=math.inf,
            flag=True, text='ñ 🦀', day=dt.date(1, 1, 1),
            at=dt.datetime(1, 1, 1, 0, 0, 0, 1), at_ns=dt.datetime(2024, 1, 1, 12, 0, 0, 123456),
            zoned=dt.datetime(2024, 7, 1, 12, tzinfo=dt.timezone.utc),
            span=dt.timedelta(days=-1, microseconds=1), clock=dt.time(23, 59, 59, 999999),
            amount=decimal.Decimal('-1234567890123456789012345678.0123456789'),
            ints=[1, None, -3], words=['a', None], pair=[1, 2],
            record={'a': 1, 'b': 'x'}, raw=b'\x00\xff', mood='happy',
        ),
        dict(
            i8=127, i16=32767, i32=2**31 - 1, i64=2**63 - 1, u8=0, u16=0, u32=0, u64=0,
            i128=2**127 - 1, f32=-0.0, f64=-math.inf, flag=False, text='',
            day=dt.date(9999, 12, 31), at=dt.datetime(9999, 12, 31, 23, 59, 59, 999999),
            at_ns=dt.datetime(1969, 12, 31, 23, 59, 59, 999999),
            zoned=dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc),
            span=dt.timedelta(days=10000), clock=dt.time(0, 0),
            amount=decimal.Decimal('0.0000000001'), ints=[], words=[], pair=[-1, 0],
            record={'a': None, 'b': None}, raw=b'', mood='sad',
        ),
        {name: None for name in [
            'i8', 'i16', 'i32', 'i64', 'u8', 'u16', 'u32', 'u64', 'i128', 'f32', 'f64',
            'flag', 'text', 'day', 'at', 'at_ns', 'zoned', 'span', 'clock', 'amount',
            'ints', 'words', 'pair', 'record', 'raw', 'mood',
        ]},
        dict(
            i8=0, i16=0, i32=0, i64=0, u8=1, u16=1, u32=1, u64=2**63, i128=2**64,
            f32=None, f64=math.nan, flag=None, text='NULL', day=dt.date(2024, 2, 29),
            at=dt.datetime(2000, 1, 1), at_ns=None, zoned=None, span=dt.timedelta(0),
            clock=None, amount=decimal.Decimal('0'), ints=[None], words=['NULL'],
            pair=None, record=None, raw=None, mood=None,
        ),
    ]
    rows = []
    for _ in range(count):
        length = rng.randint(0, 3)
        rows.append(dict(
            i8=rng.randint(-128, 127), i16=rng.randint(-32768, 32767),
            i32=rng.randint(-(2**31), 2**31 - 1), i64=rng.randint(-(2**63), 2**63 - 1),
            u8=rng.randint(0, 255), u16=rng.randint(0, 65535), u32=rng.randint(0, 2**32 - 1),
            u64=rng.randint(0, 2**64 - 1), i128=rng.randint(-(2**100), 2**100),
            f32=float(rng.randint(-1000, 1000)) / 4, f64=rng.uniform(-1e9, 1e9),
            flag=rng.random() < 0.5, text=rng.choice(WORDS),
            day=dt.date(1900, 1, 1) + dt.timedelta(days=rng.randint(0, 80000)),
            at=dt.datetime(1900, 1, 1) + dt.timedelta(
                seconds=rng.randint(0, 6 * 10**9), microseconds=rng.randint(0, 999999)),
            at_ns=dt.datetime(1970, 1, 1) + dt.timedelta(
                seconds=rng.randint(0, 2 * 10**9), microseconds=rng.randint(0, 999999)),
            zoned=dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(
                seconds=rng.randint(0, 10**9)),
            span=dt.timedelta(microseconds=rng.randint(-10**13, 10**13)),
            clock=dt.time(rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59),
                          rng.randint(0, 999999)),
            amount=decimal.Decimal(rng.randint(-10**20, 10**20)) / decimal.Decimal(10**10),
            ints=[rng.randint(-100, 100) for _ in range(length)],
            words=[rng.choice(WORDS) for _ in range(length)],
            pair=[rng.randint(-5, 5), rng.randint(-5, 5)],
            record={'a': rng.randint(0, 9), 'b': rng.choice(WORDS)},
            raw=bytes(rng.randint(0, 255) for _ in range(length)),
            mood=rng.choice(['sad', 'ok', 'happy']),
        ))
    return [dict(id=i, **row) for i, row in enumerate(edge + rows, start=1)]


def polars_frame(count: int = 200) -> pl.DataFrame:
    rows = _rows(count)

    def column(name):
        return [row[name] for row in rows]

    return pl.DataFrame([
        pl.Series('id', column('id'), dtype=pl.Int64),
        pl.Series('i8', column('i8'), dtype=pl.Int8),
        pl.Series('i16', column('i16'), dtype=pl.Int16),
        pl.Series('i32', column('i32'), dtype=pl.Int32),
        pl.Series('i64', column('i64'), dtype=pl.Int64),
        pl.Series('u8', column('u8'), dtype=pl.UInt8),
        pl.Series('u16', column('u16'), dtype=pl.UInt16),
        pl.Series('u32', column('u32'), dtype=pl.UInt32),
        pl.Series('u64', column('u64'), dtype=pl.UInt64),
        pl.Series('i128', column('i128'), dtype=pl.Int128),
        pl.Series('f32', column('f32'), dtype=pl.Float32),
        pl.Series('f64', column('f64'), dtype=pl.Float64),
        pl.Series('flag', column('flag'), dtype=pl.Boolean),
        pl.Series('text', column('text'), dtype=pl.String),
        pl.Series('day', column('day'), dtype=pl.Date),
        pl.Series('at', column('at'), dtype=pl.Datetime('us')),
        pl.Series('at_ns', column('at_ns'), dtype=pl.Datetime('ns')),
        pl.Series('zoned', column('zoned'), dtype=pl.Datetime('us', 'UTC'))
        .dt.convert_time_zone('America/New_York'),
        pl.Series('span', column('span'), dtype=pl.Duration('us')),
        pl.Series('clock', column('clock'), dtype=pl.Time),
        pl.Series('amount', column('amount'), dtype=pl.Decimal(38, 10)),
        pl.Series('ints', column('ints'), dtype=pl.List(pl.Int64)),
        pl.Series('words', column('words'), dtype=pl.List(pl.String)),
        pl.Series('pair', column('pair'), dtype=pl.Array(pl.Int32, 2)),
        pl.Series('record', column('record'), dtype=pl.Struct({'a': pl.Int64, 'b': pl.String})),
        pl.Series('raw', column('raw'), dtype=pl.Binary),
        pl.Series('mood', column('mood'), dtype=pl.Enum(['sad', 'ok', 'happy'])),
    ])


def expected_rows(count: int = 200) -> List[Dict]:
    """The values PostgreSQL stores, as psycopg2 returns them."""
    result = []
    for row in _rows(count):
        row = dict(row)
        # Time zones are stored as instants; psycopg2 returns them in UTC here.
        if row['zoned'] is not None:
            row['zoned'] = row['zoned'].astimezone(dt.timezone.utc)
        if isinstance(row['f64'], float) and math.isnan(row['f64']):
            row['f64'] = math.nan
        result.append(row)
    return result
