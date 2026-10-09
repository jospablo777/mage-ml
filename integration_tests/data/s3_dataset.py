"""
A Polars frame with one column per type that Parquet stores, for the S3 tests.

Rows 1 to 4 hold limits and special values, row 5 is null in every column, and rows from
6 on are generated with a fixed formula.
"""
import datetime as dt
import decimal
from typing import Dict

import polars as pl

SCHEMA = {
    'id': pl.Int64,
    'i8': pl.Int8,
    'i16': pl.Int16,
    'i32': pl.Int32,
    'i64': pl.Int64,
    'u8': pl.UInt8,
    'u16': pl.UInt16,
    'u32': pl.UInt32,
    'u64': pl.UInt64,
    'f32': pl.Float32,
    'f64': pl.Float64,
    'dec': pl.Decimal(38, 10),
    'flag': pl.Boolean,
    'text': pl.String,
    'raw': pl.Binary,
    'day': pl.Date,
    'clock': pl.Time,
    'ts_ms': pl.Datetime('ms'),
    'ts_us': pl.Datetime('us'),
    'ts_ns': pl.Datetime('ns'),
    'ts_utc': pl.Datetime('us', 'UTC'),
    'ts_zone': pl.Datetime('us', 'America/New_York'),
    'span': pl.Duration('us'),
    'ints': pl.List(pl.Int64),
    'texts': pl.List(pl.String),
    'pair': pl.Array(pl.Int32, 2),
    'record': pl.Struct({'a': pl.Int64, 'b': pl.String}),
    'level': pl.Enum(['low', 'mid', 'high']),
    'label': pl.Categorical(),
}
COLUMNS = list(SCHEMA)

UTC = dt.timezone.utc
EDGE_ROWS = [
    dict(
        id=1, i8=-128, i16=-32768, i32=-2147483648, i64=-(2**63), u8=0, u16=0, u32=0, u64=0,
        f32=float('-inf'), f64=float('-inf'),
        dec=decimal.Decimal('-9999999999999999999999999999.9999999999'), flag=False,
        text='', raw=b'', day=dt.date(1, 1, 1), clock=dt.time(0, 0),
        ts_ms=dt.datetime(1, 1, 1), ts_us=dt.datetime(1, 1, 1),
        ts_ns=dt.datetime(1678, 1, 1), ts_utc=dt.datetime(1970, 1, 1, tzinfo=UTC),
        ts_zone=dt.datetime(2024, 3, 10, 7, tzinfo=UTC), span=dt.timedelta(0),
        ints=[], texts=[], pair=[0, 0], record={'a': 0, 'b': ''}, level='low', label='a',
    ),
    dict(
        id=2, i8=127, i16=32767, i32=2147483647, i64=2**63 - 1, u8=255, u16=65535,
        u32=4294967295, u64=2**64 - 1, f32=float('inf'), f64=float('inf'),
        dec=decimal.Decimal('9999999999999999999999999999.9999999999'), flag=True,
        text='comma, "quote", new\nline\ttab \\ backslash', raw=b'\x00\xff\x00',
        day=dt.date(9999, 12, 31), clock=dt.time(23, 59, 59, 999999),
        ts_ms=dt.datetime(9999, 12, 31, 23, 59, 59, 999000),
        ts_us=dt.datetime(9999, 12, 31, 23, 59, 59, 999999),
        ts_ns=dt.datetime(2262, 4, 11, 23, 47, 16, 854775),
        ts_utc=dt.datetime(2262, 4, 11, 23, 47, 16, 854775, tzinfo=UTC),
        ts_zone=dt.datetime(2024, 11, 3, 6, tzinfo=UTC),
        span=dt.timedelta(days=999999999, seconds=86399, microseconds=999999),
        ints=[1, None, -3], texts=['a', None, ''], pair=[1, None],
        record={'a': None, 'b': None}, level='high', label='ñ',
    ),
    dict(
        id=3, i8=0, i16=0, i32=0, i64=2**53 + 1, u8=1, u16=1, u32=1, u64=2**63,
        f32=float('nan'), f64=float('nan'), dec=decimal.Decimal('0.0000000001'), flag=None,
        text='ñandú 中文 العربية Ελληνικά 🐍', raw='ñ'.encode(), day=dt.date(2000, 2, 29),
        clock=dt.time(12, 0, 0, 1), ts_ms=dt.datetime(2024, 2, 29, 12, 34, 56, 789000),
        ts_us=dt.datetime(2024, 2, 29, 12, 34, 56, 789012),
        ts_ns=dt.datetime(2024, 2, 29, 12, 34, 56, 789012),
        ts_utc=dt.datetime(2024, 2, 29, 17, 34, 56, 789012, tzinfo=UTC),
        ts_zone=dt.datetime(2024, 2, 29, 17, 34, 56, 789012, tzinfo=UTC),
        span=dt.timedelta(minutes=-90), ints=[2**62], texts=['ñ', '🐍'], pair=None,
        record={'a': -1, 'b': 'b'}, level='mid', label='',
    ),
    dict(
        id=4, i8=-1, i16=-1, i32=-1, i64=-1, u8=0, u16=0, u32=0, u64=0, f32=-0.0, f64=-0.0,
        dec=decimal.Decimal('-0.0000000001'), flag=False, text=' ', raw=b'x',
        day=dt.date(1969, 12, 31), clock=dt.time(0, 0, 0, 500000),
        ts_ms=dt.datetime(1969, 12, 31, 23, 59, 59, 999000),
        ts_us=dt.datetime(1969, 12, 31, 23, 59, 59, 999999),
        ts_ns=dt.datetime(1969, 12, 31, 23, 59, 59, 999999),
        ts_utc=dt.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
        ts_zone=dt.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
        span=dt.timedelta(microseconds=1), ints=[0], texts=['NULL'], pair=[-1, -2],
        record=None, level=None, label=None,
    ),
    {'id': 5},
]


def frame(generated: int = 2000) -> pl.DataFrame:
    edge = pl.DataFrame(
        [{column: row.get(column) for column in COLUMNS} for row in EDGE_ROWS],
        schema=SCHEMA,
        orient='row',
    )
    # Nanoseconds that a microsecond value cannot hold.
    edge = edge.with_columns(
        pl.when(pl.col('id') == 3)
        .then(pl.col('ts_ns') + pl.duration(nanoseconds=345))
        .otherwise(pl.col('ts_ns'))
        .alias('ts_ns'),
    )
    i = pl.col('i')
    generated_rows = pl.DataFrame({'i': range(generated)}, schema={'i': pl.Int64}).select(
        id=i + 6,
        i8=i % 255 - 127,
        i16=i * 7 % 65535 - 32767,
        i32=i * 2654435761 % 4294967295 - 2147483647,
        i64=i * 1000000000039 - 4611686018427387904,
        u8=i % 256,
        u16=i * 3 % 65536,
        u32=i * 2654435761 % 4294967296,
        u64=i.cast(pl.UInt64) * 1000000007 + 2**63,
        f32=i / 7,
        f64=i / 7,
        dec=i.cast(pl.Decimal(38, 10)) * decimal.Decimal('1.2345'),
        flag=pl.when(i % 3 == 0).then(None).otherwise(i % 2 == 0),
        text=pl.when(i % 5 == 0).then(None).otherwise(pl.format('row {} ñ', i)),
        raw=pl.when(i % 4 == 0).then(None).otherwise(pl.format('row{}', i).cast(pl.Binary)),
        day=pl.date(2020, 1, 1) + pl.duration(days=i % 3000),
        clock=(pl.datetime(2020, 1, 1) + pl.duration(microseconds=i * 977000123 % 86400000000))
        .dt.time(),
        ts_ms=pl.datetime(2020, 1, 1, time_unit='ms') + pl.duration(milliseconds=i * 997),
        ts_us=pl.datetime(2020, 1, 1) + pl.duration(microseconds=i * 997003),
        ts_ns=pl.datetime(2020, 1, 1, time_unit='ns') + pl.duration(nanoseconds=i * 997000013),
        ts_utc=pl.datetime(2020, 1, 1, time_zone='UTC') + pl.duration(milliseconds=i * 1013),
        # Crosses the daylight saving changes of March and November 2024.
        ts_zone=pl.datetime(2024, 3, 9, time_zone='UTC') + pl.duration(hours=i * 7),
        span=pl.duration(seconds=i * 61),
        ints=pl.when(i % 7 == 0).then(None).otherwise(pl.concat_list(i, i + 1)),
        texts=pl.when(i % 8 == 0).then(None).otherwise(pl.concat_list(pl.format('t{}', i))),
        pair=pl.concat_list(i, i * 2).list.to_array(2),
        record=pl.struct(a=i, b=pl.format('b{}', i)),
        level=pl.lit(['low', 'mid', 'high']).list.get(i % 3),
        label=pl.format('l{}', i % 50),
    )
    return pl.concat([edge, generated_rows.cast(SCHEMA)], how='vertical')


def mismatches(expected: pl.DataFrame, actual: pl.DataFrame, limit: int = 3) -> Dict:
    """
    Columns that differ between two frames, with their types or the first rows that
    differ. Rows are compared in order; NaN equals NaN.
    """
    problems = {}
    for column in expected.columns:
        if column not in actual.columns:
            problems[column] = 'missing'
            continue
        left, right = expected[column], actual[column]
        if left.dtype != right.dtype:
            problems[column] = (str(left.dtype), str(right.dtype))
        elif len(left) != len(right):
            problems[column] = ('rows', len(left), len(right))
        elif not left.equals(right):
            rows = [
                (expected['id'][i], left[i], right[i])
                for i in range(len(left))
                if left.slice(i, 1).equals(right.slice(i, 1)) is False
            ][:limit]
            problems[column] = rows
    extra = [c for c in actual.columns if c not in expected.columns]
    if extra:
        problems['extra columns'] = extra
    return problems
