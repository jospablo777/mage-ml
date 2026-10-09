"""
Data for the R block tests: a pandas frame with the types that cross between Python and
R blocks, and what Python blocks receive back from R.

Hand-written edge rows come first, then rows from a seeded generator. Every column has
missing values.
"""
import datetime as dt
import decimal
import math
import random
import uuid
from typing import Dict, List

import pandas as pd
import pyarrow as pa

SEED = 20261009
NEW_YORK = 'America/New_York'
CATEGORIES = ['low', 'mid', 'high']

EDGE_ROWS: List[Dict] = [
    dict(
        c_int=0, c_big=2**53 + 1, c_double=1.5, c_bool=True,
        c_text='ñ "double" \'single\' \\ back', c_category='low',
        c_date=dt.date(2024, 2, 29), c_ts=dt.datetime(2024, 1, 1, 12, 34, 56, 123456),
        c_tstz=dt.datetime(2024, 7, 1, 12, 0, 0, 1), c_duration=dt.timedelta(seconds=1.5),
        c_time=dt.time(12, 30, 1, 500000), c_decimal=decimal.Decimal('1234.56'),
        c_int_list=[1, None, 3], c_text_list=['a', 'b'], c_struct={'a': 1, 'b': 'x'},
        c_binary=b'\x00\xff', c_uuid=uuid.UUID('12345678-1234-5678-1234-567812345678'),
    ),
    dict(
        c_int=None, c_big=None, c_double=None, c_bool=None, c_text=None,
        c_category=None, c_date=None, c_ts=None, c_tstz=None, c_duration=None,
        c_time=None, c_decimal=None, c_int_list=None, c_text_list=None, c_struct=None,
        c_binary=None, c_uuid=None,
    ),
    dict(
        c_int=2**31 - 1, c_big=-(2**62), c_double=-0.0, c_bool=False, c_text='',
        c_category='high', c_date=dt.date(1969, 12, 31),
        c_ts=dt.datetime(1969, 12, 31, 23, 59, 59, 500000),
        c_tstz=dt.datetime(2024, 1, 1, 0, 0), c_duration=dt.timedelta(days=-3, microseconds=1),
        c_time=dt.time(23, 59, 59, 999999), c_decimal=decimal.Decimal('-0.01'),
        c_int_list=[], c_text_list=[], c_struct={'a': None, 'b': 'y'}, c_binary=b'',
        c_uuid=uuid.UUID('00000000-0000-0000-0000-000000000000'),
    ),
    dict(
        c_int=-(2**31) + 1, c_big=2**62, c_double=math.inf, c_bool=True,
        c_text='🦀 line\nbreak\ttab 日本語', c_category='mid', c_date=dt.date(1900, 1, 1),
        c_ts=dt.datetime(2000, 2, 29, 0, 0, 0, 1), c_tstz=dt.datetime(2000, 1, 1, 23, 59),
        c_duration=dt.timedelta(0), c_time=dt.time(0, 0), c_decimal=decimal.Decimal('0'),
        c_int_list=[2**40, -1], c_text_list=['ñ', ''], c_struct={'a': -5, 'b': None},
        c_binary=b'\x01' * 32, c_uuid=uuid.UUID('ffffffff-ffff-ffff-ffff-ffffffffffff'),
    ),
    dict(
        c_int=7, c_big=-1, c_double=-math.inf, c_bool=False, c_text='x', c_category=None,
        c_date=dt.date(2262, 4, 11), c_ts=dt.datetime(2262, 4, 11, 0, 0),
        c_tstz=dt.datetime(1999, 12, 31, 19, 0), c_duration=dt.timedelta(microseconds=1),
        c_time=dt.time(0, 0, 0, 1), c_decimal=decimal.Decimal('99999999.99'),
        c_int_list=[None], c_text_list=[None, 'z'], c_struct={'a': 0, 'b': ''},
        c_binary=b'abc', c_uuid=None,
    ),
    dict(
        c_int=-1, c_big=0, c_double=5e-324, c_bool=None, c_text=' padded ',
        c_category='low', c_date=dt.date(2000, 1, 1), c_ts=dt.datetime(2000, 1, 1),
        c_tstz=None, c_duration=dt.timedelta(hours=-1), c_time=None,
        c_decimal=decimal.Decimal('0.10'), c_int_list=[0], c_text_list=['only'],
        c_struct={'a': 2**40, 'b': 'big'}, c_binary=None,
        c_uuid=uuid.UUID('12345678-0000-0000-0000-000000000001'),
    ),
    dict(
        c_int=1, c_big=1, c_double=1.7976931348623157e308, c_bool=True,
        c_text='NA', c_category='mid', c_date=None, c_ts=None,
        c_tstz=dt.datetime(2024, 3, 10, 7, 30), c_duration=None,
        c_time=dt.time(6, 0), c_decimal=None, c_int_list=None, c_text_list=['NA'],
        c_struct=None, c_binary=b'\n', c_uuid=None,
    ),
]


def _maybe(rng: random.Random, value, missing: float = 0.1):
    return None if rng.random() < missing else value


def generated_rows(count: int) -> List[Dict]:
    rng = random.Random(SEED)
    words = ['alpha', 'beta', 'gamma', 'ñandú', 'Straße', 'мир', '東京', '', 'NULL']
    rows = []
    for _ in range(count):
        list_length = rng.randint(0, 4)
        rows.append(dict(
            c_int=_maybe(rng, rng.randint(-(2**31) + 1, 2**31 - 1)),
            c_big=_maybe(rng, rng.randint(-(2**62), 2**62)),
            c_double=_maybe(rng, rng.uniform(-1e6, 1e6)),
            c_bool=_maybe(rng, rng.random() < 0.5),
            c_text=_maybe(rng, ' '.join(rng.choice(words) for _ in range(rng.randint(0, 3)))),
            c_category=_maybe(rng, rng.choice(CATEGORIES)),
            c_date=_maybe(rng, dt.date(1950, 1, 1) + dt.timedelta(days=rng.randint(0, 40000))),
            c_ts=_maybe(rng, dt.datetime(1950, 1, 1) + dt.timedelta(
                seconds=rng.randint(0, 3 * 10**9), microseconds=rng.randint(0, 999999))),
            c_tstz=_maybe(rng, dt.datetime(2000, 1, 1) + dt.timedelta(
                seconds=rng.randint(0, 10**9), microseconds=rng.randint(0, 999999))),
            c_duration=_maybe(rng, dt.timedelta(microseconds=rng.randint(-10**12, 10**12))),
            c_time=_maybe(rng, dt.time(
                rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59),
                rng.randint(0, 999999))),
            c_decimal=_maybe(rng, decimal.Decimal(rng.randint(-10**10, 10**10)) / 100),
            c_int_list=_maybe(rng, [
                _maybe(rng, rng.randint(-1000, 1000)) for _ in range(list_length)
            ]),
            c_text_list=_maybe(rng, [rng.choice(words) for _ in range(list_length)]),
            c_struct=_maybe(rng, {
                'a': _maybe(rng, rng.randint(-100, 100)),
                'b': _maybe(rng, rng.choice(words)),
            }),
            c_binary=_maybe(rng, bytes(rng.randint(0, 255) for _ in range(list_length))),
            c_uuid=_maybe(rng, uuid.UUID(int=rng.getrandbits(128))),
        ))
    return rows


def rows(count: int = 200) -> List[Dict]:
    result = EDGE_ROWS + generated_rows(count)
    return [dict(id=index, **row) for index, row in enumerate(result, start=1)]


def source_frame(count: int = 200) -> pd.DataFrame:
    """The frame a Python block hands to an R block."""
    data = rows(count)

    def column(name):
        return [row[name] for row in data]

    zoned = pd.Series(pd.to_datetime(
        [None if v is None else pd.Timestamp(v) for v in column('c_tstz')],
    )).astype('datetime64[us]').dt.tz_localize('UTC').dt.tz_convert(NEW_YORK)
    return pd.DataFrame({
        'id': pd.array(column('id'), dtype='Int64'),
        'c_int': pd.array(column('c_int'), dtype='Int64'),
        'c_big': pd.array(column('c_big'), dtype='Int64'),
        'c_double': pd.Series(
            [math.nan if v is None else v for v in column('c_double')], dtype='float64',
        ),
        'c_bool': pd.array(column('c_bool'), dtype='boolean'),
        'c_text': pd.Series(column('c_text'), dtype='str'),
        'c_category': pd.Categorical(column('c_category'), categories=CATEGORIES),
        'c_date': pd.array(column('c_date'), dtype=pd.ArrowDtype(pa.date32())),
        'c_ts': pd.Series(
            [None if v is None else pd.Timestamp(v) for v in column('c_ts')],
            dtype='datetime64[us]',
        ),
        'c_tstz': zoned,
        'c_duration': pd.Series(
            [None if v is None else pd.Timedelta(v) for v in column('c_duration')],
            dtype='timedelta64[us]',
        ),
        'c_time': pd.array(column('c_time'), dtype=pd.ArrowDtype(pa.time64('us'))),
        'c_decimal': pd.array(
            column('c_decimal'), dtype=pd.ArrowDtype(pa.decimal128(12, 2)),
        ),
        'c_int_list': pd.array(
            column('c_int_list'), dtype=pd.ArrowDtype(pa.list_(pa.int64())),
        ),
        'c_text_list': pd.array(
            column('c_text_list'), dtype=pd.ArrowDtype(pa.list_(pa.string())),
        ),
        'c_struct': pd.Series(column('c_struct'), dtype=object),
        'c_binary': pd.Series(column('c_binary'), dtype=object),
        'c_uuid': pd.Series(column('c_uuid'), dtype=object),
    })


def values(series: pd.Series) -> List:
    """The values of series as Python objects, with None for every missing value."""
    if isinstance(series.dtype, pd.ArrowDtype):
        # astype(object) turns Arrow lists into NumPy arrays of floats.
        return pa.array(series).to_pylist()
    result = []
    for value in series.astype(object).tolist():
        if value is None or value is pd.NA or value is pd.NaT:
            result.append(None)
        elif isinstance(value, float) and math.isnan(value):
            result.append(None)
        elif hasattr(value, 'tolist') and not isinstance(value, (str, bytes)):
            result.append(value.tolist())
        else:
            result.append(value)
    return result


# Columns that come back from R exactly as they went.
EXACT_COLUMNS = [
    'id', 'c_int', 'c_big', 'c_double', 'c_bool', 'c_text', 'c_category', 'c_date', 'c_ts',
    'c_tstz', 'c_duration', 'c_time', 'c_int_list', 'c_text_list', 'c_struct', 'c_binary',
]


def _first_difference(actual: List, expected: List):
    if len(actual) != len(expected):
        return f'{len(actual)} rows, expected {len(expected)}'
    for index, (a, e) in enumerate(zip(actual, expected)):
        if a != e:
            return f'row {index}: {a!r}, expected {e!r}'
    return None


def round_trip_mismatches(source: pd.DataFrame, result: pd.DataFrame) -> Dict[str, str]:
    """The columns of result that differ from source, with their first difference."""
    expected = {name: values(source[name]) for name in EXACT_COLUMNS}
    # R has no decimal type; decimals come back as doubles. UUIDs come back as text.
    expected['c_decimal'] = [
        None if v is None else float(v) for v in values(source['c_decimal'])
    ]
    expected['c_uuid'] = [None if v is None else str(v) for v in values(source['c_uuid'])]
    mismatches = {}
    for name, column in expected.items():
        difference = _first_difference(values(result[name]), column)
        if difference:
            mismatches[name] = difference
    return mismatches


def derived_values(source: pd.DataFrame, multiplier: float, prefix: str) -> Dict[str, List]:
    """The columns the R transformers add, computed in Python."""
    return dict(
        r_big_plus_one=[None if v is None else v + 1 for v in values(source['c_big'])],
        r_text_length=[None if v is None else len(v) for v in values(source['c_text'])],
        r_list_length=[0 if v is None else len(v) for v in values(source['c_int_list'])],
        r_date_year=[None if v is None else v.year for v in values(source['c_date'])],
        r_ts_plus_hour=[
            None if v is None else v + dt.timedelta(hours=1) for v in values(source['c_ts'])
        ],
        r_duration_seconds=[
            None if v is None else v.total_seconds() for v in values(source['c_duration'])
        ],
        r_struct_a=[
            None if v is None or v['a'] is None else float(v['a'])
            for v in values(source['c_struct'])
        ],
        r_scaled=[None if v is None else v * multiplier for v in values(source['c_double'])],
        r_label=[prefix + (v or '') for v in values(source['c_text'])],
    )


def derived_mismatches(
    source: pd.DataFrame, result: pd.DataFrame, multiplier: float, prefix: str,
) -> Dict[str, str]:
    mismatches = {}
    for name, expected in derived_values(source, multiplier, prefix).items():
        difference = _first_difference(values(result[name]), expected)
        if difference:
            mismatches[name] = difference
    return mismatches
