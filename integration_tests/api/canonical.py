"""
Compare data frames from any library by value.

Each value becomes a plain Python value: missing markers become None, NumPy and Arrow
scalars become Python scalars, timestamps become datetimes, arrays become lists. A column
matches when every value matches, type included.
"""
import datetime as dt
import decimal
import math
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import polars as pl


def canonical(value: Any) -> Any:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, np.generic):
        return canonical(value.item())
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, (np.ndarray, list, tuple)):
        return [canonical(v) for v in list(value)]
    if isinstance(value, bytearray):
        return bytes(value)
    return value


def columns(frame: Any) -> Dict[str, List[Any]]:
    if isinstance(frame, pl.DataFrame):
        return {c: [canonical(v) for v in frame[c].to_list()] for c in frame.columns}
    return {c: [canonical(v) for v in frame[c].tolist()] for c in frame.columns}


def same(a: Any, b: Any) -> bool:
    if type(a) is not type(b):
        # bool is an int and int is not a float; a type change is a difference.
        return False
    if isinstance(a, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    if isinstance(a, dt.datetime):
        return a == b and (a.tzinfo is None) == (b.tzinfo is None)
    if isinstance(a, decimal.Decimal):
        return a == b and a.as_tuple().exponent == b.as_tuple().exponent
    return a == b


def mismatched_columns(frame: Any, expected: pl.DataFrame) -> Dict[str, str]:
    """Columns whose values differ from expected, with the first difference."""
    got = columns(frame)
    problems = {}
    for column, values in columns(expected).items():
        if column not in got:
            problems[column] = 'missing'
            continue
        for index, (a, b) in enumerate(zip(got[column], values)):
            if not same(a, b):
                problems[column] = f'row {index}: {a!r} != {b!r}'
                break
        else:
            if len(got[column]) != len(values):
                problems[column] = f'{len(got[column])} rows != {len(values)}'
    return problems
