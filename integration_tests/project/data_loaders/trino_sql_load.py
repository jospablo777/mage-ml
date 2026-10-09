import datetime as dt
import decimal
import uuid

import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(**kwargs):
    rows = int(kwargs.get('rows', 1000))
    return pd.DataFrame({
        'id': pd.array(range(1, rows + 1), dtype='Int64'),
        'big': pd.array([2**53 + i if i % 7 else None for i in range(rows)], dtype='Int64'),
        'text': pd.Series([f"ñ 'q' {i}" if i % 5 else None for i in range(rows)], dtype='str'),
        'day': [dt.date(2024, 1, 1) + dt.timedelta(days=i) for i in range(rows)],
        'at': pd.to_datetime(['2024-01-01 00:00:00.000001'] * rows) + pd.to_timedelta(
            range(rows), unit='s'),
        'amount': [decimal.Decimal(i) / 100 for i in range(rows)],
        'key': [uuid.UUID(int=i) for i in range(rows)],
    })
