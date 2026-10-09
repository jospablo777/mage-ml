import io

import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load(**kwargs):
    response = requests.get(
        f"{kwargs['api_url']}/datasets/orders",
        params=dict(format='parquet', rows=kwargs['rows']),
        timeout=30,
    )
    response.raise_for_status()
    # pyarrow-backed dtypes keep integers with NULL, decimals and bytes exact.
    return pd.read_parquet(io.BytesIO(response.content), dtype_backend='pyarrow')


@test
def test_every_row_was_fetched(frame, **kwargs) -> None:
    assert len(frame) == int(kwargs['rows']), len(frame)


@test
def test_values_are_exact(frame, **kwargs) -> None:
    assert frame['big_id'].iloc[0] == 2**62 + 1
    assert str(frame['price'].iloc[0]) == '12345678901234.5678'
    assert frame['name'].iloc[1] == ''
