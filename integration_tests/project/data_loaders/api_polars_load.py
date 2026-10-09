import io

import polars as pl
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load(**kwargs):
    response = requests.get(
        f"{kwargs['api_url']}/datasets/orders",
        params=dict(format='arrow-stream', rows=kwargs['rows']),
        timeout=30,
    )
    response.raise_for_status()
    return pl.read_ipc_stream(io.BytesIO(response.content))


@test
def test_types_are_exact(frame, **kwargs) -> None:
    assert frame.height == int(kwargs['rows'])
    assert frame.schema['price'] == pl.Decimal(18, 4)
    assert frame.schema['created_at'] == pl.Datetime('us', 'UTC')
    assert frame['big_id'][0] == 2**62 + 1
