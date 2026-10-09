import io

import polars as pl
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load(**kwargs):
    rows = [
        dict(driver_id=driver, event_timestamp=f'2024-01-01T{hour:02d}:30:00+00:00')
        for driver in (1001, 1002, 1003) for hour in range(6)
    ]
    response = requests.post(
        f"{kwargs['feast_url']}/get-historical-features",
        json=dict(entity_rows=rows, feature_service='driver_activity', format='arrow'),
        timeout=60,
    )
    response.raise_for_status()
    return pl.read_ipc_stream(io.BytesIO(response.content))


@test
def test_types(frame, **kwargs) -> None:
    assert frame.height == 18
    assert frame.schema['avg_daily_trips'] == pl.Int64
    assert frame.schema['event_timestamp'] == pl.Datetime('us', 'UTC')
