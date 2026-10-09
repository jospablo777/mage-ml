import pandas as pd
import pyarrow as pa
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load(**kwargs):
    response = requests.post(
        f"{kwargs['feast_url']}/get-online-features",
        json=dict(
            feature_service='driver_activity',
            entities=dict(driver_id=[1001, 1002, 1003, 1004, 1005]),
        ),
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    # Results come in the order of metadata.feature_names, not the request order.
    columns = {
        name: result['values']
        for name, result in zip(payload['metadata']['feature_names'], payload['results'])
    }
    # Arrow infers int64 for integers next to NULL values. pd.DataFrame would infer
    # float64, which rounds integers above 2**53, and convert_dtypes cannot undo that.
    return pa.table(columns).to_pandas(types_mapper=pd.ArrowDtype)


@test
def test_features_are_present(frame, **kwargs) -> None:
    assert len(frame) == 5
    assert frame['avg_daily_trips'].iloc[4] == 1005 * 10**13 + 47
