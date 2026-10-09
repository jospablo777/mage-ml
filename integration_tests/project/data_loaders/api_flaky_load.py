import polars as pl
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(**kwargs):
    # Fails with 503 for the first calls; the block's retry_config runs it again.
    response = requests.get(
        f"{kwargs['api_url']}/flaky/{kwargs['key']}",
        params=dict(failures=kwargs['failures']),
        timeout=10,
    )
    response.raise_for_status()
    return pl.DataFrame(response.json()['data'])
