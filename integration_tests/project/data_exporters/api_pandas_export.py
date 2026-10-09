import io

import requests

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export(frame, **kwargs) -> None:
    buffer = io.BytesIO()
    frame.to_parquet(buffer)
    response = requests.post(
        f"{kwargs['api_url']}/collections/{kwargs['collection']}",
        data=buffer.getvalue(),
        headers={'Content-Type': 'application/vnd.apache.parquet'},
        timeout=60,
    )
    response.raise_for_status()
    assert response.json()['rows'] == len(frame)
