import io

import polars as pl
import requests

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export(frame, **kwargs) -> None:
    assert isinstance(frame, pl.LazyFrame), type(frame)
    frame = frame.collect()
    buffer = io.BytesIO()
    frame.write_ipc_stream(buffer)
    response = requests.post(
        f"{kwargs['api_url']}/collections/{kwargs['collection']}",
        data=buffer.getvalue(),
        headers={'Content-Type': 'application/vnd.apache.arrow.stream'},
        timeout=60,
    )
    response.raise_for_status()
    assert response.json()['rows'] == frame.height
