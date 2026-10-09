import requests

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export(data, **kwargs) -> None:
    requests.post(
        f"{kwargs['api_url']}/collections/{kwargs['collection']}",
        json=[dict(ran=True)],
        timeout=10,
    ).raise_for_status()
