import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(**kwargs):
    response = requests.get(f"{kwargs['api_url']}/status/{kwargs['code']}", timeout=10)
    response.raise_for_status()
    return response.json()
