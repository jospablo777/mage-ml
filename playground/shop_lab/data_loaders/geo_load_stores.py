from os import path

import geopandas as gpd

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_stores(*args, **kwargs) -> gpd.GeoDataFrame:
    """Stores as points. The GeoDataFrame passes to the next block with its CRS."""
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        stores = loader.load('SELECT * FROM shop.stores', exact_types=True)
    return gpd.GeoDataFrame(
        stores,
        geometry=gpd.points_from_xy(stores['longitude'], stores['latitude']),
        crs='EPSG:4326',
    )
