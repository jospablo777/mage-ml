from os import path

import geopandas as gpd

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_customer_locations(*args, **kwargs) -> gpd.GeoDataFrame:
    """Customers with a known location, in Costa Rica."""
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        customers = loader.load(
            """
            SELECT customer_id, city, segment, lifetime_value, latitude, longitude
            FROM shop.customers
            WHERE latitude IS NOT NULL AND country = 'Costa Rica'
            """,
            exact_types=True,
        )
    return gpd.GeoDataFrame(
        customers,
        geometry=gpd.points_from_xy(customers['longitude'], customers['latitude']),
        crs='EPSG:4326',
    )
