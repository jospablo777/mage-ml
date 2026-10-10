import geopandas as gpd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# CRTM05, Costa Rica's projected coordinate system: distances in meters.
COSTA_RICA_METERS = 'EPSG:8908'


@transformer
def nearest_store(customers: gpd.GeoDataFrame, stores: gpd.GeoDataFrame, *args, **kwargs):
    """The nearest store of each customer and its distance in kilometers."""
    print(f'customers: {customers.crs}, stores: {stores.crs}')
    stores = stores[stores['country'] == 'Costa Rica'][['store_id', 'name', 'geometry']]
    joined = gpd.sjoin_nearest(
        customers.to_crs(COSTA_RICA_METERS),
        stores.to_crs(COSTA_RICA_METERS),
        how='left',
        distance_col='distance_m',
    )
    joined['distance_km'] = (joined.pop('distance_m') / 1000).round(2)
    joined = joined.drop(columns=['index_right']).rename(columns={'name': 'store_name'})
    return joined.to_crs('EPSG:4326')


@test
def every_customer_has_a_store(output, *args) -> None:
    assert output['store_id'].notna().all(), 'A customer has no nearest store'
    assert output.crs is not None and output.crs.to_epsg() == 4326, 'The CRS was lost'
