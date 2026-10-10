from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_coverage(coverage, **kwargs) -> None:
    """Writes the coverage with the geometry as WKT text."""
    table = coverage.drop(columns='geometry').assign(location_wkt=coverage.geometry.to_wkt())
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        loader.export(table, 'analytics', 'geo_customer_coverage', if_exists='replace')
