from os import path

from utils.driver_sql import read_sql

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(window, *args, **kwargs):
    config = ConfigFileLoader(path.join(get_repo_path(), 'io_config.yaml'), 'warehouse')
    with Postgres.with_config(config) as loader:
        return loader.load(read_sql('sql/drivers/trips.sql'), params=window)
