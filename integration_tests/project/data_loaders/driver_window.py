from os import path

from utils.driver_sql import read_sql

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(cursor, *args, **kwargs):
    """The window of new drivers: after last_id, up to high_id."""
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'warehouse'
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        high = loader.load(read_sql('sql/drivers/high_cursor.sql'))
    return {'last_id': int(cursor['last_id']), 'high_id': int(high.iloc[0]['high_id'])}
