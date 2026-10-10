from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_all_events(*args, **kwargs):
    """
    The whole web_events table with every type PostgreSQL has here: uuid, timestamptz,
    jsonb, NaN in a double column and nulls. Open the output to explore a million rows.
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        return loader.load('SELECT * FROM shop.web_events', polars=True)
