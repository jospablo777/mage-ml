from os import path

import polars as pl

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_web_stats(*args, **kwargs):
    """Sessions and events per signed-in customer, as a Polars table: a root of a Merge."""
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        frame = loader.load(
            """
            SELECT customer_id, count(DISTINCT session_id)::bigint AS sessions,
                   count(*)::bigint AS events
            FROM shop.web_events
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
            """,
        )
    return pl.from_pandas(frame)
