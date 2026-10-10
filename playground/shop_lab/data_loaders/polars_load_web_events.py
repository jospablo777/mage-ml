from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_web_events(*args, **kwargs):
    """Every web event, about a million rows, as a Polars DataFrame."""
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        return loader.load(
            """
            SELECT event_id, session_id, customer_id, occurred_at, event_type, page,
                   device, duration_ms, scroll_depth
            FROM shop.web_events
            """,
            polars=True,
        )
