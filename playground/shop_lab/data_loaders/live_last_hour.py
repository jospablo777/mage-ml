from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def live_last_hour(*args, **kwargs):
    """The events of the last hour, as a Polars frame."""
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as postgres:
        return postgres.load(
            "SELECT occurred_at, event_type, amount FROM analytics.live_events "
            "WHERE occurred_at >= now() - interval '1 hour'",
            polars=True,
        )
