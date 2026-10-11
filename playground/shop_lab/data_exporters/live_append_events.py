from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def live_append_events(events, *args, **kwargs) -> None:
    """Appends the minute's events to analytics.live_events and keeps the last six hours."""
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as postgres:
        postgres.export(events, 'analytics', 'live_events', if_exists='append', index=False)
        postgres.execute(
            "DELETE FROM analytics.live_events WHERE occurred_at < now() - interval '6 hours'"
        )
        postgres.commit()
    print(f'Appended {len(events)} events')
