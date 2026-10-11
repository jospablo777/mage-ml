from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def live_replace_kpis(kpis, *args, **kwargs) -> None:
    """Replaces analytics.live_minute_kpis with the last hour, minute by minute."""
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as postgres:
        postgres.export(kpis.to_pandas(), 'analytics', 'live_minute_kpis', if_exists='replace',
                        index=False)
