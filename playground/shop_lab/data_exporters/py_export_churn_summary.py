from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_churn_summary(summary, **kwargs) -> None:
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        loader.export(summary, 'analytics', 'polyglot_churn_summary', if_exists='replace')
