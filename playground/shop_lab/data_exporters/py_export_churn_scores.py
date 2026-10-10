from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_churn_scores(scores, **kwargs) -> None:
    """Writes the scores that the R block computed."""
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        loader.export(scores, 'analytics', 'polyglot_churn_scores', if_exists='replace')
