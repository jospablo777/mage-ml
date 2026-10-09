from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from mage_ai.settings.repo import get_repo_path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export(frame, **kwargs) -> None:
    config = ConfigFileLoader(path.join(get_repo_path(), 'io_config.yaml'), 'mysql')
    with MySQL.with_config(config) as client:
        client.export(frame, None, 'polars_result', verbose=False, if_exists='replace')
