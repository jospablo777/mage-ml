from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.duckdb import DuckDB
from mage_ai.settings.repo import get_repo_path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export(frame, **kwargs) -> None:
    config = ConfigFileLoader(path.join(get_repo_path(), 'io_config.yaml'), 'duckdb')
    client = DuckDB.with_config(config)
    try:
        client.export(frame, table_name='polars_result', if_exists='replace', verbose=False)
    finally:
        client.close()
