from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.db2 import Db2
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_db2(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to an IBM Db2 database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://github.com/jospablo777/mage-ml/blob/master/docs-md/integrations/ibm-cloud.md#db2
    """
    schema_name = None  # The user's schema by default
    table_name = 'your_table_name'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Db2.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
        )
