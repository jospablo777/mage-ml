from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.ibm_cos import IBMCloudObjectStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_ibm_cos(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to an IBM Cloud Object Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://github.com/jospablo777/mage-ml/blob/master/docs-md/integrations/ibm-cloud.md#cloud-object-storage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'your_bucket_name'
    object_key = 'your_object_key'

    IBMCloudObjectStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )
