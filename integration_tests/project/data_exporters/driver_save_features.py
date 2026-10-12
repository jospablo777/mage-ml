from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(features, *args, **kwargs):
    import os

    config = ConfigFileLoader(path.join(get_repo_path(), 'io_config.yaml'), 'analytics')
    with Postgres.with_config(config) as loader:
        loader.export(
            features[['driver_id', 'conv_rate', 'acc_rate', 'avg_daily_trips',
                      'event_timestamp']],
            os.environ['MAGE_TEST_SCHEMA'], 'driver_features', index=False,
            if_exists='append',
        )
    return features
