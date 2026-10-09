from datetime import timedelta
from os import path

import requests

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export(frame, **kwargs) -> None:
    config = ConfigFileLoader(path.join(get_repo_path(), 'io_config.yaml'), 'feast')
    with Postgres.with_config(config) as client:
        client.export(
            frame,
            schema_name='public',
            table_name='driver_hourly_stats',
            if_exists='append',
            verbose=False,
        )
    response = requests.post(
        f"{kwargs['feast_url']}/materialize",
        json=dict(
            start_ts=frame['event_timestamp'].min().isoformat(),
            end_ts=(frame['event_timestamp'].max() + timedelta(minutes=1)).isoformat(),
            feature_views=['driver_hourly_stats'],
        ),
        timeout=120,
    )
    response.raise_for_status()
