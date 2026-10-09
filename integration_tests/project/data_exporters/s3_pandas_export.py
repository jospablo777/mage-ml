from os import path

import pandas as pd

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from mage_ai.settings.repo import get_repo_path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export(frame: pd.DataFrame, **kwargs) -> None:
    config = ConfigFileLoader(path.join(get_repo_path(), 'io_config.yaml'), 's3')
    # The frame holds nanoseconds, which are written as nanoseconds only on request.
    S3.with_config(config).export(
        frame, kwargs['bucket'], 'result/data.parquet', coerce_timestamps=None,
    )
