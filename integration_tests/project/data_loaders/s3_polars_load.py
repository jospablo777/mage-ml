from os import path

import polars as pl

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load(**kwargs) -> pl.DataFrame:
    config = ConfigFileLoader(path.join(get_repo_path(), 'io_config.yaml'), 's3')
    return S3.with_config(config).load(kwargs['bucket'], 'source/data.parquet', polars=True)


@test
def test_exact_types(frame, **kwargs) -> None:
    assert frame.height == int(kwargs['expected_rows'])
    assert frame.schema['u64'] == pl.UInt64
    assert frame.schema['pair'] == pl.Array(pl.Int32, 2)
