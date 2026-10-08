from os import path

import polars as pl

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load(**kwargs):
    config = ConfigFileLoader(path.join(get_repo_path(), 'io_config.yaml'), 'default')
    with Postgres.with_config(config) as client:
        return client.load(
            f"SELECT * FROM {kwargs['schema']}.src ORDER BY id",
            polars=True,
            verbose=False,
        )


@test
def test_every_row_was_fetched(frame, **kwargs) -> None:
    assert isinstance(frame, pl.DataFrame), type(frame)
    assert frame.height == int(kwargs['expected_rows']), frame.height


@test
def test_types_are_exact(frame, **kwargs) -> None:
    assert frame.schema['c_bigint'] == pl.Int64
    assert frame.schema['c_numeric'] == pl.Decimal(38, 10)
    assert frame.filter(pl.col('id') == 1)['c_bigint'].item() == 2**53 + 1
