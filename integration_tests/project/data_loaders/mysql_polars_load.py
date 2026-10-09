from os import path

import polars as pl

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load(**kwargs) -> pl.DataFrame:
    config = ConfigFileLoader(path.join(get_repo_path(), 'io_config.yaml'), 'mysql')
    with MySQL.with_config(config) as client:
        # DECIMAL(65,30) reaches Polars as text; the pipeline leaves it out.
        return client.load(
            'SELECT * FROM src ORDER BY id', verbose=False, polars=True,
        ).drop('c_decimal_wide')


@test
def test_exact_types(frame, **kwargs) -> None:
    assert frame.height == int(kwargs['expected_rows'])
    assert frame.schema['c_ubigint'] == pl.UInt64
    assert frame.schema['c_timestamp'] == pl.Datetime('us', 'UTC')
