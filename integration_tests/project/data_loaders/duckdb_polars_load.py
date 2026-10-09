from os import path

import polars as pl

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.duckdb import DuckDB
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load(**kwargs) -> pl.DataFrame:
    config = ConfigFileLoader(path.join(get_repo_path(), 'io_config.yaml'), 'duckdb')
    client = DuckDB.with_config(config)
    try:
        return client.load('SELECT * FROM src ORDER BY id', verbose=False, polars=True)
    finally:
        client.close()


@test
def test_exact_types(frame, **kwargs) -> None:
    assert frame.height == int(kwargs['expected_rows'])
    assert frame.schema['c_hugeint'] == pl.Int128
    assert frame.schema['c_ubigint'] == pl.UInt64
    assert frame.schema['c_decimal_wide'] == pl.Decimal(38, 10)
