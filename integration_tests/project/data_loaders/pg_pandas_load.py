from os import path

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
            exact_types=True,
            verbose=False,
        )


@test
def test_every_row_was_fetched(frame, **kwargs) -> None:
    assert len(frame) == int(kwargs['expected_rows']), len(frame)


@test
def test_types_are_exact(frame, **kwargs) -> None:
    assert str(frame['c_bigint'].dtype) == 'Int64', frame['c_bigint'].dtype
    assert frame.loc[frame['id'] == 1, 'c_bigint'].item() == 2**53 + 1
    assert frame.loc[frame['id'] == 1, 'c_bytea'].item() == b'\x00\xff\x10binary\x00'
