import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Keeps the latest row of each key. The key column comes from the key variable (id by
    default), and the column whose largest value is the latest from order_by (updated_at
    by default). Rows without a key are dropped.
    """
    rows = data.collect() if isinstance(data, pl.LazyFrame) else data
    if not isinstance(rows, pl.DataFrame):
        rows = pl.from_pandas(rows)
    key = kwargs.get('key', 'id')
    order_by = kwargs.get('order_by', 'updated_at')
    return (
        rows.filter(pl.col(key).is_not_null())
        .sort(order_by, descending=True, nulls_last=True)
        .unique(subset=key, keep='first', maintain_order=True)
    )


@test
def test_keys_are_unique(output, *args, **kwargs) -> None:
    key = kwargs.get('key', 'id')
    assert output[key].is_duplicated().sum() == 0, f'A {key} repeats'
