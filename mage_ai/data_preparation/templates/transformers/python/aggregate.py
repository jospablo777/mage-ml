import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Groups the rows by the columns of the group_by variable (your_column by default) and
    sums the numeric columns of each group, with the number of rows in rows.
    """
    rows = data.collect() if isinstance(data, pl.LazyFrame) else data
    if not isinstance(rows, pl.DataFrame):
        rows = pl.from_pandas(rows)
    keys = kwargs.get('group_by', ['your_column'])
    keys = [keys] if isinstance(keys, str) else list(keys)
    numeric = [
        name for name, dtype in rows.schema.items() if dtype.is_numeric() and name not in keys
    ]
    return rows.group_by(keys, maintain_order=True).agg(
        pl.len().alias('rows'), *[pl.col(name).sum() for name in numeric],
    )


@test
def test_output_has_rows(output, *args) -> None:
    assert len(output) > 0, 'The output is empty'
