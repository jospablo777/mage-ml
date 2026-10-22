import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Turns columns into rows: one row per id and column, with the column's name in variable
    and its value, as text, in value. The id variable (id by default) names the columns
    that identify a row; pivot() does the reverse.
    """
    rows = data.collect() if isinstance(data, pl.LazyFrame) else data
    if not isinstance(rows, pl.DataFrame):
        rows = pl.from_pandas(rows)
    index = kwargs.get('id', 'id')
    index = [index] if isinstance(index, str) else list(index)
    values = [name for name in rows.columns if name not in index]
    return rows.with_columns(pl.col(values).cast(pl.String)).unpivot(
        on=values, index=index, variable_name='variable', value_name='value',
    )


@test
def test_output_has_rows(output, *args) -> None:
    assert len(output) > 0, 'The output is empty'
