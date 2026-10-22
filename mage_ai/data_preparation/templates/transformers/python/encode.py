import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(data, *args, **kwargs):
    """
    One-hot encoding: a column per value of the column the encode variable names (category
    by default), 1 where the row has the value and 0 elsewhere, named column_value. The new
    columns replace the encoded one.
    """
    rows = data.collect() if isinstance(data, pl.LazyFrame) else data
    if not isinstance(rows, pl.DataFrame):
        rows = pl.from_pandas(rows)
    column = kwargs.get('encode', 'category')
    return rows.to_dummies(columns=[column], separator='_')
