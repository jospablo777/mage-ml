import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(frame: pl.DataFrame, *args, **kwargs) -> pl.LazyFrame:
    return frame.lazy().with_columns(
        text_length=pl.col('text').str.len_chars(),
        ints_length=pl.col('ints').list.len(),
        day_year=pl.col('day').dt.year(),
    )
