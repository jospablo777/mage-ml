import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(frame: pl.DataFrame, *args, **kwargs) -> pl.LazyFrame:
    # A LazyFrame output: Mage stores the result and the next block scans it.
    return frame.lazy().with_columns(
        name_length=pl.col('name').str.len_chars(),
        doubled=pl.col('amount') * 2,
        tag_count=pl.col('tags').list.len(),
        year=pl.col('day').dt.year(),
        is_even=pl.col('id') % 2 == 0,
        upper_name=pl.col('name').str.to_uppercase(),
    )
