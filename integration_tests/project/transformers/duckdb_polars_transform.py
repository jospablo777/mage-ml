import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(frame: pl.DataFrame, *args, **kwargs) -> pl.LazyFrame:
    return frame.lazy().with_columns(
        text_length=pl.col('c_varchar').str.len_chars(),
        bigint_plus_one=pl.col('c_bigint').cast(pl.Int128) + 1,
        list_length=pl.col('c_int_list').list.len(),
        date_year=pl.col('c_date').dt.year(),
    )
