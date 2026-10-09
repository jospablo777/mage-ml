import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(frame: pl.DataFrame, *args, **kwargs) -> pl.LazyFrame:
    return frame.lazy().with_columns(
        varchar_length=pl.col('c_varchar').str.len_chars(),
        # Int64 arithmetic wraps on overflow; Int128 holds the maximum plus one.
        bigint_plus_one=pl.col('c_bigint').cast(pl.Int128) + 1,
    )
