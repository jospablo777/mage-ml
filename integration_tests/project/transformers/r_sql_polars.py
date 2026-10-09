import json

import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(frame, *args, **kwargs) -> pl.DataFrame:
    # The R block's output arrives as pandas. A Polars column has one type, so the JSON
    # values, which are objects, arrays and numbers, become JSON text.
    frame = frame.assign(
        c_jsonb=[None if v is None else json.dumps(v) for v in frame['c_jsonb']],
    )
    return pl.from_pandas(frame).with_columns(
        polars_bigint_plus_one=pl.col('c_bigint').cast(pl.Int128) + 1,
    )
