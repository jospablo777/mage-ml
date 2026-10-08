import json

import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

KINDS = {dict: 'object', list: 'array', str: 'string', bool: 'boolean', int: 'number',
         float: 'number'}


def json_kind(text):
    value = json.loads(text)
    return None if value is None else KINDS[type(value)]


@transformer
def transform(frame, **kwargs):
    """
    Add columns that SQL can compute the same way, and pass every source column through.
    """
    return frame.with_columns(
        text_len=pl.col('c_text').str.len_chars().cast(pl.Int64),
        not_bool=~pl.col('c_bool'),
        int_diff=pl.col('c_integer').cast(pl.Int64) - pl.col('c_smallint').cast(pl.Int64),
        date_year=pl.col('c_date').dt.year().cast(pl.Int64),
        int_array_len=pl.col('c_int_array').list.len().cast(pl.Int64),
        json_kind=pl.col('c_jsonb').map_elements(json_kind, return_dtype=pl.String),
        bytea_len=pl.col('c_bytea').bin.size().cast(pl.Int64),
    )
