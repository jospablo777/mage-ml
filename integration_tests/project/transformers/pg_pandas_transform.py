import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


def json_kind(value):
    if value is None:
        return None
    if isinstance(value, dict):
        return 'object'
    if isinstance(value, list):
        return 'array'
    if isinstance(value, str):
        return 'string'
    if isinstance(value, bool):
        return 'boolean'
    return 'number'


def lengths(values):
    return pd.array([None if v is None else len(v) for v in values], dtype='Int64')


@transformer
def transform(frame, **kwargs):
    """
    Add columns that SQL can compute the same way, and pass every source column through.
    """
    out = frame.copy()
    out['text_len'] = pd.array(
        [len(v) if isinstance(v, str) else None for v in frame['c_text']], dtype='Int64',
    )
    out['not_bool'] = ~frame['c_bool']
    out['int_diff'] = frame['c_integer'] - frame['c_smallint']
    out['date_year'] = pd.array(
        [None if d is None else d.year for d in frame['c_date']], dtype='Int64',
    )
    out['int_array_len'] = lengths(frame['c_int_array'])
    out['json_kind'] = pd.Series([json_kind(v) for v in frame['c_jsonb']], dtype='str')
    out['bytea_len'] = lengths(frame['c_bytea'])
    return out
