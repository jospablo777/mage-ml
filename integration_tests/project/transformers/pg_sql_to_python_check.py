"""
Checks the frame a PostgreSQL SQL loader returns: integers stay integers, and every row
of the source arrives.
"""
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def check(frame: pd.DataFrame, **kwargs) -> pd.DataFrame:
    assert len(frame) == int(kwargs['expected_rows']), len(frame)
    for column in ('id', 'c_smallint', 'c_integer', 'c_bigint'):
        assert str(frame[column].dtype) == 'Int64', (column, frame[column].dtype)
    assert frame['c_bigint'].max() == 2**63 - 1
    assert frame['c_bigint'].min() == -(2**63)
    return frame.assign(python_bigint_is_null=frame['c_bigint'].isna())
